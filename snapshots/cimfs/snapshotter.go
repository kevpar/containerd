/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package cimfs

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"github.com/Microsoft/go-winio/vhd"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/plugin"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/storage"
	"github.com/containerd/continuity/fs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/windows"
)

func init() {
	plugin.Register(&plugin.Registration{
		Type: plugin.SnapshotPlugin,
		ID:   "cimfs",
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			return newSnapshotter(ic.Root)
		},
	})
}

type snapshotter struct {
	root string
	ms   *storage.MetaStore
}

func newSnapshotter(root string) (snapshots.Snapshotter, error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	ms, err := storage.NewMetaStore(filepath.Join(root, "metadata.db"))
	if err != nil {
		return nil, err
	}

	if err := os.Mkdir(filepath.Join(root, "snapshots"), 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}

	return snapshotter{
		root,
		ms,
	}, nil
}

func (s snapshotter) snapshotDir(id string) string {
	return filepath.Join(s.root, "snapshots", id)
}

func (s snapshotter) cimPath(id string) string {
	return filepath.Join(s.snapshotDir(id), "snapshot.cim")
}

func (s snapshotter) scratchPath(id string) string {
	return filepath.Join(s.snapshotDir(id), "sandbox.vhdx")
}

func (s snapshotter) Stat(ctx context.Context, key string) (_ snapshots.Info, err error) {
	ctx, t, err := s.ms.TransactionContext(ctx, false)
	if err != nil {
		return snapshots.Info{}, err
	}
	defer func() {
		err2 := t.Rollback()
		if err == nil {
			err = errors.Wrap(err2, "failed to roll back transaction")
		}
	}()

	_, info, _, err := storage.GetInfo(ctx, key)
	return info, err
}

func (s snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	panic("not implemented")
}

func (s snapshotter) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	panic("not implemented")
}

func (s snapshotter) Mounts(ctx context.Context, key string) (_ []mount.Mount, err error) {
	ctx, t, err := s.ms.TransactionContext(ctx, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		err2 := t.Rollback()
		if err == nil {
			err = errors.Wrap(err2, "failed to roll back transaction")
		}
	}()

	snapshot, err := storage.GetSnapshot(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get snapshot mount")
	}
	return s.mounts(snapshot)
}

func (s snapshotter) mounts(snapshot storage.Snapshot) ([]mount.Mount, error) {
	if snapshot.Kind != snapshots.KindActive {
		panic("view snapshot not supported yet")
	}

	parentPaths := make([]string, 0, len(snapshot.ParentIDs))
	for _, id := range snapshot.ParentIDs {
		parentPaths = append(parentPaths, s.snapshotDir(id))
	}
	parentPathJSON, err := json.Marshal(parentPaths)
	if err != nil {
		return nil, err
	}

	return []mount.Mount{
		{
			Type:   "cimfs",
			Source: s.snapshotDir(snapshot.ID),
			Options: []string{
				"parentLayerPaths=" + string(parentPathJSON),
			},
		},
	}, nil
}

func createVHDX(path string, sizeGB uint32) (err error) {
	if err := vhd.CreateVhdx(path, sizeGB, 1); err != nil {
		return errors.Wrap(err, "failed to create VHD")
	}
	vhd, err := vhd.OpenVirtualDisk(path, vhd.VirtualDiskAccessNone, vhd.OpenVirtualDiskFlagNone)
	if err != nil {
		return errors.Wrap(err, "failed to open VHD")
	}
	defer func() {
		err2 := windows.CloseHandle(windows.Handle(vhd))
		if err == nil {
			err = errors.Wrap(err2, "failed to close VHD")
		}
	}()
	if err := hcsFormatWritableLayerVhd(uintptr(vhd)); err != nil {
		return errors.Wrap(err, "failed to format VHD")
	}
	return nil
}

type cancellableDefer struct {
	f    func()
	once sync.Once
}

func newCancellableDefer(f func()) *cancellableDefer {
	return &cancellableDefer{
		f: f,
	}
}

func (cd *cancellableDefer) Do() {
	cd.once.Do(cd.f)
}

func (cd *cancellableDefer) Cancel() {
	cd.once.Do(func() {})
}

func (s snapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts []snapshots.Opt) (_ []mount.Mount, err error) {
	if kind != snapshots.KindActive {
		panic("view snapshot not supported yet")
	}

	logrus.WithFields(logrus.Fields{
		"kind":   kind,
		"key":    key,
		"parent": parent,
	}).Info("Creating CimFS snapshot")

	ctx, t, err := s.ms.TransactionContext(ctx, true)
	if err != nil {
		return nil, err
	}
	rollback := newCancellableDefer(func() {
		err2 := t.Rollback()
		if err == nil {
			err = errors.Wrap(err2, "failed to roll back transaction")
		}
	})
	defer rollback.Do()

	snapshot, err := storage.CreateSnapshot(ctx, kind, key, parent, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snapshot")
	}

	if err := os.MkdirAll(s.snapshotDir(snapshot.ID), 770); err != nil {
		return nil, errors.Wrap(err, "failed to create snapshot directory")
	}
	// TODO flexible storage size?
	if err := createVHDX(s.scratchPath(snapshot.ID), 20); err != nil {
		return nil, errors.Wrap(err, "failed to create VHD")
	}

	if err := t.Commit(); err != nil {
		return nil, errors.Wrap(err, "commit failed")
	}
	rollback.Cancel()

	return s.mounts(snapshot)
}

func (s snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return s.createSnapshot(ctx, snapshots.KindActive, key, parent, opts)
}

func (s snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return s.createSnapshot(ctx, snapshots.KindView, key, parent, opts)
}

func (s snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) (err error) {
	ctx, t, err := s.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}
	rollback := newCancellableDefer(func() {
		err2 := t.Rollback()
		if err == nil {
			err = errors.Wrap(err2, "failed to roll back transaction")
		}
	})
	defer rollback.Do()

	// TODO put real storage amount here
	usage := fs.Usage{
		Size: 0,
	}
	if _, err = storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
		return errors.Wrap(err, "failed to commit snapshot")
	}

	if err := t.Commit(); err != nil {
		return errors.Wrap(err, "commit failed")
	}
	rollback.Cancel()

	return nil
}

func (s snapshotter) Remove(ctx context.Context, key string) (err error) {
	ctx, t, err := s.ms.TransactionContext(ctx, false)
	if err != nil {
		return err
	}
	defer func() {
		err2 := t.Rollback()
		if err == nil {
			err = errors.Wrap(err2, "failed to roll back transaction")
		}
	}()

	id, _, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return err
	}

	// TODO unmount

	return os.RemoveAll(s.snapshotDir(id))
}

func (s snapshotter) Walk(ctx context.Context, fn func(context.Context, snapshots.Info) error) (err error) {
	ctx, t, err := s.ms.TransactionContext(ctx, false)
	if err != nil {
		return err
	}
	defer func() {
		err2 := t.Rollback()
		if err == nil {
			err = errors.Wrap(err2, "failed to roll back transaction")
		}
	}()

	return storage.WalkInfo(ctx, fn)
}

func (s snapshotter) Close() error {
	return nil
}
