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
	"archive/tar"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"path/filepath"
	"strings"

	"github.com/Microsoft/go-winio/pkg/guid"
	"github.com/Microsoft/hcsshim/pkg/cimfs"
	"github.com/containerd/containerd/mount"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/windows"
)

// TODO SYMLINKS, TOMBSTONES

// Each layer is an independent cim containing the files for that layer.
// Initially, each cim will be a partial representation, but later to support
// down-level Xenon guests (e.g. RS1) will be a fully-expanded layer with WCIFS
// reparse points for files that are inherited from previous layers.
//
// Unpacking each layer goes through several steps:
// - For each file in the layer, place it into the cim at the correct location
// - For any reg hives in Files\Windows\System32\Config, add a link to them from
//   the Hives dir
// -

const (
	preRegFSName = "prereg"
	layerFSName  = "layer"
)

type hive struct {
	name  string
	base  string
	delta string
}

var (
	hives = []hive{
		{"SYSTEM", "SYSTEM_BASE", "SYSTEM_DELTA"},
		{"SOFTWARE", "SOFTWARE_BASE", "SOFTWARE_DELTA"},
		{"SAM", "SAM_BASE", "SAM_DELTA"},
		{"SECURITY", "SECURITY_BASE", "SECURITY_DELTA"},
		{"DEFAULT", "DEFAULTUSER_BASE", "DEFAULTUSER_DELTA"},
	}
)

func snapshotCimPath(dir string) string {
	return filepath.Join(dir, "snapshot.cim")
}

func unpackTar(cimPath string, fsName string, tr *tar.Reader) (err error) {
	cim, err := cimfs.Open(cimPath, "", fsName)
	if err != nil {
		return errors.Wrap(err, "failed to open CimFS")
	}
	defer func() {
		err2 := cim.Close()
		if err == nil {
			err = err2
		}
	}()

	hivesPath := filepath.Join("Files", "Windows", "System32", "Config")
	var seenHives []hive
	var seenHivesDir bool

	h, err := tr.Next()
	for err == nil {
		base := path.Base(h.Name)
		if strings.HasPrefix(base, ".wh.") && false { //archive.WhiteoutPrefix) {
			// name := path.Join(path.Dir(h.Name), base[len(archive.WhiteoutPrefix):])
			// write tombstone
			panic("not implemented")
		} else {
			// if link: err = w.AddLink(filepath.FromSlash(hdr.Name), filepath.FromSlash(hdr.Linkname))

			info, err := fileInfoFromTar(h)
			if err != nil {
				return err
			}

			newPath := filepath.FromSlash(h.Name) // TODO: need backslashes for cimfs?

			if newPath == "Hives" {
				seenHivesDir = true
			}

			// If this is a registry hive, record that we have seen it.
			for _, hive := range hives {
				if strings.ToLower(newPath) == strings.ToLower(filepath.Join(hivesPath, hive.name)) {
					seenHives = append(seenHives, hive)
				}
			}

			logrus.WithFields(logrus.Fields{
				"newPath": newPath,
				// "info":    fmt.Sprintf("%#v", info),
			}).Debug("Adding file")

			func(path string, info *cimfs.FileInfo, r io.Reader) (err error) {
				if err := cim.AddFile(path, info); err != nil {
					return errors.Wrap(err, "failed to add file")
				}
				defer func() {
					err2 := cim.CloseStream()
					if err == nil {
						err = err2
					}
				}()
				if _, err := io.Copy(cim, r); err != nil {
					return errors.Wrap(err, "failed to write stream")
				}
				return nil
			}(newPath, info, tr)
		}

		h, err = tr.Next()
	}

	if err != io.EOF {
		return errors.Wrap(err, "failed iterating tar entries")
	}

	// Add layout file
	content := []byte("vhd-with-hives\n")
	if err := addFile(
		cim,
		"layout",
		&cimfs.FileInfo{
			Size: int64(len(content)),
		},
		content); err != nil {
		return errors.Wrap(err, "failed to write layout file")
	}

	// Add Hives dir
	if !seenHivesDir {
		if err := addDir(cim, "Hives"); err != nil {
			return errors.Wrap(err, "failed to create Hives dir")
		}
	}
	for _, hive := range seenHives {
		existingPath := filepath.Join(hivesPath, hive.name)
		targetPath := filepath.Join("Hives", hive.base)
		// logrus.WithFields(logrus.Fields{
		// 	"existingPath": existingPath,
		// 	"targetPath":   targetPath,
		// }).Info("Creating hive link")
		if err := cim.AddLink(existingPath, targetPath); err != nil {
			return errors.Wrap(err, "failed to create hive link")
		}
	}

	return cim.Commit()
}

func mergeHives(cimPath string, fsName string, parentCimPath string, parentFSName string) (_ string, err error) {
	g, err := guid.NewV4()
	if err != nil {
		return "", errors.Wrap(err, "failed to generate GUID")
	}
	layerMountPath := fmt.Sprintf(`\\?\Volume{%s}\`, g)
	if err := cimfs.MountImage(cimPath, fsName, g); err != nil {
		return "", errors.Wrap(err, "failed to mount layer CimFS")
	}
	defer func() {
		err2 := cimfs.UnmountImage(g)
		if err == nil {
			err = errors.Wrap(err2, "failed to unmount layer CimFS")
		}
	}()
	g2, err := guid.NewV4()
	if err != nil {
		return "", errors.Wrap(err, "failed to generate GUID")
	}
	parentMountPath := fmt.Sprintf(`\\?\Volume{%s}\`, g2)
	if err := cimfs.MountImage(parentCimPath, parentFSName, g2); err != nil {
		return "", errors.Wrap(err, "failed to mount parent CimFS")
	}
	defer func() {
		err2 := cimfs.UnmountImage(g2)
		if err == nil {
			err = errors.Wrap(err2, "failed to unmount parent CimFS")
		}
	}()

	tmp, err := ioutil.TempDir("", "")
	if err != nil {
		return "", errors.Wrap(err, "failed to create temp dir")
	}

	for _, hive := range hives {
		parentHivePath := filepath.Join(parentMountPath, "Hives", hive.base)
		layerHivePath := filepath.Join(tmp, hive.base)
		deltaHivePath := filepath.Join(layerMountPath, "Hives", hive.delta)

		logrus.WithFields(logrus.Fields{
			"parentHivePath": parentHivePath,
			"layerHivePath":  layerHivePath,
			"deltaHivePath":  deltaHivePath,
		}).Debug("Merging registry hives")

		var baseHive, deltaHive, mergedHive orHKey
		if err := orOpenHive(parentHivePath, &baseHive); err != nil {
			return "", errors.Wrapf(err, "failed to open base hive %s", parentHivePath)
		}
		defer func() {
			err2 := orCloseHive(baseHive)
			if err == nil {
				err = errors.Wrap(err2, "failed to close base hive")
			}
		}()
		if err := orOpenHive(deltaHivePath, &deltaHive); err != nil {
			return "", errors.Wrapf(err, "failed to open delta hive %s", deltaHivePath)
		}
		defer func() {
			err2 := orCloseHive(deltaHive)
			if err == nil {
				err = errors.Wrap(err2, "failed to close delta hive")
			}
		}()
		if err := orMergeHives([]orHKey{baseHive, deltaHive}, &mergedHive); err != nil {
			return "", errors.Wrap(err, "failed to merge hives")
		}
		defer func() {
			err2 := orCloseHive(mergedHive)
			if err == nil {
				err = errors.Wrap(err2, "failed to close merged hive")
			}
		}()
		if err := orSaveHive(mergedHive, layerHivePath, 6, 1); err != nil {
			return "", errors.Wrap(err, "failed to save hive")
		}
	}

	return tmp, nil
}

func apply(ctx context.Context, mounts []mount.Mount, r io.Reader) (err error) {
	layerPath, parentLayerPaths, err := parseMounts(mounts)
	if err != nil {
		return err
	}
	cimPath := snapshotCimPath(layerPath)
	var parentCimPath string
	if len(parentLayerPaths) > 0 {
		parentCimPath = snapshotCimPath(parentLayerPaths[0])
	}

	if err := unpackTar(cimPath, preRegFSName, tar.NewReader(r)); err != nil {
		return errors.Wrap(err, "failed to unpack tar")
	}

	if parentCimPath != "" {
		mergedHiveDir, err := mergeHives(cimPath, preRegFSName, parentCimPath, layerFSName)
		if err != nil {
			return errors.Wrap(err, "failed to fixup hives")
		}
		cim, err := cimfs.Open(cimPath, preRegFSName, layerFSName)
		if err != nil {
			return errors.Wrap(err, "failed to open cimfs")
		}
		defer func() {
			err2 := cim.Close()
			if err == nil {
				err = err2
			}
		}()
		mergedHives, err := ioutil.ReadDir(mergedHiveDir)
		if err != nil {
			return errors.Wrap(err, "failed to enumerate hives")
		}
		for _, h := range mergedHives {
			newName := filepath.Join("Hives", h.Name())
			if err := cim.AddFile(newName, &cimfs.FileInfo{Size: h.Size()}); err != nil {
				return errors.Wrap(err, "failed to add hive to cim")
			}
			data, err := ioutil.ReadFile(filepath.Join(mergedHiveDir, h.Name()))
			if err != nil {
				return errors.Wrap(err, "failed to read hive")
			}
			if _, err := cim.Write(data); err != nil {
				return errors.Wrap(err, "failed to write hive to cim")
			}
			if err := cim.CloseStream(); err != nil {
				return errors.Wrap(err, "failed to close hive in cim")
			}
		}
		if err := cim.Commit(); err != nil {
			return errors.Wrap(err, "failed to commit hive cim")
		}
	} else {
		cim, err := cimfs.Open(cimPath, preRegFSName, layerFSName)
		if err != nil {
			return errors.Wrap(err, "failed to open cim123")
		}
		defer func() {
			err2 := cim.Close()
			if err == nil {
				err = err2
			}
		}()
		if err := cim.Commit(); err != nil {
			return errors.Wrap(err, "failed to commit123 cim")
		}
	}

	return nil
}

func addFile(cim *cimfs.FileSystem, path string, info *cimfs.FileInfo, content []byte) (err error) {
	if err := cim.AddFile(path, info); err != nil {
		return err
	}
	defer func() {
		err2 := cim.CloseStream()
		if err == nil {
			err = err2
		}
	}()
	if _, err := cim.Write(content); err != nil {
		return err
	}
	return nil
}

func addDir(cim *cimfs.FileSystem, path string) error {
	if err := cim.AddFile(path, &cimfs.FileInfo{Attributes: windows.FILE_ATTRIBUTE_DIRECTORY}); err != nil {
		return err
	}
	return cim.CloseStream()
}

func parseMounts(mounts []mount.Mount) (string, []string, error) {
	if len(mounts) != 1 {
		return "", nil, errors.New("wrong number of mounts")
	}
	m := mounts[0]
	if m.Type != "cimfs" {
		return "", nil, errors.New("invalid mount type")
	}
	var parentPaths []string
	parentPathOptPrefix := "parentLayerPaths="
	for _, opt := range m.Options {
		if strings.HasPrefix(opt, parentPathOptPrefix) {
			if err := json.Unmarshal([]byte(opt[len(parentPathOptPrefix):]), &parentPaths); err != nil {
				return "", nil, err
			}
		}
	}
	return m.Source, parentPaths, nil
}
