package mounter

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/Microsoft/go-winio/pkg/guid"
	"github.com/Microsoft/hcsshim/pkg/cimfs"
	"github.com/containerd/containerd/plugin"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
)

func init() {
	plugin.Register(&plugin.Registration{
		Type: plugin.ServicePlugin,
		ID:   "mounter",
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			return newMounter(ic.Root)
		},
	})
}

type mounter struct {
	mu sync.Mutex
	db *bolt.DB
}

// plugins, err := ic.GetByType(plugin.ServicePlugin)
// if err != nil {
// 	return nil, err
// }
// p, ok := plugins[services.ContainersService]
// if !ok {
// 	return nil, errors.New("containers service not found")
// }
// i, err := p.Instance()
// if err != nil {
// 	return nil, err
// }
// return &service{local: i.(api.ContainersClient)}, nil

func newMounter(root string) (*mounter, error) {
	db, err := bolt.Open(filepath.Join(root, "store.db"), 0660, nil)
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		if err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			m, err := mountFromBucket(b)
			if err != nil {
				return errors.Wrap(err, "failed to get mount from bucket")
			}
			if _, err := os.Stat(m.mountPath); err == nil {
				if err := cimfs.UnmountImage(m.g); err != nil {
					return errors.Wrap(err, "failed to unmount cimfs")
				}
			}
			if err := tx.DeleteBucket(name); err != nil {
				return errors.Wrap(err, "failed to delete mount bucket")
			}
			return nil
		}); err != nil {
			return errors.Wrap(err, "error enumerating buckets")
		}
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "error when cleaning up mounts")
	}
	return &mounter{db: db}, nil
}

func (s *mounter) Increment(cimPath string) (_ string, err error) {
	tx, err := s.db.Begin(true)
	if err != nil {
		return "", errors.Wrap(err, "failed to start tx")
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	b, err := tx.CreateBucketIfNotExists([]byte(cimPath))
	if err != nil {
		return "", errors.Wrap(err, "failed to create bucket")
	}

	m, err := mountFromBucket(b)
	if err != nil {
		return "", errors.Wrap(err, "failed to get mount from bucket")
	}

	if m.refCount == 0 {
		m.g, err = guid.NewV4()
		if err != nil {
			return "", errors.Wrap(err, "failed to generate GUID")
		}
		m.mountPath = fmt.Sprintf(`\\?\Volume{%s}\`, m.g)
		if err := cimfs.MountImage(cimPath, m.g); err != nil {
			return "", errors.Wrap(err, "failed to mount cim")
		}
	}

	m.refCount++

	if err := m.writeToBucket(b); err != nil {
		return "", errors.Wrap(err, "failed to write mount to bucket")
	}

	return m.mountPath, nil
}

func (s mounter) Decrement(cimPath string) error {
	tx, err := s.db.Begin(true)
	if err != nil {
		return errors.Wrap(err, "failed to start tx")
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	b := tx.Bucket([]byte(cimPath))
	if b == nil {
		return fmt.Errorf("decrement of unmounted cimPath: %s", cimPath)
	}

	m, err := mountFromBucket(b)
	if err != nil {
		return errors.Wrap(err, "failed to get mount from bucket")
	}

	m.refCount--

	if m.refCount == 0 {
		if err := cimfs.UnmountImage(m.g); err != nil {
			return errors.Wrap(err, "failed to unmount cim")
		}
		if err := tx.DeleteBucket([]byte(cimPath)); err != nil {
			return errors.Wrap(err, "failed to delete bucket")
		}
	} else {
		if err := m.writeToBucket(b); err != nil {
			return errors.Wrap(err, "failed to write mount to bucket")
		}
	}

	return nil
}

type mount struct {
	refCount  uint32
	g         guid.GUID
	mountPath string
}

func mountFromBucket(b *bolt.Bucket) (_ mount, err error) {
	var refCount uint32
	d := b.Get([]byte("refCount"))
	if d != nil {
		refCount = binary.LittleEndian.Uint32(d)
	}

	var g guid.GUID
	gData := b.Get([]byte("g"))
	if gData != nil {
		g, err = guid.FromString(string(gData))
		if err != nil {
			return mount{}, errors.Wrap(err, "failed to parse GUID")
		}
	}

	return mount{
		refCount:  refCount,
		g:         g,
		mountPath: string(b.Get([]byte("mountPath"))),
	}, nil
}

func (m mount) writeToBucket(b *bolt.Bucket) error {
	d := make([]byte, 4)
	binary.LittleEndian.PutUint32(d, m.refCount)
	if err := b.Put([]byte("refCount"), d); err != nil {
		return errors.Wrap(err, "failed to write refCount")
	}
	if err := b.Put([]byte("g"), []byte(m.g.String())); err != nil {
		return errors.Wrap(err, "failed to write g")
	}
	if err := b.Put([]byte("mountPath"), []byte(m.mountPath)); err != nil {
		return errors.Wrap(err, "failed to write mountPath")
	}
	return nil
}
