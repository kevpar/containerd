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

package mount

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/Microsoft/go-winio/pkg/guid"
	"github.com/Microsoft/hcsshim"
	"github.com/Microsoft/hcsshim/pkg/cimfs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	// ErrNotImplementOnWindows is returned when an action is not implemented for windows
	ErrNotImplementOnWindows = errors.New("not implemented under windows")
)

var mounts map[string]Mount

func init() {
	mounts = make(map[string]Mount)
}

// Mount to the provided target
func (m *Mount) Mount(target string) error {
	switch m.Type {
	case "windows-layer":
		return m.mountLegacy(target)
	case "cimfs":
		return m.mountCimFS(target)
	default:
		return errors.Errorf("invalid windows mount type: '%s'", m.Type)
	}
}

func (m *Mount) mountCimFS(target string) (err error) {
	var parentPaths []string
	parentPathOptPrefix := "parentLayerPaths="
	for _, opt := range m.Options {
		if strings.HasPrefix(opt, parentPathOptPrefix) {
			if err := json.Unmarshal([]byte(opt[len(parentPathOptPrefix):]), &parentPaths); err != nil {
				return err
			}
		}
	}

	if err := hcsshim.ActivateLayer(hcsshim.DriverInfo{}, m.Source); err != nil {
		return errors.Wrap(err, "failed to activate mount source")
	}
	defer func() {
		logrus.Info("Deferred vhd mount cleanup")
		if err2 := hcsshim.DeactivateLayer(hcsshim.DriverInfo{}, m.Source); err2 != nil {
			logrus.WithError(err2).Error("Failed to unmount VHD")
		}
	}()

	parentMountPaths := make([]string, 0, len(parentPaths))
	for _, parentPath := range parentPaths {
		var g hcsshim.GUID
		g, err = hcsshim.NameToGuid(parentPath)
		if err != nil {
			return errors.Wrap(err, "failed to convert name to GUID")
		}
		g2 := guid.FromWindowsArray(g)
		logrus.WithFields(logrus.Fields{
			"layer": parentPath,
			"guid":  g2.String(),
		}).Info("Mounting CimFS layer")
		if err := cimfs.MountImage(parentPath, "layer.cimfs", g2); err != nil {
			return errors.Wrapf(err, "failed to mount CimFS: %q", g2.String())
		}
		defer func() {
			logrus.WithField("guid", g2.String()).Info("Deferred cimfs cleanup")
			if err != nil {
				if err2 := cimfs.UnmountImage(g2); err2 != nil {
					logrus.WithError(err2).Error("Failed to unmount CimFS")
				}
			}
		}()
		mountPath := fmt.Sprintf(`\\?\Volume{%s}\`, g2.String())
		parentMountPaths = append(parentMountPaths, mountPath)
	}

	logrus.WithFields(logrus.Fields{
		"layerPath":        m.Source,
		"parentLayerPaths": parentMountPaths,
	}).Info("PrepareLayer")
	if err := hcsshim.PrepareLayer(hcsshim.DriverInfo{}, m.Source, parentMountPaths); err != nil {
		return errors.Wrap(err, "failed to prepare layer")
	}

	mounts[m.Source] = *m
	return nil
}

func (m *Mount) mountLegacy(target string) error {
	home, layerID := filepath.Split(m.Source)

	parentLayerPaths, err := m.GetParentPaths()
	if err != nil {
		return err
	}

	var di = hcsshim.DriverInfo{
		HomeDir: home,
	}

	if err = hcsshim.ActivateLayer(di, layerID); err != nil {
		return errors.Wrapf(err, "failed to activate layer %s", m.Source)
	}
	defer func() {
		if err != nil {
			hcsshim.DeactivateLayer(di, layerID)
		}
	}()

	if err = hcsshim.PrepareLayer(di, layerID, parentLayerPaths); err != nil {
		return errors.Wrapf(err, "failed to prepare layer %s", m.Source)
	}

	mounts[m.Source] = *m
	return nil
}

// ParentLayerPathsFlag is the options flag used to represent the JSON encoded
// list of parent layers required to use the layer
const ParentLayerPathsFlag = "parentLayerPaths="

// GetParentPaths of the mount
func (m *Mount) GetParentPaths() ([]string, error) {
	switch m.Type {
	case "windows-layer":
		var parentLayerPaths []string
		for _, option := range m.Options {
			if strings.HasPrefix(option, ParentLayerPathsFlag) {
				err := json.Unmarshal([]byte(option[len(ParentLayerPathsFlag):]), &parentLayerPaths)
				if err != nil {
					return nil, errors.Wrap(err, "failed to unmarshal parent layer paths from mount")
				}
			}
		}
		return parentLayerPaths, nil
	case "cimfs":
		var parentPaths []string
		parentPathOptPrefix := "parentLayerPaths="
		for _, opt := range m.Options {
			if strings.HasPrefix(opt, parentPathOptPrefix) {
				if err := json.Unmarshal([]byte(opt[len(parentPathOptPrefix):]), &parentPaths); err != nil {
					return nil, err
				}
			}
		}
		parentMountPaths := make([]string, 0, len(parentPaths))
		for _, parentPath := range parentPaths {
			g, err := hcsshim.NameToGuid(parentPath)
			if err != nil {
				return nil, errors.Wrap(err, "failed to convert name to GUID")
			}
			g2 := guid.FromWindowsArray(g)
			mountPath := fmt.Sprintf(`\\?\Volume{%s}\`, g2.String())
			parentMountPaths = append(parentMountPaths, mountPath)
		}
		return parentMountPaths, nil
	default:
		return nil, errors.Errorf("invalid windows mount type: '%s'", m.Type)
	}
}

func (m *Mount) unmountCimFS() error {
	if err := hcsshim.UnprepareLayer(hcsshim.DriverInfo{}, m.Source); err != nil {
		return errors.Wrapf(err, "failed to unprepare layer %s", m.Source)
	}

	var parentPaths []string
	parentPathOptPrefix := "parentLayerPaths="
	for _, opt := range m.Options {
		if strings.HasPrefix(opt, parentPathOptPrefix) {
			if err := json.Unmarshal([]byte(opt[len(parentPathOptPrefix):]), &parentPaths); err != nil {
				return err
			}
		}
	}
	for _, parentPath := range parentPaths {
		g, err := hcsshim.NameToGuid(parentPath)
		if err != nil {
			return errors.Wrap(err, "failed to convert name to GUID")
		}
		g2 := guid.FromWindowsArray(g)
		if err := cimfs.UnmountImage(g2); err != nil {
			return errors.Wrap(err, "failed to unmount CimFS")
		}
	}

	if err := hcsshim.DeactivateLayer(hcsshim.DriverInfo{}, m.Source); err != nil {
		return errors.Wrapf(err, "failed to deactivate layer %s", m.Source)
	}

	return nil
}

func (m *Mount) unmountLegacy() error {
	logrus.Info("Unmount")

	if err := hcsshim.UnprepareLayer(hcsshim.DriverInfo{}, m.Source); err != nil {
		return errors.Wrapf(err, "failed to unprepare layer %s", m.Source)
	}

	if err := hcsshim.DeactivateLayer(hcsshim.DriverInfo{}, m.Source); err != nil {
		return errors.Wrapf(err, "failed to deactivate layer %s", m.Source)
	}

	return nil
}

// Unmount the mount at the provided path
func Unmount(mount string, flags int) error {
	logrus.WithField("mount", mount).Info("Unmount")
	m, ok := mounts[mount]
	if !ok {
		return errors.Errorf("unknown mount path: %s", mount)
	}

	switch m.Type {
	case "windows-layer":
		return m.unmountLegacy()
	case "cimfs":
		return m.unmountCimFS()
	default:
		return errors.Errorf("invalid windows mount type: '%s'", m.Type)
	}
}

// UnmountAll unmounts from the provided path
func UnmountAll(mount string, flags int) error {
	return Unmount(mount, flags)
}
