package cimfs

import (
	"os"
	"unsafe"

	"github.com/Microsoft/go-winio"
	"github.com/Microsoft/go-winio/pkg/guid"
	"github.com/pkg/errors"
	"golang.org/x/sys/windows"
)

// FileInfo represents the metadata for a single file in the image.
type FileInfo struct {
	Size int64

	CreationTime   windows.Filetime
	LastWriteTime  windows.Filetime
	ChangeTime     windows.Filetime
	LastAccessTime windows.Filetime

	Attributes uint32

	SecurityDescriptor []byte
	ReparseData        []byte
	EAs                []winio.ExtendedAttribute
}

type fileInfoInternal struct {
	Attributes uint32
	FileSize   int64

	CreationTime   windows.Filetime
	LastWriteTime  windows.Filetime
	ChangeTime     windows.Filetime
	LastAccessTime windows.Filetime

	SecurityDescriptorBuffer unsafe.Pointer
	SecurityDescriptorSize   uint32

	ReparseDataBuffer unsafe.Pointer
	ReparseDataSize   uint32

	EAs     unsafe.Pointer
	EACount uint32
}

type fsHandle uintptr
type streamHandle uintptr

// FileSystem represents a single CimFS filesystem. On disk, the image is
// composed of a filesystem file and several object ID and region files.
type FileSystem struct {
	handle       fsHandle
	activeStream streamHandle
}

// Open opens an existing CimFS filesystem, or creates one if it doesn't exist.
func Open(imagePath string, oldFSName string, newFSName string) (*FileSystem, error) {
	if err := os.MkdirAll(imagePath, 0); err != nil {
		return nil, err
	}
	var err error
	var oldNameBytes *uint16
	if oldFSName != "" {
		oldFSName = oldFSName + ".fs"
		oldNameBytes, err = windows.UTF16PtrFromString(oldFSName)
		if err != nil {
			return nil, err
		}
	}
	var newNameBytes *uint16
	if newFSName != "" {
		newFSName = newFSName + ".fs"
		newNameBytes, err = windows.UTF16PtrFromString(newFSName)
		if err != nil {
			return nil, err
		}
	}
	var handle fsHandle
	if err := cimCreateImage(imagePath, oldNameBytes, newNameBytes, &handle); err != nil {
		return nil, err
	}

	return &FileSystem{handle: handle}, nil
}

// AddFile adds an entry for a file to the image. The file is added at the
// specified path. After calling this function, the file is set as the active
// stream for the image, so data can be written by calling `Write`.
func (fs *FileSystem) AddFile(path string, info *FileInfo) error {
	infoInternal := &fileInfoInternal{
		Attributes:     info.Attributes,
		FileSize:       info.Size,
		CreationTime:   info.CreationTime,
		LastWriteTime:  info.LastWriteTime,
		ChangeTime:     info.ChangeTime,
		LastAccessTime: info.LastAccessTime,
	}

	if len(info.SecurityDescriptor) > 0 {
		infoInternal.SecurityDescriptorBuffer = unsafe.Pointer(&info.SecurityDescriptor[0])
		infoInternal.SecurityDescriptorSize = uint32(len(info.SecurityDescriptor))
	}

	if len(info.ReparseData) > 0 {
		infoInternal.ReparseDataBuffer = unsafe.Pointer(&info.ReparseData[0])
		infoInternal.ReparseDataSize = uint32(len(info.ReparseData))
	}

	if len(info.EAs) > 0 {
		buf, err := winio.EncodeExtendedAttributes(info.EAs)
		if err != nil {
			return err
		}
		infoInternal.EAs = unsafe.Pointer(&buf[0])
		infoInternal.EACount = uint32(len(buf))
	}

	return cimCreateFile(fs.handle, path, infoInternal, &fs.activeStream)
}

// Write writes bytes to the active stream.
func (fs *FileSystem) Write(p []byte) (int, error) {
	if fs.activeStream == 0 {
		return 0, errors.New("No active stream")
	}

	err := cimWriteStream(fs.activeStream, uintptr(unsafe.Pointer(&p[0])), uint32(len(p)))
	if err != nil {
		return 0, err
	}

	return len(p), nil
}

// CloseStream closes the active stream.
func (fs *FileSystem) CloseStream() error {
	if fs.activeStream == 0 {
		return errors.New("No active stream")
	}

	return cimCloseStream(fs.activeStream)
}

// TODO do this as part of Close?
func (fs *FileSystem) Commit() error {
	return cimCommitImage(fs.handle)
}

// Close closes the CimFS filesystem.
func (fs *FileSystem) Close() error {
	return cimCloseImage(fs.handle)
}

// RemoveFile deletes the file at `path` from the image.
func (fs *FileSystem) RemoveFile(path string) error {
	return cimDeletePath(fs.handle, path)
}

// AddLink adds a hard link from `oldPath` to `newPath` in the image.
func (fs *FileSystem) AddLink(oldPath string, newPath string) error {
	return cimCreateHardLink(fs.handle, newPath, oldPath)
}

// MountImage mounts the CimFS image at `path` to the volume `volumeGUID`.
func MountImage(imagePath string, fsName string, volumeGUID guid.GUID) error {
	fsName = fsName + ".fs"
	return cimMountImage(imagePath, fsName, 0, &volumeGUID)
}

// UnmountImage unmounts the CimFS volume `volumeGUID`.
func UnmountImage(volumeGUID guid.GUID) error {
	return cimDismountImage(&volumeGUID)
}
