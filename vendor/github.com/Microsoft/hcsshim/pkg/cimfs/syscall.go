package cimfs

import (
	"github.com/Microsoft/go-winio/pkg/guid"
)

type g = guid.GUID

//go:generate go run ../../mksyscall_windows.go -output zsyscall_windows.go syscall.go

//sys cimMountImage(imagePath string, fsName string, flags uint32, volumeID *g) (hr error) = cimfs.CimMountImage
//sys cimDismountImage(volumeID *g) (hr error) = cimfs.CimDismountImage

//sys cimCreateImage(imagePath string, oldFSName *uint16, newFSName *uint16, cimFSHandle *fsHandle) (hr error) = cimfs.CimCreateImage
//sys cimCloseImage(cimFSHandle fsHandle) (hr error) = cimfs.CimCloseImage
//sys cimCommitImage(cimFSHandle fsHandle) (hr error) = cimfs.CimCommitImage

//sys cimCreateFile(cimFSHandle fsHandle, path string, file *fileInfoInternal, cimStreamHandle *streamHandle) (hr error) = cimfs.CimCreateFile
//sys cimCloseStream(cimStreamHandle streamHandle) (hr error) = cimfs.CimCloseStream
//sys cimWriteStream(cimStreamHandle streamHandle, buffer uintptr, bufferSize uint32) (hr error) = cimfs.CimWriteStream
//sys cimDeletePath(cimFSHandle fsHandle, path string) (hr error) = cimfs.CimDeletePath
//sys cimCreateHardLink(cimFSHandle fsHandle, newPath string, oldPath string) (hr error) = cimfs.CimCreateHardLink
