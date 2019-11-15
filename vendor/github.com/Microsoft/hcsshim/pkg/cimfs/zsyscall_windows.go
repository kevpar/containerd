// Code generated mksyscall_windows.exe DO NOT EDIT

package cimfs

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

var _ unsafe.Pointer

// Do the interface allocations only once for common
// Errno values.
const (
	errnoERROR_IO_PENDING = 997
)

var (
	errERROR_IO_PENDING error = syscall.Errno(errnoERROR_IO_PENDING)
)

// errnoErr returns common boxed Errno values, to prevent
// allocations at runtime.
func errnoErr(e syscall.Errno) error {
	switch e {
	case 0:
		return nil
	case errnoERROR_IO_PENDING:
		return errERROR_IO_PENDING
	}
	// TODO: add more here, after collecting data on the common
	// error values see on Windows. (perhaps when running
	// all.bat?)
	return e
}

var (
	modcimfs = windows.NewLazySystemDLL("cimfs.dll")

	procCimMountImage     = modcimfs.NewProc("CimMountImage")
	procCimDismountImage  = modcimfs.NewProc("CimDismountImage")
	procCimCreateImage    = modcimfs.NewProc("CimCreateImage")
	procCimCloseImage     = modcimfs.NewProc("CimCloseImage")
	procCimCommitImage    = modcimfs.NewProc("CimCommitImage")
	procCimCreateFile     = modcimfs.NewProc("CimCreateFile")
	procCimCloseStream    = modcimfs.NewProc("CimCloseStream")
	procCimWriteStream    = modcimfs.NewProc("CimWriteStream")
	procCimDeletePath     = modcimfs.NewProc("CimDeletePath")
	procCimCreateHardLink = modcimfs.NewProc("CimCreateHardLink")
)

func cimMountImage(imagePath string, fsName string, flags uint32, volumeID *g) (hr error) {
	var _p0 *uint16
	_p0, hr = syscall.UTF16PtrFromString(imagePath)
	if hr != nil {
		return
	}
	var _p1 *uint16
	_p1, hr = syscall.UTF16PtrFromString(fsName)
	if hr != nil {
		return
	}
	return _cimMountImage(_p0, _p1, flags, volumeID)
}

func _cimMountImage(imagePath *uint16, fsName *uint16, flags uint32, volumeID *g) (hr error) {
	r0, _, _ := syscall.Syscall6(procCimMountImage.Addr(), 4, uintptr(unsafe.Pointer(imagePath)), uintptr(unsafe.Pointer(fsName)), uintptr(flags), uintptr(unsafe.Pointer(volumeID)), 0, 0)
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func cimDismountImage(volumeID *g) (hr error) {
	r0, _, _ := syscall.Syscall(procCimDismountImage.Addr(), 1, uintptr(unsafe.Pointer(volumeID)), 0, 0)
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func cimCreateImage(imagePath string, oldFSName *uint16, newFSName *uint16, cimFSHandle *fsHandle) (hr error) {
	var _p0 *uint16
	_p0, hr = syscall.UTF16PtrFromString(imagePath)
	if hr != nil {
		return
	}
	return _cimCreateImage(_p0, oldFSName, newFSName, cimFSHandle)
}

func _cimCreateImage(imagePath *uint16, oldFSName *uint16, newFSName *uint16, cimFSHandle *fsHandle) (hr error) {
	r0, _, _ := syscall.Syscall6(procCimCreateImage.Addr(), 4, uintptr(unsafe.Pointer(imagePath)), uintptr(unsafe.Pointer(oldFSName)), uintptr(unsafe.Pointer(newFSName)), uintptr(unsafe.Pointer(cimFSHandle)), 0, 0)
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func cimCloseImage(cimFSHandle fsHandle) (hr error) {
	r0, _, _ := syscall.Syscall(procCimCloseImage.Addr(), 1, uintptr(cimFSHandle), 0, 0)
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func cimCommitImage(cimFSHandle fsHandle) (hr error) {
	r0, _, _ := syscall.Syscall(procCimCommitImage.Addr(), 1, uintptr(cimFSHandle), 0, 0)
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func cimCreateFile(cimFSHandle fsHandle, path string, file *fileInfoInternal, cimStreamHandle *streamHandle) (hr error) {
	var _p0 *uint16
	_p0, hr = syscall.UTF16PtrFromString(path)
	if hr != nil {
		return
	}
	return _cimCreateFile(cimFSHandle, _p0, file, cimStreamHandle)
}

func _cimCreateFile(cimFSHandle fsHandle, path *uint16, file *fileInfoInternal, cimStreamHandle *streamHandle) (hr error) {
	r0, _, _ := syscall.Syscall6(procCimCreateFile.Addr(), 4, uintptr(cimFSHandle), uintptr(unsafe.Pointer(path)), uintptr(unsafe.Pointer(file)), uintptr(unsafe.Pointer(cimStreamHandle)), 0, 0)
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func cimCloseStream(cimStreamHandle streamHandle) (hr error) {
	r0, _, _ := syscall.Syscall(procCimCloseStream.Addr(), 1, uintptr(cimStreamHandle), 0, 0)
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func cimWriteStream(cimStreamHandle streamHandle, buffer uintptr, bufferSize uint32) (hr error) {
	r0, _, _ := syscall.Syscall(procCimWriteStream.Addr(), 3, uintptr(cimStreamHandle), uintptr(buffer), uintptr(bufferSize))
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func cimDeletePath(cimFSHandle fsHandle, path string) (hr error) {
	var _p0 *uint16
	_p0, hr = syscall.UTF16PtrFromString(path)
	if hr != nil {
		return
	}
	return _cimDeletePath(cimFSHandle, _p0)
}

func _cimDeletePath(cimFSHandle fsHandle, path *uint16) (hr error) {
	r0, _, _ := syscall.Syscall(procCimDeletePath.Addr(), 2, uintptr(cimFSHandle), uintptr(unsafe.Pointer(path)), 0)
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func cimCreateHardLink(cimFSHandle fsHandle, newPath string, oldPath string) (hr error) {
	var _p0 *uint16
	_p0, hr = syscall.UTF16PtrFromString(newPath)
	if hr != nil {
		return
	}
	var _p1 *uint16
	_p1, hr = syscall.UTF16PtrFromString(oldPath)
	if hr != nil {
		return
	}
	return _cimCreateHardLink(cimFSHandle, _p0, _p1)
}

func _cimCreateHardLink(cimFSHandle fsHandle, newPath *uint16, oldPath *uint16) (hr error) {
	r0, _, _ := syscall.Syscall(procCimCreateHardLink.Addr(), 3, uintptr(cimFSHandle), uintptr(unsafe.Pointer(newPath)), uintptr(unsafe.Pointer(oldPath)))
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}
