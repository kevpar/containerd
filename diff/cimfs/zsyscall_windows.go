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
	modoffreg = windows.NewLazySystemDLL("offreg.dll")

	procORMergeHives = modoffreg.NewProc("ORMergeHives")
	procOROpenHive   = modoffreg.NewProc("OROpenHive")
	procORCloseHive  = modoffreg.NewProc("ORCloseHive")
	procORSaveHive   = modoffreg.NewProc("ORSaveHive")
)

func orMergeHives(hiveHandles []orHKey, result *orHKey) (win32err error) {
	var _p0 *orHKey
	if len(hiveHandles) > 0 {
		_p0 = &hiveHandles[0]
	}
	r0, _, _ := syscall.Syscall(procORMergeHives.Addr(), 3, uintptr(unsafe.Pointer(_p0)), uintptr(len(hiveHandles)), uintptr(unsafe.Pointer(result)))
	if r0 != 0 {
		win32err = syscall.Errno(r0)
	}
	return
}

func orOpenHive(hivePath string, result *orHKey) (win32err error) {
	var _p0 *uint16
	_p0, win32err = syscall.UTF16PtrFromString(hivePath)
	if win32err != nil {
		return
	}
	return _orOpenHive(_p0, result)
}

func _orOpenHive(hivePath *uint16, result *orHKey) (win32err error) {
	r0, _, _ := syscall.Syscall(procOROpenHive.Addr(), 2, uintptr(unsafe.Pointer(hivePath)), uintptr(unsafe.Pointer(result)), 0)
	if r0 != 0 {
		win32err = syscall.Errno(r0)
	}
	return
}

func orCloseHive(handle orHKey) (win32err error) {
	r0, _, _ := syscall.Syscall(procORCloseHive.Addr(), 1, uintptr(handle), 0, 0)
	if r0 != 0 {
		win32err = syscall.Errno(r0)
	}
	return
}

func orSaveHive(handle orHKey, hivePath string, osMajorVersion uint32, osMinorVersion uint32) (win32err error) {
	var _p0 *uint16
	_p0, win32err = syscall.UTF16PtrFromString(hivePath)
	if win32err != nil {
		return
	}
	return _orSaveHive(handle, _p0, osMajorVersion, osMinorVersion)
}

func _orSaveHive(handle orHKey, hivePath *uint16, osMajorVersion uint32, osMinorVersion uint32) (win32err error) {
	r0, _, _ := syscall.Syscall6(procORSaveHive.Addr(), 4, uintptr(handle), uintptr(unsafe.Pointer(hivePath)), uintptr(osMajorVersion), uintptr(osMinorVersion), 0, 0)
	if r0 != 0 {
		win32err = syscall.Errno(r0)
	}
	return
}
