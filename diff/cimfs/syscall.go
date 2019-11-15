package cimfs

//go:generate go run mksyscall_windows.go -output zsyscall_windows.go syscall.go

//sys orMergeHives(hiveHandles []orHKey, result *orHKey) (win32err error) = offreg.ORMergeHives
//sys orOpenHive(hivePath string, result *orHKey) (win32err error) = offreg.OROpenHive
//sys orCloseHive(handle orHKey) (win32err error) = offreg.ORCloseHive
//sys orSaveHive(handle orHKey, hivePath string, osMajorVersion uint32, osMinorVersion uint32) (win32err error) = offreg.ORSaveHive

type orHKey uintptr
