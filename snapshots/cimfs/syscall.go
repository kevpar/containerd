package cimfs

//go:generate go run $GOROOT/src/syscall/mksyscall_windows.go -output zsyscall_windows.go syscall.go

//sys hcsFormatWritableLayerVhd(handle uintptr) (hr error) = computestorage.HcsFormatWritableLayerVhd
//sys hcsGetLayerVhdMountPath(handle uintptr, mountPath **uint16) (hr error) = computestorage.HcsGetLayerVhdMountPath

type attachVirtualDiskParameters struct {
	Version uint32
	Reserved uint32
}

const (
	ATTACH_VIRTUAL_DISK_FLAG_NO_DRIVE_LETTER uint32 = 2
	ATTACH_VIRTUAL_DISK_FLAG_BYPASS_DEFAULT_ENCRYPTION_POLICY uint32 = 6
)

//sys attachVirtualDisk(handle syscall.Handle, secDesc uintptr, flags uint32, providerFlags uint32, parameters *attachVirtualDiskParameters, overlapped uintptr) (err error) [failretval != 0] = VirtDisk.AttachVirtualDisk