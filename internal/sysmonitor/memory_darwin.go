//go:build darwin && cgo

package sysmonitor

/*
#include <mach/mach.h>
#include <mach/mach_host.h>
#include <sys/sysctl.h>

kern_return_t get_vm_stats(vm_statistics64_t vmstat) {
    mach_msg_type_number_t count = HOST_VM_INFO64_COUNT;
    mach_port_t host_port = mach_host_self();
    kern_return_t ret = host_statistics64(host_port, HOST_VM_INFO64, (host_info64_t)vmstat, &count);

    // Important: mach_host_self() returns a port send right that must be deallocated,
    // unlike mach_task_self() which usually doesn't.
    // However, standard practice often keeps host_port cached.
    // Given the snippet, we'll ensure we don't leak the port reference if possible,
    // though mach_host_self implementation details vary.
    mach_port_deallocate(mach_task_self(), host_port);

    return ret;
}

uint64_t get_hw_memsize() {
    uint64_t memsize = 0;
    size_t len = sizeof(memsize);
    int mib[2] = {CTL_HW, HW_MEMSIZE};
    sysctl(mib, 2, &memsize, &len, NULL, 0);
    return memsize;
}
*/
import "C"

import (
	"fmt"
)

type darwinMemoryReader struct{}

// newPlatformMemoryReader is the factory entry point.
func newPlatformMemoryReader(_ FileSystem) ProcessMemoryReader {
	return &darwinMemoryReader{}
}

// Sample returns the current system memory statistics.
func (d *darwinMemoryReader) Sample() (SystemMemory, error) {
	total := uint64(C.get_hw_memsize())
	if total == 0 {
		return SystemMemory{}, fmt.Errorf("failed to get total memory via sysctl")
	}

	var vmStat C.vm_statistics64_data_t
	if ret := C.get_vm_stats(&vmStat); ret != C.KERN_SUCCESS {
		return SystemMemory{}, fmt.Errorf("failed to get host VM statistics: kern_return_t=%d", ret)
	}

	// Page size is usually 16384 (16kb) on M1/M2/M3 (ARM64) and 4096 (4kb) on Intel.
	// C.vm_kernel_page_size handles this automatically.
	pageSize := uint64(C.vm_kernel_page_size)

	free := uint64(vmStat.free_count) * pageSize
	inactive := uint64(vmStat.inactive_count) * pageSize
	speculative := uint64(vmStat.speculative_count) * pageSize

	// "Available" memory on macOS is generally considered to be:
	// Free + Inactive (file cache that can be dropped) + Speculative
	return SystemMemory{
		Total:     total,
		Available: free + inactive + speculative,
	}, nil
}
