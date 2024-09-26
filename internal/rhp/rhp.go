package rhp

/*
#cgo LDFLAGS: ${SRCDIR}/librhp.a -ldl
#include "rhp.h"
#include <stdlib.h>
#include <stddef.h>
*/
import "C"

import (
	"unsafe"

	"go.sia.tech/core/types"
)

const SectorSize = 1 << 22

func SectorRoot(sector *[SectorSize]byte) (h types.Hash256) {
	// Call the sector_root function. The pointer must be an array of 4MiB
	C.sector_root(
		(*C.uint8_t)(unsafe.Pointer(&sector[0])),
		(*C.uint8_t)(unsafe.Pointer(&h[0])),
	)
	return
}
