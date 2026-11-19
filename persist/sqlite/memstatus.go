package sqlite

import "C"

import "fmt"

// Disable Memory statistics since they take a global mutex on every allocation.
// Must run before SQLite is initialized.
//
// This is the same as building SQLite with SQLITE_DISABLE_MEMSTATUS=0 which is
// one of the recommended build flags. See https://www.sqlite.org/compile.html
func init() {
	if rc := C.disableMemstatus(); rc != 0 {
		panic(fmt.Sprintf("failed to disable sqlite memstatus: %d", rc))
	}
}
