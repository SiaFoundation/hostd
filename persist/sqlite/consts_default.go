//go:build !testing

package sqlite

const (
	// the number of records to limit long-running sector queries to
	sqlSectorBatchSize = 256 // 1 GiB
)
