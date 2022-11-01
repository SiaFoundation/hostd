package rhp

import "go.sia.tech/hostd/host/registry"

type (
	instruction interface {
		rpcObject
		RequiresContract() bool
		RequiresFinalization() bool
	}

	// instrAppendSector stores a sector on the host and appends its Merkle root
	// to a contract.
	instrAppendSector struct {
		SectorDataOffset uint64
		ProofRequired    bool
	}

	// instrAppendSectorRoot appends a sector root to a contract
	instrAppendSectorRoot struct {
		MerkleRootOffset uint64
		ProofRequired    bool
	}

	// instrDropSectors removes the last n sectors from a contract and deletes
	// them from the host.
	instrDropSectors struct {
		SectorCountOffset uint64
		ProofRequired     bool
	}

	// instrHasSector checks if a sector is present on the host.
	instrHasSector struct {
		MerkleRootOffset uint64
	}

	// instrReadOffset reads a range of bytes from an offset in the contract.
	instrReadOffset struct {
		LengthOffset  uint64
		OffsetOffset  uint64
		ProofRequired bool
	}

	// instrReadSector reads a range of bytes from a sector.
	instrReadSector struct {
		LengthOffset     uint64
		OffsetOffset     uint64
		MerkleRootOffset uint64
		ProofRequired    bool
	}

	// instrSwapSector swaps two sectors in a contract
	instrSwapSector struct {
		Sector1Offset uint64
		Sector2Offset uint64
		ProofRequired bool
	}

	// instrUpdateSector overwrites data in an existing sector.
	instrUpdateSector struct {
		Offset        uint64
		Length        uint64
		DataOffset    uint64
		ProofRequired bool
	}

	// instrStoreSector temporarily stores a sector on the host. The sector is
	// not associated with any contract and collateral is not risked.
	instrStoreSector struct {
		DataOffset uint64
		Duration   uint64
	}

	// instrRevision returns the latest revision of a contract
	instrRevision struct {
	}

	// instrReadRegistry reads a value from the registry
	instrReadRegistry struct {
		PublicKeyOffset uint64
		PublicKeyLength uint64
		TweakOffset     uint64
		Version         uint8
	}

	// instrReadRegistryNoVersion reads a pre-1.5.7 read registry instruction
	// without the version byte
	instrReadRegistryNoVersion struct {
		instrReadRegistry
	}

	instrUpdateRegistry struct {
		TweakOffset     uint64
		RevisionOffset  uint64
		SignatureOffset uint64
		PublicKeyOffset uint64
		PublicKeyLength uint64
		DataOffset      uint64
		DataLength      uint64
		EntryType       registry.ValueType
	}

	// instrUpdateRegistryNoType reads a pre-1.5.7 update registry instruction
	// without the entry type byte
	instrUpdateRegistryNoType struct {
		instrUpdateRegistry
	}
)

var (
	idInstrAppendSector     = newSpecifier("AppendSector")
	idInstrAppendSectorRoot = newSpecifier("AppendSectorRoot")
	idInstrDropSectors      = newSpecifier("DropSectors")
	idInstrHasSector        = newSpecifier("HasSector")
	idInstrStoreSector      = newSpecifier("StoreSector")
	idInstrUpdateSector     = newSpecifier("UpdateSector")
	idInstrReadOffset       = newSpecifier("ReadOffset")
	idInstrReadSector       = newSpecifier("ReadSector")
	idInstrContractRevision = newSpecifier("Revision")
	idInstrSectorRoots      = newSpecifier("SectorRoots")
	idInstrSwapSector       = newSpecifier("SwapSector")
	idInstrUpdateRegistry   = newSpecifier("UpdateRegistry")
	idInstrReadRegistry     = newSpecifier("ReadRegistry")
	idInstrReadRegistrySID  = newSpecifier("ReadRegistrySID")
)

func (i *instrAppendSector) RequiresContract() bool {
	return true
}

func (i *instrAppendSector) RequiresFinalization() bool {
	return true
}

func (i *instrAppendSector) encodedLen() int {
	return 9
}

func (i *instrAppendSector) encodeTo(e *encoder) {
	e.WriteUint64(i.SectorDataOffset)
	e.WriteBool(i.ProofRequired)
}

func (i *instrAppendSector) decodeFrom(d *decoder) error {
	i.SectorDataOffset = d.ReadUint64()
	i.ProofRequired = d.ReadBool()
	return d.Err()
}

func (i *instrAppendSectorRoot) RequiresContract() bool {
	return true
}

func (i *instrAppendSectorRoot) RequiresFinalization() bool {
	return true
}

func (i *instrAppendSectorRoot) encodedLen() int {
	return 9
}

func (i *instrAppendSectorRoot) encodeTo(e *encoder) {
	e.WriteUint64(i.MerkleRootOffset)
	e.WriteBool(i.ProofRequired)
}

func (i *instrAppendSectorRoot) decodeFrom(d *decoder) error {
	i.MerkleRootOffset = d.ReadUint64()
	i.ProofRequired = d.ReadBool()
	return d.Err()
}

func (i *instrDropSectors) RequiresContract() bool {
	return true
}

func (i *instrDropSectors) RequiresFinalization() bool {
	return true
}

func (i *instrDropSectors) encodedLen() int {
	return 9
}

func (i *instrDropSectors) encodeTo(e *encoder) {
	e.WriteUint64(i.SectorCountOffset)
	e.WriteBool(i.ProofRequired)
}

func (i *instrDropSectors) decodeFrom(d *decoder) error {
	i.SectorCountOffset = d.ReadUint64()
	i.ProofRequired = d.ReadBool()
	return d.Err()
}

func (i *instrHasSector) RequiresContract() bool {
	return false
}

func (i *instrHasSector) RequiresFinalization() bool {
	return false
}

func (i *instrHasSector) encodedLen() int {
	return 8
}

func (i *instrHasSector) encodeTo(e *encoder) {
	e.WriteUint64(i.MerkleRootOffset)
}

func (i *instrHasSector) decodeFrom(d *decoder) error {
	i.MerkleRootOffset = d.ReadUint64()
	return d.Err()
}

func (i *instrReadOffset) RequiresContract() bool {
	return true
}

func (i *instrReadOffset) RequiresFinalization() bool {
	return false
}

func (i *instrReadOffset) encodedLen() int {
	return 17
}

func (i *instrReadOffset) encodeTo(e *encoder) {
	e.WriteUint64(i.LengthOffset)
	e.WriteUint64(i.OffsetOffset)
	e.WriteBool(i.ProofRequired)
}

func (i *instrReadOffset) decodeFrom(d *decoder) error {
	i.LengthOffset = d.ReadUint64()
	i.OffsetOffset = d.ReadUint64()
	i.ProofRequired = d.ReadBool()
	return d.Err()
}

func (i *instrReadSector) RequiresContract() bool {
	return true
}

func (i *instrReadSector) RequiresFinalization() bool {
	return false
}

func (i *instrReadSector) encodedLen() int {
	return 25
}

func (i *instrReadSector) encodeTo(e *encoder) {
	e.WriteUint64(i.LengthOffset)
	e.WriteUint64(i.OffsetOffset)
	e.WriteUint64(i.MerkleRootOffset)
	e.WriteBool(i.ProofRequired)
}

func (i *instrReadSector) decodeFrom(d *decoder) error {
	i.LengthOffset = d.ReadUint64()
	i.OffsetOffset = d.ReadUint64()
	i.MerkleRootOffset = d.ReadUint64()
	i.ProofRequired = d.ReadBool()
	return d.Err()
}

func (i *instrSwapSector) RequiresContract() bool {
	return true
}

func (i *instrSwapSector) RequiresFinalization() bool {
	return true
}

func (i *instrSwapSector) encodedLen() int {
	return 17
}

func (i *instrSwapSector) encodeTo(e *encoder) {
	e.WriteUint64(i.Sector1Offset)
	e.WriteUint64(i.Sector2Offset)
	e.WriteBool(i.ProofRequired)
}

func (i *instrSwapSector) decodeFrom(d *decoder) error {
	i.Sector1Offset = d.ReadUint64()
	i.Sector2Offset = d.ReadUint64()
	i.ProofRequired = d.ReadBool()
	return d.Err()
}

func (i *instrUpdateSector) RequiresContract() bool {
	return true
}

func (i *instrUpdateSector) RequiresFinalization() bool {
	return true
}

func (i *instrUpdateSector) encodedLen() int {
	return 25
}

func (i *instrUpdateSector) encodeTo(e *encoder) {
	e.WriteUint64(i.Offset)
	e.WriteUint64(i.Length)
	e.WriteUint64(i.DataOffset)
	e.WriteBool(i.ProofRequired)
}

func (i *instrUpdateSector) decodeFrom(d *decoder) error {
	i.Offset = d.ReadUint64()
	i.Length = d.ReadUint64()
	i.DataOffset = d.ReadUint64()
	i.ProofRequired = d.ReadBool()
	return d.Err()
}

func (i *instrStoreSector) RequiresContract() bool {
	return false
}

func (i *instrStoreSector) RequiresFinalization() bool {
	return false
}

func (i *instrStoreSector) encodedLen() int {
	return 16
}

func (i *instrStoreSector) encodeTo(e *encoder) {
	e.WriteUint64(i.DataOffset)
	e.WriteUint64(i.Duration)
}

func (i *instrStoreSector) decodeFrom(d *decoder) error {
	i.DataOffset = d.ReadUint64()
	i.Duration = d.ReadUint64()
	return d.Err()
}

func (i *instrRevision) RequiresContract() bool {
	return true
}

func (i *instrRevision) RequiresFinalization() bool {
	return false
}

func (i *instrRevision) encodedLen() int {
	return 0
}

func (i *instrRevision) encodeTo(e *encoder) {
}

func (i *instrRevision) decodeFrom(d *decoder) error {
	return d.Err()
}

func (i *instrReadRegistry) RequiresContract() bool {
	return false
}

func (i *instrReadRegistry) RequiresFinalization() bool {
	return false
}

func (i *instrReadRegistry) encodedLen() int {
	return 25
}

func (i *instrReadRegistry) encodeTo(e *encoder) {
	e.WriteUint64(i.PublicKeyOffset)
	e.WriteUint64(i.PublicKeyLength)
	e.WriteUint64(i.TweakOffset)
	e.WriteUint8(i.Version)
}

func (i *instrReadRegistry) decodeFrom(d *decoder) error {
	i.PublicKeyOffset = d.ReadUint64()
	i.PublicKeyLength = d.ReadUint64()
	i.TweakOffset = d.ReadUint64()
	i.Version = d.ReadUint8()
	return d.Err()
}

func (i *instrReadRegistryNoVersion) encodedLen() int {
	return 24
}

func (i *instrReadRegistryNoVersion) encodeTo(e *encoder) {
	e.WriteUint64(i.PublicKeyOffset)
	e.WriteUint64(i.PublicKeyLength)
	e.WriteUint64(i.TweakOffset)
}

func (i *instrReadRegistryNoVersion) decodeFrom(d *decoder) error {
	i.PublicKeyOffset = d.ReadUint64()
	i.PublicKeyLength = d.ReadUint64()
	i.TweakOffset = d.ReadUint64()
	i.Version = 1
	return d.Err()
}

func (i *instrUpdateRegistry) RequiresContract() bool {
	return false
}

func (i *instrUpdateRegistry) RequiresFinalization() bool {
	return false
}

func (i *instrUpdateRegistry) encodedLen() int {
	return 57
}

func (i *instrUpdateRegistry) encodeTo(e *encoder) {
	e.WriteUint64(i.TweakOffset)
	e.WriteUint64(i.RevisionOffset)
	e.WriteUint64(i.SignatureOffset)
	e.WriteUint64(i.PublicKeyOffset)
	e.WriteUint64(i.PublicKeyLength)
	e.WriteUint8(uint8(i.EntryType))
}

func (i *instrUpdateRegistry) decodeFrom(d *decoder) error {
	i.TweakOffset = d.ReadUint64()
	i.RevisionOffset = d.ReadUint64()
	i.SignatureOffset = d.ReadUint64()
	i.PublicKeyOffset = d.ReadUint64()
	i.PublicKeyLength = d.ReadUint64()
	i.EntryType = registry.ValueType(d.ReadUint8())
	return d.Err()
}

func (i *instrUpdateRegistryNoType) encodedLen() int {
	return 56
}

func (i *instrUpdateRegistryNoType) encodeTo(e *encoder) {
	e.WriteUint64(i.TweakOffset)
	e.WriteUint64(i.RevisionOffset)
	e.WriteUint64(i.SignatureOffset)
	e.WriteUint64(i.PublicKeyOffset)
	e.WriteUint64(i.PublicKeyLength)
}

func (i *instrUpdateRegistryNoType) decodeFrom(d *decoder) error {
	i.TweakOffset = d.ReadUint64()
	i.RevisionOffset = d.ReadUint64()
	i.SignatureOffset = d.ReadUint64()
	i.PublicKeyOffset = d.ReadUint64()
	i.PublicKeyLength = d.ReadUint64()
	i.EntryType = registry.EntryTypeArbitrary
	return d.Err()
}
