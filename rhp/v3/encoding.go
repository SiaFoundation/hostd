package rhp

import (
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"time"
	"unsafe"

	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
)

type (
	// An encoder writes Sia objects to an underlying stream.
	encoder struct {
		w   io.Writer
		buf [1024]byte
		n   int
		err error
	}

	// A decoder reads values from an underlying stream. Callers MUST check
	// (*decoder).Err before using any decoded values.
	decoder struct {
		lr  io.LimitedReader
		buf [64]byte
		err error
	}

	// An rpcObject is an object that can be encoded and decoded for the Sia RPC
	// protocol.
	rpcObject interface {
		encodedLen() int
		encodeTo(*encoder)
		decodeFrom(*decoder) error
	}
)

// Flush writes any pending data to the underlying stream. It returns the first
// error encountered by the encoder.
func (e *encoder) Flush() error {
	if e.err == nil && e.n > 0 {
		_, e.err = e.w.Write(e.buf[:e.n])
		e.n = 0
	}
	return e.err
}

// Write implements io.Writer.
func (e *encoder) Write(p []byte) (int, error) {
	lenp := len(p)
	for e.err == nil && len(p) > 0 {
		if e.n == len(e.buf) {
			e.Flush()
		}
		c := copy(e.buf[e.n:], p)
		e.n += c
		p = p[c:]
	}
	return lenp, e.err
}

// Err returns the first error encountered by the encoder
func (e *encoder) Err() error {
	return e.err
}

// WriteBool writes a bool value to the underlying stream.
func (e *encoder) WriteBool(b bool) {
	var buf [1]byte
	if b {
		buf[0] = 1
	}
	e.Write(buf[:])
}

// WriteUint8 writes a uint8 value to the underlying stream.
func (e *encoder) WriteUint8(u uint8) {
	e.Write([]byte{u})
}

// WriteUint64 writes a uint64 value to the underlying stream.
func (e *encoder) WriteUint64(u uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], u)
	e.Write(buf[:])
}

// WritePrefix writes a length prefix to the underlying stream.
func (e *encoder) WritePrefix(i int) { e.WriteUint64(uint64(i)) }

// WriteTime writes a time.Time value to the underlying stream.
func (e *encoder) WriteTime(t time.Time) { e.WriteUint64(uint64(t.Unix())) }

// WriteBytes writes a length-prefixed []byte to the underlying stream.
func (e *encoder) WriteBytes(b []byte) {
	e.WritePrefix(len(b))
	e.Write(b)
}

// WriteString writes a length-prefixed string to the underlying stream.
func (e *encoder) WriteString(s string) {
	e.WriteBytes([]byte(s))
}

// SetErr sets the decoder's error if it has not already been set. SetErr should
// only be called from DecodeFrom methods.
func (d *decoder) SetErr(err error) error {
	if err != nil && d.err == nil {
		d.err = err
		// clear d.buf so that future reads always return zero
		d.buf = [len(d.buf)]byte{}
	}
	return d.err
}

// Err returns the first error encountered during decoding.
func (d *decoder) Err() error { return d.err }

// Read implements the io.Reader interface. It always returns an error if fewer
// than len(p) bytes were read.
func (d *decoder) Read(p []byte) (int, error) {
	n := 0
	for len(p[n:]) > 0 && d.err == nil {
		want := len(p[n:])
		if want > len(d.buf) {
			want = len(d.buf)
		}
		var read int
		read, d.err = io.ReadFull(&d.lr, d.buf[:want])
		n += copy(p[n:], d.buf[:read])
	}
	return n, d.err
}

// ReadBool reads a bool value from the underlying stream.
func (d *decoder) ReadBool() bool {
	d.Read(d.buf[:1])
	switch d.buf[0] {
	case 0:
		return false
	case 1:
		return true
	default:
		d.SetErr(fmt.Errorf("invalid bool value (%v)", d.buf[0]))
		return false
	}
}

// ReadUint8 reads a uint8 value from the underlying stream.
func (d *decoder) ReadUint8() uint8 {
	d.Read(d.buf[:1])
	return d.buf[0]
}

// ReadUint64 reads a uint64 value from the underlying stream.
func (d *decoder) ReadUint64() uint64 {
	d.Read(d.buf[:8])
	return binary.LittleEndian.Uint64(d.buf[:8])
}

// ReadPrefix reads a length prefix from the underlying stream. If the length
// exceeds the number of bytes remaining in the stream, ReadPrefix sets d.Err
// and returns 0.
func (d *decoder) ReadPrefix() int {
	n := d.ReadUint64()
	if n > uint64(d.lr.N) {
		d.SetErr(fmt.Errorf("encoded object contains invalid length prefix (%v elems > %v bytes left in stream)", n, d.lr.N))
		return 0
	}
	return int(n)
}

// ReadTime reads a time.Time from the underlying stream.
func (d *decoder) ReadTime() time.Time { return time.Unix(int64(d.ReadUint64()), 0).UTC() }

// ReadBytes reads a length-prefixed []byte from the underlying stream.
func (d *decoder) ReadBytes() []byte {
	b := make([]byte, d.ReadPrefix())
	d.Read(b)
	return b
}

// ReadString reads a length-prefixed string from the underlying stream.
func (d *decoder) ReadString() string {
	return string(d.ReadBytes())
}

// newEncoder returns an encoder that wraps the provided stream.
func newEncoder(w io.Writer) *encoder {
	return &encoder{
		w: w,
	}
}

// newDecoder returns a decoder that wraps the provided stream.
func newDecoder(lr io.LimitedReader) *decoder {
	return &decoder{
		lr: lr,
	}
}

func (s *Specifier) encodedLen() int {
	return 16
}

func (s *Specifier) encodeTo(e *encoder) {
	e.Write(s[:])
}

func (s *Specifier) decodeFrom(d *decoder) error {
	d.Read(s[:])
	return d.Err()
}

func (spk *objSiaPublicKey) encodedLen() int {
	return len(spk.Algorithm) + 8 + len(spk.Key)
}

func (spk *objSiaPublicKey) encodeTo(e *encoder) {
	e.Write(spk.Algorithm[:])
	e.WriteBytes(spk.Key)
}

func (spk *objSiaPublicKey) decodeFrom(d *decoder) error {
	d.Read(spk.Algorithm[:])
	spk.Key = d.ReadBytes()
	return d.Err()
}

func (cf *objCoveredFields) foreach(fn func(*[]uint64)) {
	fn(&cf.SiacoinInputs)
	fn(&cf.SiacoinOutputs)
	fn(&cf.FileContracts)
	fn(&cf.FileContractRevisions)
	fn(&cf.StorageProofs)
	fn(&cf.SiafundInputs)
	fn(&cf.SiafundOutputs)
	fn(&cf.MinerFees)
	fn(&cf.ArbitraryData)
	fn(&cf.TransactionSignatures)
}

func (cf *objCoveredFields) encodedLen() int {
	fieldsSize := 0
	cf.foreach(func(f *[]uint64) {
		fieldsSize += 8 + len(*f)*8
	})
	return 1 + fieldsSize
}

func (cf *objCoveredFields) encodeTo(e *encoder) {
	e.WriteBool(cf.WholeTransaction)
	cf.foreach(func(f *[]uint64) {
		e.WritePrefix(len(*f))
		for _, u := range *f {
			e.WriteUint64(u)
		}
	})
}

func (cf *objCoveredFields) decodeFrom(d *decoder) error {
	cf.WholeTransaction = d.ReadBool()
	cf.foreach(func(f *[]uint64) {
		*f = make([]uint64, d.ReadPrefix())
		for i := range *f {
			(*f)[i] = d.ReadUint64()
		}
	})
	return d.Err()
}

func (sig *objTransactionSignature) encodedLen() int {
	return len(sig.ParentID) + 8 + 8 + (*objCoveredFields)(&sig.CoveredFields).encodedLen() + 8 + len(sig.Signature)
}

func (sig *objTransactionSignature) encodeTo(e *encoder) {
	e.Write(sig.ParentID[:])
	e.WriteUint64(sig.PublicKeyIndex)
	e.WriteUint64(uint64(sig.Timelock))
	(*objCoveredFields)(&sig.CoveredFields).encodeTo(e)
	e.WriteBytes(sig.Signature)
}

func (sig *objTransactionSignature) decodeFrom(d *decoder) error {
	d.Read(sig.ParentID[:])
	sig.PublicKeyIndex = d.ReadUint64()
	sig.Timelock = types.BlockHeight(d.ReadUint64())
	(*objCoveredFields)(&sig.CoveredFields).decodeFrom(d)
	sig.Signature = d.ReadBytes()
	return d.Err()
}

func (c *objCurrency) big() *big.Int {
	return &(*struct {
		i big.Int
	})(unsafe.Pointer(c)).i
}

func (c *objCurrency) encodedLen() int {
	const (
		_m    = ^big.Word(0)
		_logS = _m>>8&1 + _m>>16&1 + _m>>32&1
		_S    = 1 << _logS
	)
	bits := c.big().Bits()
	size := len(bits) * _S
zeros:
	for i := len(bits) - 1; i >= 0; i-- {
		for j := _S - 1; j >= 0; j-- {
			if (bits[i] >> uintptr(j*8)) != 0 {
				break zeros
			}
			size--
		}
	}
	return 8 + size
}

func (c *objCurrency) encodeTo(e *encoder) {
	const (
		_m    = ^big.Word(0)
		_logS = _m>>8&1 + _m>>16&1 + _m>>32&1
		_S    = 1 << _logS
	)
	bits := c.big().Bits()
	var i int
	for i = len(bits)*_S - 1; i >= 0; i-- {
		if bits[i/_S]>>(uint(i%_S)*8) != 0 {
			break
		}
	}

	buf := make([]byte, i+1)
	for ; i >= 0; i-- {
		buf[i] = byte(bits[i/_S] >> (uint(i%_S) * 8))
	}
	e.WriteBytes(buf)
}

func (c *objCurrency) decodeFrom(d *decoder) error {
	c.big().SetBytes(d.ReadBytes())
	return d.Err()
}

func (uc *objUnlockConditions) encodedLen() int {
	keysSize := 8
	for i := range uc.PublicKeys {
		keysSize += (*objSiaPublicKey)(&uc.PublicKeys[i]).encodedLen()
	}
	return 8 + 8 + keysSize
}

func (uc *objUnlockConditions) encodeTo(e *encoder) {
	e.WriteUint64(uint64(uc.Timelock))
	e.WritePrefix(len(uc.PublicKeys))
	for i := range uc.PublicKeys {
		(*objSiaPublicKey)(&uc.PublicKeys[i]).encodeTo(e)
	}
	e.WriteUint64(uc.SignaturesRequired)
}

func (uc *objUnlockConditions) decodeFrom(d *decoder) error {
	uc.Timelock = types.BlockHeight(d.ReadUint64())
	uc.PublicKeys = make([]types.SiaPublicKey, d.ReadPrefix())
	for i := range uc.PublicKeys {
		(*objSiaPublicKey)(&uc.PublicKeys[i]).decodeFrom(d)
	}
	uc.SignaturesRequired = d.ReadUint64()
	return d.Err()
}

func (sci *objSiacoinInput) encodedLen() int {
	return len(sci.ParentID) + (*objUnlockConditions)(&sci.UnlockConditions).encodedLen()
}

func (sci *objSiacoinInput) encodeTo(e *encoder) {
	e.Write(sci.ParentID[:])
	(*objUnlockConditions)(&sci.UnlockConditions).encodeTo(e)
}

func (sci *objSiacoinInput) decodeFrom(d *decoder) error {
	d.Read(sci.ParentID[:])
	(*objUnlockConditions)(&sci.UnlockConditions).decodeFrom(d)
	return d.Err()
}

func (sco *objSiacoinOutput) encodedLen() int {
	return len(sco.UnlockHash) + (*objCurrency)(&sco.Value).encodedLen()
}

func (sco *objSiacoinOutput) encodeTo(e *encoder) {
	(*objCurrency)(&sco.Value).encodeTo(e)
	e.Write(sco.UnlockHash[:])
}

func (sco *objSiacoinOutput) decodeFrom(d *decoder) error {
	(*objCurrency)(&sco.Value).decodeFrom(d)
	d.Read(sco.UnlockHash[:])
	return d.Err()
}

func (sfi *objSiafundInput) encodedLen() int {
	return len(sfi.ParentID) + (*objUnlockConditions)(&sfi.UnlockConditions).encodedLen() + len(sfi.ClaimUnlockHash)
}

func (sfi *objSiafundInput) encodeTo(e *encoder) {
	e.Write(sfi.ParentID[:])
	(*objUnlockConditions)(&sfi.UnlockConditions).encodeTo(e)
	e.Write(sfi.ClaimUnlockHash[:])
}

func (sfi *objSiafundInput) decodeFrom(d *decoder) error {
	d.Read(sfi.ParentID[:])
	(*objUnlockConditions)(&sfi.UnlockConditions).decodeFrom(d)
	d.Read(sfi.ClaimUnlockHash[:])
	return d.Err()
}

func (sfo *objSiafundOutput) encodedLen() int {
	return (*objCurrency)(&sfo.Value).encodedLen() + len(sfo.UnlockHash) + (*objCurrency)(&sfo.ClaimStart).encodedLen()
}

func (sfo *objSiafundOutput) encodeTo(e *encoder) {
	(*objCurrency)(&sfo.Value).encodeTo(e)
	e.Write(sfo.UnlockHash[:])
	(*objCurrency)(&sfo.ClaimStart).encodeTo(e)
}

func (sfo *objSiafundOutput) decodeFrom(d *decoder) error {
	(*objCurrency)(&sfo.Value).decodeFrom(d)
	d.Read(sfo.UnlockHash[:])
	(*objCurrency)(&sfo.ClaimStart).decodeFrom(d)
	return d.Err()
}

func (fc *objFileContract) encodedLen() int {
	validSize := 8
	for i := range fc.ValidProofOutputs {
		validSize += (*objSiacoinOutput)(&fc.ValidProofOutputs[i]).encodedLen()
	}
	missedSize := 8
	for i := range fc.MissedProofOutputs {
		missedSize += (*objSiacoinOutput)(&fc.MissedProofOutputs[i]).encodedLen()
	}
	return 8 + len(fc.FileMerkleRoot) + 8 + 8 + (*objCurrency)(&fc.Payout).encodedLen() + validSize + missedSize + len(fc.UnlockHash) + 8
}

func (fc *objFileContract) encodeTo(e *encoder) {
	e.WriteUint64(fc.FileSize)
	e.Write(fc.FileMerkleRoot[:])
	e.WriteUint64(uint64(fc.WindowStart))
	e.WriteUint64(uint64(fc.WindowEnd))
	(*objCurrency)(&fc.Payout).encodeTo(e)
	e.WritePrefix(len(fc.ValidProofOutputs))
	for i := range fc.ValidProofOutputs {
		(*objSiacoinOutput)(&fc.ValidProofOutputs[i]).encodeTo(e)
	}
	e.WritePrefix(len(fc.MissedProofOutputs))
	for i := range fc.MissedProofOutputs {
		(*objSiacoinOutput)(&fc.MissedProofOutputs[i]).encodeTo(e)
	}
	e.Write(fc.UnlockHash[:])
	e.WriteUint64(fc.RevisionNumber)
}

func (fc *objFileContract) decodeFrom(d *decoder) error {
	fc.FileSize = d.ReadUint64()
	d.Read(fc.FileMerkleRoot[:])
	fc.WindowStart = types.BlockHeight(d.ReadUint64())
	fc.WindowEnd = types.BlockHeight(d.ReadUint64())
	(*objCurrency)(&fc.Payout).decodeFrom(d)
	fc.ValidProofOutputs = make([]types.SiacoinOutput, d.ReadPrefix())
	for i := range fc.ValidProofOutputs {
		(*objSiacoinOutput)(&fc.ValidProofOutputs[i]).decodeFrom(d)
	}
	fc.MissedProofOutputs = make([]types.SiacoinOutput, d.ReadPrefix())
	for i := range fc.MissedProofOutputs {
		(*objSiacoinOutput)(&fc.MissedProofOutputs[i]).decodeFrom(d)
	}
	d.Read(fc.UnlockHash[:])
	fc.RevisionNumber = d.ReadUint64()
	return d.Err()
}

func (fcr *objFileContractRevision) encodedLen() int {
	validSize := 8
	for i := range fcr.NewValidProofOutputs {
		validSize += (*objSiacoinOutput)(&fcr.NewValidProofOutputs[i]).encodedLen()
	}
	missedSize := 8
	for i := range fcr.NewMissedProofOutputs {
		missedSize += (*objSiacoinOutput)(&fcr.NewMissedProofOutputs[i]).encodedLen()
	}
	return len(fcr.ParentID) + (*objUnlockConditions)(&fcr.UnlockConditions).encodedLen() + 8 + 8 + len(fcr.NewFileMerkleRoot) + 8 + 8 + validSize + missedSize + len(fcr.NewUnlockHash)
}

func (fcr *objFileContractRevision) encodeTo(e *encoder) {
	e.Write(fcr.ParentID[:])
	(*objUnlockConditions)(&fcr.UnlockConditions).encodeTo(e)
	e.WriteUint64(fcr.NewRevisionNumber)
	e.WriteUint64(fcr.NewFileSize)
	e.Write(fcr.NewFileMerkleRoot[:])
	e.WriteUint64(uint64(fcr.NewWindowStart))
	e.WriteUint64(uint64(fcr.NewWindowEnd))
	e.WritePrefix(len(fcr.NewValidProofOutputs))
	for i := range fcr.NewValidProofOutputs {
		(*objSiacoinOutput)(&fcr.NewValidProofOutputs[i]).encodeTo(e)
	}
	e.WritePrefix(len(fcr.NewMissedProofOutputs))
	for i := range fcr.NewMissedProofOutputs {
		(*objSiacoinOutput)(&fcr.NewMissedProofOutputs[i]).encodeTo(e)
	}
	e.Write(fcr.NewUnlockHash[:])
}

func (fcr *objFileContractRevision) decodeFrom(d *decoder) error {
	d.Read(fcr.ParentID[:])
	(*objUnlockConditions)(&fcr.UnlockConditions).decodeFrom(d)
	fcr.NewRevisionNumber = d.ReadUint64()
	fcr.NewFileSize = d.ReadUint64()
	d.Read(fcr.NewFileMerkleRoot[:])
	fcr.NewWindowStart = types.BlockHeight(d.ReadUint64())
	fcr.NewWindowEnd = types.BlockHeight(d.ReadUint64())
	fcr.NewValidProofOutputs = make([]types.SiacoinOutput, d.ReadPrefix())
	for i := range fcr.NewValidProofOutputs {
		(*objSiacoinOutput)(&fcr.NewValidProofOutputs[i]).decodeFrom(d)
	}
	fcr.NewMissedProofOutputs = make([]types.SiacoinOutput, d.ReadPrefix())
	for i := range fcr.NewMissedProofOutputs {
		(*objSiacoinOutput)(&fcr.NewMissedProofOutputs[i]).decodeFrom(d)
	}
	d.Read(fcr.NewUnlockHash[:])
	return d.Err()
}

func (sp *objStorageProof) encodedLen() int {
	return len(sp.ParentID) + len(sp.Segment) + 8 + crypto.HashSize*len(sp.HashSet)
}

func (sp *objStorageProof) encodeTo(e *encoder) {
	e.Write(sp.ParentID[:])
	e.Write(sp.Segment[:])
	e.WritePrefix(len(sp.HashSet))
	for i := range sp.HashSet {
		e.Write(sp.HashSet[i][:])
	}
}

func (sp *objStorageProof) decodeFrom(d *decoder) error {
	d.Read(sp.ParentID[:])
	d.Read(sp.Segment[:])
	sp.HashSet = make([]crypto.Hash, d.ReadPrefix())
	for i := range sp.HashSet {
		d.Read(sp.HashSet[i][:])
	}
	return d.Err()
}

func (t *objTransaction) encodedLen() int {
	size := 8
	for i := range t.SiacoinInputs {
		size += (*objSiacoinInput)(&t.SiacoinInputs[i]).encodedLen()
	}
	size += 8
	for i := range t.SiacoinOutputs {
		size += (*objSiacoinOutput)(&t.SiacoinOutputs[i]).encodedLen()
	}
	size += 8
	for i := range t.FileContracts {
		size += (*objFileContract)(&t.FileContracts[i]).encodedLen()
	}
	size += 8
	for i := range t.FileContractRevisions {
		size += (*objFileContractRevision)(&t.FileContractRevisions[i]).encodedLen()
	}
	size += 8
	for i := range t.StorageProofs {
		size += (*objStorageProof)(&t.StorageProofs[i]).encodedLen()
	}
	size += 8
	for i := range t.SiafundInputs {
		size += (*objSiafundInput)(&t.SiafundInputs[i]).encodedLen()
	}
	size += 8
	for i := range t.SiafundOutputs {
		size += (*objSiafundOutput)(&t.SiafundOutputs[i]).encodedLen()
	}
	size += 8
	for i := range t.MinerFees {
		size += (*objCurrency)(&t.MinerFees[i]).encodedLen()
	}
	size += 8
	for i := range t.ArbitraryData {
		size += 8 + len(t.ArbitraryData[i])
	}
	size += 8
	for i := range t.TransactionSignatures {
		size += (*objTransactionSignature)(&t.TransactionSignatures[i]).encodedLen()
	}
	return size
}

func (t *objTransaction) encodeTo(e *encoder) {
	e.WritePrefix(len(t.SiacoinInputs))
	for i := range t.SiacoinInputs {
		(*objSiacoinInput)(&t.SiacoinInputs[i]).encodeTo(e)
	}
	e.WritePrefix(len(t.SiacoinOutputs))
	for i := range t.SiacoinOutputs {
		(*objSiacoinOutput)(&t.SiacoinOutputs[i]).encodeTo(e)
	}
	e.WritePrefix(len(t.FileContracts))
	for i := range t.FileContracts {
		(*objFileContract)(&t.FileContracts[i]).encodeTo(e)
	}
	e.WritePrefix(len(t.FileContractRevisions))
	for i := range t.FileContractRevisions {
		(*objFileContractRevision)(&t.FileContractRevisions[i]).encodeTo(e)
	}
	e.WritePrefix(len(t.StorageProofs))
	for i := range t.StorageProofs {
		(*objStorageProof)(&t.StorageProofs[i]).encodeTo(e)
	}
	e.WritePrefix(len(t.SiafundInputs))
	for i := range t.SiafundInputs {
		(*objSiafundInput)(&t.SiafundInputs[i]).encodeTo(e)
	}
	e.WritePrefix(len(t.SiafundOutputs))
	for i := range t.SiafundOutputs {
		(*objSiafundOutput)(&t.SiafundOutputs[i]).encodeTo(e)
	}
	e.WritePrefix(len(t.MinerFees))
	for i := range t.MinerFees {
		(*objCurrency)(&t.MinerFees[i]).encodeTo(e)
	}
	e.WritePrefix(len(t.ArbitraryData))
	for i := range t.ArbitraryData {
		e.WriteBytes(t.ArbitraryData[i])
	}
	e.WritePrefix(len(t.TransactionSignatures))
	for i := range t.TransactionSignatures {
		(*objTransactionSignature)(&t.TransactionSignatures[i]).encodeTo(e)
	}
}

func (t *objTransaction) decodeFrom(d *decoder) error {
	t.SiacoinInputs = make([]types.SiacoinInput, d.ReadPrefix())
	for i := range t.SiacoinInputs {
		(*objSiacoinInput)(&t.SiacoinInputs[i]).decodeFrom(d)
	}
	t.SiacoinOutputs = make([]types.SiacoinOutput, d.ReadPrefix())
	for i := range t.SiacoinOutputs {
		(*objSiacoinOutput)(&t.SiacoinOutputs[i]).decodeFrom(d)
	}
	t.FileContracts = make([]types.FileContract, d.ReadPrefix())
	for i := range t.FileContracts {
		(*objFileContract)(&t.FileContracts[i]).decodeFrom(d)
	}
	t.FileContractRevisions = make([]types.FileContractRevision, d.ReadPrefix())
	for i := range t.FileContractRevisions {
		(*objFileContractRevision)(&t.FileContractRevisions[i]).decodeFrom(d)
	}
	t.StorageProofs = make([]types.StorageProof, d.ReadPrefix())
	for i := range t.StorageProofs {
		(*objStorageProof)(&t.StorageProofs[i]).decodeFrom(d)
	}
	t.SiafundInputs = make([]types.SiafundInput, d.ReadPrefix())
	for i := range t.SiafundInputs {
		(*objSiafundInput)(&t.SiafundInputs[i]).decodeFrom(d)
	}
	t.SiafundOutputs = make([]types.SiafundOutput, d.ReadPrefix())
	for i := range t.SiafundOutputs {
		(*objSiafundOutput)(&t.SiafundOutputs[i]).decodeFrom(d)
	}
	t.MinerFees = make([]types.Currency, d.ReadPrefix())
	for i := range t.MinerFees {
		(*objCurrency)(&t.MinerFees[i]).decodeFrom(d)
	}
	t.ArbitraryData = make([][]byte, d.ReadPrefix())
	for i := range t.ArbitraryData {
		t.ArbitraryData[i] = d.ReadBytes()
	}
	t.TransactionSignatures = make([]types.TransactionSignature, d.ReadPrefix())
	for i := range t.TransactionSignatures {
		(*objTransactionSignature)(&t.TransactionSignatures[i]).decodeFrom(d)
	}
	return d.Err()
}

func (oa *objAccountID) encodedLen() int {
	// note: sia v1 encodes these as a types.SiaPublicKey or an empty string
	// 16 byte specifier + 8 byte key length prefix + 32 byte key
	return 16 + 8 + 32
}

func (oa *objAccountID) encodeTo(e *encoder) {
	e.Write(types.SignatureEd25519[:])
	e.WriteBytes(oa[:])
}

func (oa *objAccountID) decodeFrom(d *decoder) error {
	var prefix Specifier
	d.Read(prefix[:])
	if prefix != keyEd25519 {
		return d.SetErr(errors.New("account ID must be ed25519"))
	}
	n := d.ReadPrefix()
	if n != ed25519.PublicKeySize {
		return d.SetErr(errors.New("account ID must be 32 bytes"))
	}
	d.Read(oa[:])
	return d.Err()
}

func (re *rpcError) encodedLen() int {
	return len(re.Type) + 8 + len(re.Data) + 8 + len(re.Description)
}

func (re *rpcError) encodeTo(e *encoder) {
	re.Type.encodeTo(e)
	e.WriteBytes(re.Data)
	e.WriteString(re.Description)
}

func (re *rpcError) decodeFrom(d *decoder) error {
	re.Type.decodeFrom(d)
	re.Data = d.ReadBytes()
	re.Description = d.ReadString()
	return d.Err()
}

func (resp *rpcResponse) encodedLen() int {
	if resp.err != nil {
		return 1 + resp.err.encodedLen()
	}
	return 1 + resp.obj.encodedLen()
}

func (resp *rpcResponse) encodeTo(e *encoder) {
	e.WriteBool(resp.err != nil)
	if resp.err != nil {
		resp.err.encodeTo(e)
	} else {
		resp.obj.encodeTo(e)
	}
}

func (resp *rpcResponse) decodeFrom(d *decoder) error {
	if isErr := d.ReadBool(); isErr {
		resp.err = new(rpcError)
		resp.err.decodeFrom(d)
	} else {
		resp.obj.decodeFrom(d)
	}
	return d.Err()
}

func (r *rpcUpdatePriceTableResponse) encodedLen() int {
	return 8 + len(r.PriceTableJSON)
}

func (r *rpcUpdatePriceTableResponse) encodeTo(e *encoder) {
	e.WriteBytes(r.PriceTableJSON)
}

func (r *rpcUpdatePriceTableResponse) decodeFrom(d *decoder) error {
	r.PriceTableJSON = d.ReadBytes()
	return d.Err()
}

func (r *rpcTrackingResponse) encodedLen() int {
	return 0
}

func (r *rpcTrackingResponse) encodeTo(e *encoder) {
}

func (r *rpcTrackingResponse) decodeFrom(d *decoder) error {
	return d.Err()
}

func (wm *withdrawalMessage) encodedLen() int {
	return (*objAccountID)(&wm.AccountID).encodedLen() + (*objCurrency)(&wm.Amount).encodedLen() + 8 + 8
}

func (wm *withdrawalMessage) encodeTo(e *encoder) {
	(*objAccountID)(&wm.AccountID).encodeTo(e)
	e.WriteUint64(uint64(wm.Expiry))
	(*objCurrency)(&wm.Amount).encodeTo(e)
	e.Write(wm.Nonce[:])
}

func (wm *withdrawalMessage) decodeFrom(d *decoder) error {
	(*objAccountID)(&wm.AccountID).decodeFrom(d)
	wm.Expiry = types.BlockHeight(d.ReadUint64())
	(*objCurrency)(&wm.Amount).decodeFrom(d)
	d.Read(wm.Nonce[:])
	return d.Err()
}

func (r *rpcPayByContractRequest) encodedLen() (n int) {
	n = 32 + 8 + (*objAccountID)(&r.RefundAccount).encodedLen() + 8 + 8 + 8 + len(r.Signature)
	for _, c := range r.NewValidProofValues {
		n += (*objCurrency)(&c).encodedLen()
	}
	for _, c := range r.NewMissedProofValues {
		n += (*objCurrency)(&c).encodedLen()
	}
	return
}

func (r *rpcPayByContractRequest) encodeTo(e *encoder) {
	e.Write(r.ContractID[:])
	e.WriteUint64(r.NewRevisionNumber)
	e.WritePrefix(len(r.NewValidProofValues))
	for _, c := range r.NewValidProofValues {
		(*objCurrency)(&c).encodeTo(e)
	}
	e.WritePrefix(len(r.NewMissedProofValues))
	for _, c := range r.NewMissedProofValues {
		(*objCurrency)(&c).encodeTo(e)
	}
	(*objAccountID)(&r.RefundAccount).encodeTo(e)
	e.WriteBytes(r.Signature)
}

func (r *rpcPayByContractRequest) decodeFrom(d *decoder) error {
	d.Read(r.ContractID[:])
	r.NewRevisionNumber = d.ReadUint64()
	r.NewValidProofValues = make([]types.Currency, d.ReadPrefix())
	for i := range r.NewValidProofValues {
		(*objCurrency)(&r.NewValidProofValues[i]).decodeFrom(d)
	}
	r.NewMissedProofValues = make([]types.Currency, d.ReadPrefix())
	for i := range r.NewMissedProofValues {
		(*objCurrency)(&r.NewMissedProofValues[i]).decodeFrom(d)
	}
	(*objAccountID)(&r.RefundAccount).decodeFrom(d)
	r.Signature = d.ReadBytes()
	return d.Err()
}

func (r *rpcPayByContractResponse) encodedLen() int {
	return 64
}

func (r *rpcPayByContractResponse) encodeTo(e *encoder) {
	e.Write(r.Signature[:])
}

func (r *rpcPayByContractResponse) decodeFrom(d *decoder) error {
	d.Read(r.Signature[:])
	return d.Err()
}

func (r *rpcPayByEphemeralAccountRequest) encodedLen() int {
	return r.Message.encodedLen() + 64 + 8
}

func (r *rpcPayByEphemeralAccountRequest) encodeTo(e *encoder) {
	r.Message.encodeTo(e)
	e.Write(r.Signature[:])
	e.WriteUint64(r.Priority)
}

func (r *rpcPayByEphemeralAccountRequest) decodeFrom(d *decoder) error {
	r.Message.decodeFrom(d)
	d.Read(r.Signature[:])
	r.Priority = d.ReadUint64()
	return d.Err()
}

func (r *rpcFundAccountRequest) encodedLen() int {
	return (*objAccountID)(&r.Account).encodedLen()
}

func (r *rpcFundAccountRequest) encodeTo(e *encoder) {
	(*objAccountID)(&r.Account).encodeTo(e)
}

func (r *rpcFundAccountRequest) decodeFrom(d *decoder) error {
	return (*objAccountID)(&r.Account).decodeFrom(d)
}

func (fr *fundReceipt) encodedLen() int {
	return (*objSiaPublicKey)(&fr.Host).encodedLen() + (*objAccountID)(&fr.Account).encodedLen() + (*objCurrency)(&fr.Amount).encodedLen() + 8
}

func (fr *fundReceipt) encodeTo(e *encoder) {
	(*objSiaPublicKey)(&fr.Host).encodeTo(e)
	(*objAccountID)(&fr.Account).encodeTo(e)
	(*objCurrency)(&fr.Amount).encodeTo(e)
	e.WriteTime(fr.Timestamp)
}

func (fr *fundReceipt) decodeFrom(d *decoder) error {
	(*objSiaPublicKey)(&fr.Host).decodeFrom(d)
	(*objAccountID)(&fr.Account).decodeFrom(d)
	(*objCurrency)(&fr.Amount).decodeFrom(d)
	fr.Timestamp = d.ReadTime()
	return d.Err()
}

func (r *rpcFundAccountResponse) encodedLen() int {
	return (*objCurrency)(&r.Balance).encodedLen() + r.Receipt.encodedLen() + 64
}

func (r *rpcFundAccountResponse) encodeTo(e *encoder) {
	(*objCurrency)(&r.Balance).encodeTo(e)
	r.Receipt.encodeTo(e)
	e.Write(r.Signature[:])
}

func (r *rpcFundAccountResponse) decodeFrom(d *decoder) error {
	(*objCurrency)(&r.Balance).decodeFrom(d)
	r.Receipt.decodeFrom(d)
	d.Read(r.Signature[:])
	return d.Err()
}

func (r *rpcAccountBalanceRequest) encodedLen() int {
	return (*objAccountID)(&r.AccountID).encodedLen()
}

func (r *rpcAccountBalanceRequest) encodeTo(e *encoder) {
	(*objAccountID)(&r.AccountID).encodeTo(e)
}

func (r *rpcAccountBalanceRequest) decodeFrom(d *decoder) error {
	return (*objAccountID)(&r.AccountID).decodeFrom(d)
}

func (r *rpcAccountBalanceResponse) encodedLen() int {
	return (*objCurrency)(&r.Balance).encodedLen()
}

func (r *rpcAccountBalanceResponse) encodeTo(d *encoder) {
	(*objCurrency)(&r.Balance).encodeTo(d)
}

func (r *rpcAccountBalanceResponse) decodeFrom(d *decoder) error {
	return (*objCurrency)(&r.Balance).decodeFrom(d)
}

func (r *rpcExecuteOutput) encodedLen() (n int) {
	// error prefix + error string
	n += 8
	if r.Error != nil {
		n += len(r.Error.Error())
	}
	// currency + uint64 + hash + uint64 + length prefix + len(proof) + currency
	// + currency
	n += (*objCurrency)(&r.AdditionalCollateral).encodedLen() + 8 + 32 + 8 + 8 + len(r.Proof)*32 + (*objCurrency)(&r.TotalCost).encodedLen() + (*objCurrency)(&r.FailureRefund).encodedLen()
	return n
}

func (r *rpcExecuteOutput) encodeTo(e *encoder) {
	(*objCurrency)(&r.AdditionalCollateral).encodeTo(e)
	e.WriteUint64(r.OutputLength)
	e.Write(r.NewMerkleRoot[:])
	e.WriteUint64(r.NewSize)
	e.WritePrefix(len(r.Proof))
	for _, h := range r.Proof {
		e.Write(h[:])
	}
	if r.Error != nil {
		e.WriteString(r.Error.Error())
	} else {
		e.WriteString("")
	}
	(*objCurrency)(&r.TotalCost).encodeTo(e)
	(*objCurrency)(&r.FailureRefund).encodeTo(e)
}

func (r *rpcExecuteOutput) decodeFrom(d *decoder) error {
	(*objCurrency)(&r.AdditionalCollateral).decodeFrom(d)
	r.OutputLength = d.ReadUint64()
	d.Read(r.NewMerkleRoot[:])
	r.NewSize = d.ReadUint64()
	r.Proof = make([]crypto.Hash, d.ReadPrefix())
	for i := range r.Proof {
		d.Read(r.Proof[i][:])
	}
	if s := d.ReadString(); len(s) != 0 {
		r.Error = errors.New(s)
	}
	(*objCurrency)(&r.TotalCost).decodeFrom(d)
	(*objCurrency)(&r.FailureRefund).decodeFrom(d)
	return d.Err()
}

func (r *program) encodedLen() (n int) {
	// length prefix
	n = 8
	for _, i := range r.Instructions {
		// specifier + length prefix + arguments
		n += 16 + 8 + i.encodedLen()
	}
	return
}

func (r *program) encodeTo(e *encoder) {
	e.WritePrefix(len(r.Instructions))
	for _, instr := range r.Instructions {
		var id Specifier
		switch instr.(type) {
		case *instrAppendSector:
			id = idInstrAppendSector
		case *instrAppendSectorRoot:
			id = idInstrAppendSectorRoot
		case *instrDropSectors:
			id = idInstrDropSectors
		case *instrHasSector:
			id = idInstrHasSector
		case *instrStoreSector:
			id = idInstrStoreSector
		case *instrUpdateSector:
			id = idInstrUpdateSector
		case *instrReadOffset:
			id = idInstrReadOffset
		case *instrReadSector:
			id = idInstrReadSector
		case *instrRevision:
			id = idInstrContractRevision
		case *instrSwapSector:
			id = idInstrSwapSector
		case *instrUpdateRegistry, *instrUpdateRegistryNoType:
			id = idInstrUpdateRegistry
		case *instrReadRegistry, *instrReadRegistryNoVersion:
			id = idInstrReadRegistry
		}

		e.Write(id[:])
		e.WritePrefix(instr.encodedLen())
		instr.encodeTo(e)
	}
}

func (p *program) decodeFrom(d *decoder) error {
	p.Instructions = make([]instruction, d.ReadPrefix())
	for i := range p.Instructions {
		// instructions are prefixed by their specifier and argument length
		var instr instruction
		var id Specifier
		d.Read(id[:])
		argsLen := d.ReadUint64()
		switch id {
		case idInstrAppendSector:
			instr = new(instrAppendSector)
		case idInstrAppendSectorRoot:
			instr = new(instrAppendSectorRoot)
		case idInstrDropSectors:
			instr = new(instrDropSectors)
		case idInstrHasSector:
			instr = new(instrHasSector)
		case idInstrStoreSector:
			instr = new(instrStoreSector)
		case idInstrUpdateSector:
			instr = new(instrUpdateSector)
		case idInstrReadOffset:
			instr = new(instrReadOffset)
		case idInstrReadSector:
			instr = new(instrReadSector)
		case idInstrContractRevision:
			instr = new(instrRevision)
		case idInstrSwapSector:
			instr = new(instrSwapSector)
		case idInstrUpdateRegistry:
			if argsLen == 56 { // special handling for pre-1.5.7 update registry instructions
				instr = new(instrUpdateRegistryNoType)
			} else {
				instr = new(instrUpdateRegistry)
			}
		case idInstrReadRegistry:
			if argsLen == 24 { // special handling for pre-1.5.7 read registry instructions
				instr = new(instrReadRegistryNoVersion)
			} else {
				instr = new(instrReadRegistry)
			}
		default:
			return d.SetErr(fmt.Errorf("unrecognized instruction id: %q", id))
		}

		// check that the length of arguments matches the expected length
		if argsLen != uint64(instr.encodedLen()) {
			return d.SetErr(fmt.Errorf("invalid length for instruction %q (%v != %v)", id, argsLen, instr.encodedLen()))
		} else if err := instr.decodeFrom(d); err != nil {
			return fmt.Errorf("error decoding instruction %v: %w", id, err)
		}

		p.RequiresContract = p.RequiresContract || instr.RequiresContract()
		p.RequiresFinalization = p.RequiresFinalization || instr.RequiresFinalization()
		p.Instructions[i] = instr
	}
	return d.Err()
}

func (r *rpcExecuteProgramRequest) encodedLen() int {
	return 32 + 8 + r.Program.encodedLen()
}

func (r *rpcExecuteProgramRequest) encodeTo(e *encoder) {
	e.Write(r.FileContractID[:])
	r.Program.encodeTo(e)
	e.WritePrefix(r.ProgramDataLength)
}

func (r *rpcExecuteProgramRequest) decodeFrom(d *decoder) error {
	d.Read(r.FileContractID[:])
	r.Program.decodeFrom(d)
	r.ProgramDataLength = d.ReadPrefix()
	return d.Err()
}
