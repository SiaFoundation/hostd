package rhp

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	rhp2 "go.sia.tech/core/rhp/v2"
	rhp3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/rhp"
	"go.uber.org/zap"
)

// read registry instruction versions
const (
	readRegistryNoType = 1
	readRegistryType   = 2
)

type (
	programData []byte

	programExecutor struct {
		hostKey types.PrivateKey

		instructions []rhp3.Instruction
		programData  programData
		priceTable   rhp3.HostPriceTable

		budget *accounts.Budget
		cost   rhp3.ResourceCost
		usage  accounts.Usage

		revision          *contracts.SignedRevision
		remainingDuration uint64
		updater           *contracts.ContractUpdater
		tempSectors       []storage.TempSector

		finalize     bool
		releaseFuncs []func() error

		log       *zap.Logger
		contracts ContractManager
		storage   StorageManager
		registry  RegistryManager

		committed bool
	}
)

var (
	// ErrContractRequired is returned when a contract is required to execute a
	// program but is not provided
	ErrContractRequired = errors.New("contract required")
)

func (pe *programExecutor) instructionOutput(output []byte, proof []types.Hash256, err error) rhp3.RPCExecuteProgramResponse {
	resp := rhp3.RPCExecuteProgramResponse{
		AdditionalCollateral: pe.cost.Collateral,
		TotalCost:            pe.cost.Base.Add(pe.cost.Storage).Add(pe.cost.Egress).Add(pe.cost.Ingress),
		FailureRefund:        pe.cost.Storage,
		OutputLength:         uint64(len(output)),
		Proof:                proof,
		Output:               output,
		Error:                err,
	}
	if pe.updater != nil {
		resp.NewMerkleRoot = pe.updater.MerkleRoot()
		resp.NewSize = pe.updater.SectorCount() * rhp2.SectorSize
	}
	return resp
}

func (pe *programExecutor) payForExecution(cost rhp3.ResourceCost, usage accounts.Usage) error {
	if err := pe.budget.Spend(usage); err != nil {
		return err
	}
	pe.cost = pe.cost.Add(cost)
	pe.usage = pe.usage.Add(usage)
	return nil
}

func (pe *programExecutor) executeAppendSector(instr *rhp3.InstrAppendSector, log *zap.Logger) ([]byte, []types.Hash256, error) {
	sector, err := pe.programData.Sector(instr.SectorDataOffset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector: %w", err)
	}
	rootCalcStart := time.Now()
	root := rhp2.SectorRoot(sector)
	log.Debug("calculated sector root", zap.Duration("duration", time.Since(rootCalcStart)))
	// pay for execution
	cost := pe.priceTable.AppendSectorCost(pe.remainingDuration)
	if err := pe.payForExecution(cost, costToAccountUsage(cost)); err != nil {
		return nil, nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	release, err := pe.storage.Write(root, sector)
	if errors.Is(err, storage.ErrNotEnoughStorage) {
		return nil, nil, err
	} else if err != nil {
		return nil, nil, ErrHostInternalError
	}
	pe.releaseFuncs = append(pe.releaseFuncs, release)
	pe.updater.AppendSector(root)

	if !instr.ProofRequired {
		return nil, nil, nil
	}

	proofStart := time.Now()
	roots := pe.updater.SectorRoots()
	proof, _ := rhp2.BuildDiffProof([]rhp2.RPCWriteAction{{Type: rhp2.RPCWriteActionAppend}}, roots[:len(roots)-1]) // TODO: add rhp3 proof methods
	log.Debug("built proof", zap.Duration("duration", time.Since(proofStart)))
	return nil, proof, nil
}

func (pe *programExecutor) executeAppendSectorRoot(instr *rhp3.InstrAppendSectorRoot, log *zap.Logger) ([]byte, []types.Hash256, error) {
	root, err := pe.programData.Hash(instr.MerkleRootOffset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector root: %w", err)
	}
	// pay for execution
	cost := pe.priceTable.AppendSectorRootCost(pe.remainingDuration)
	if err := pe.payForExecution(cost, costToAccountUsage(cost)); err != nil {
		return nil, nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	// lock the sector to prevent it from being garbage collected
	release, err := pe.storage.LockSector(root)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector: %w", err)
	}
	pe.releaseFuncs = append(pe.releaseFuncs, release)
	pe.updater.AppendSector(root)
	if !instr.ProofRequired {
		return nil, nil, nil
	}
	proofStart := time.Now()
	roots := pe.updater.SectorRoots()
	proof, _ := rhp2.BuildDiffProof([]rhp2.RPCWriteAction{{Type: rhp2.RPCWriteActionAppend}}, roots[:len(roots)-1]) // TODO: add rhp3 proof methods
	log.Debug("built proof", zap.Duration("duration", time.Since(proofStart)))
	return nil, proof, nil
}

func (pe *programExecutor) executeDropSectors(instr *rhp3.InstrDropSectors, log *zap.Logger) ([]byte, []types.Hash256, error) {
	count, err := pe.programData.Uint64(instr.SectorCountOffset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector count: %w", err)
	}
	// pay for execution
	cost := pe.priceTable.DropSectorsCost(count)
	if err := pe.payForExecution(cost, costToAccountUsage(cost)); err != nil {
		return nil, nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	// construct the proof before updating the roots
	var proof []types.Hash256
	if instr.ProofRequired {
		proofStart := time.Now()
		proof = rhp2.BuildSectorRangeProof(pe.updater.SectorRoots(), pe.updater.SectorCount()-count, pe.updater.SectorCount()) // TODO: add rhp3 proof methods
		log.Debug("built proof", zap.Duration("duration", time.Since(proofStart)))
	}

	// trim the sectors
	if err := pe.updater.TrimSectors(count); err != nil {
		return nil, nil, fmt.Errorf("failed to drop sectors: %w", err)
	}
	return nil, proof, nil
}

func (pe *programExecutor) executeHasSector(instr *rhp3.InstrHasSector) ([]byte, []types.Hash256, error) {
	root, err := pe.programData.Hash(instr.MerkleRootOffset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector root: %w", err)
	}
	// pay for execution
	cost := pe.priceTable.HasSectorCost()
	if err := pe.payForExecution(cost, costToAccountUsage(cost)); err != nil {
		return nil, nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	var has bool
	release, err := pe.storage.LockSector(root)
	if err != nil && !errors.Is(err, storage.ErrSectorNotFound) {
		return nil, nil, fmt.Errorf("failed to locate sector %q: %w", root, err)
	} else if err == nil {
		has = true
		pe.releaseFuncs = append(pe.releaseFuncs, release)
	}

	output := make([]byte, 1)
	if has {
		output[0] = 1
	}
	return output, nil, nil
}

func (pe *programExecutor) executeReadOffset(instr *rhp3.InstrReadOffset, log *zap.Logger) ([]byte, []types.Hash256, error) {
	offset, err := pe.programData.Uint64(instr.OffsetOffset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read offset: %w", err)
	}
	length, err := pe.programData.Uint64(instr.LengthOffset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read length: %w", err)
	}
	// pay for execution
	cost := pe.priceTable.ReadOffsetCost(length)
	if err := pe.payForExecution(cost, costToAccountUsage(cost)); err != nil {
		return nil, nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	sectorIndex := offset / rhp2.SectorSize
	relOffset := offset % rhp2.SectorSize

	root, err := pe.updater.SectorRoot(sectorIndex)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get root: %w", err)
	}

	sector, err := pe.storage.Read(root)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector: %w", err)
	}

	// if no proof was requested, return the data
	if !instr.ProofRequired {
		return sector[relOffset : relOffset+length], nil, nil
	}

	proofStartTime := time.Now()
	proofStart := relOffset / rhp2.LeafSize
	proofEnd := (relOffset + length) / rhp2.LeafSize
	proof := rhp2.BuildProof(sector, proofStart, proofEnd, nil)
	log.Debug("built proof", zap.Duration("duration", time.Since(proofStartTime)))
	return sector[relOffset : relOffset+length], proof, nil
}

func (pe *programExecutor) executeReadSector(instr *rhp3.InstrReadSector, log *zap.Logger) ([]byte, []types.Hash256, error) {
	root, err := pe.programData.Hash(instr.MerkleRootOffset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector root: %w", err)
	}
	length, err := pe.programData.Uint64(instr.LengthOffset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read length: %w", err)
	}
	offset, err := pe.programData.Uint64(instr.OffsetOffset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read offset: %w", err)
	}

	// validate the offset and length
	switch {
	case length == 0:
		return nil, nil, fmt.Errorf("read length cannot be 0")
	case offset+length > rhp2.SectorSize:
		return nil, nil, fmt.Errorf("read length %v is out of bounds", length)
	case instr.ProofRequired && (offset%rhp2.LeafSize != 0 || length%rhp2.LeafSize != 0):
		return nil, nil, fmt.Errorf("read offset (%d) and length (%d) must be multiples of %d", offset, length, rhp2.LeafSize)
	}

	// pay for execution
	cost := pe.priceTable.ReadSectorCost(length)
	if err := pe.payForExecution(cost, costToAccountUsage(cost)); err != nil {
		return nil, nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	// read the sector
	sector, err := pe.storage.Read(root)
	if errors.Is(err, storage.ErrSectorNotFound) {
		log.Debug("failed to read sector", zap.String("root", root.String()), zap.Error(err))
		return nil, nil, storage.ErrSectorNotFound
	} else if err != nil {
		log.Error("failed to read sector", zap.String("root", root.String()), zap.Error(err))
		return nil, nil, ErrHostInternalError
	}

	// if no proof was requested, return the data
	if !instr.ProofRequired {
		return sector[offset : offset+length], nil, nil
	}

	proofStartTime := time.Now()
	proofStart := offset / rhp2.LeafSize
	proofEnd := (offset + length) / rhp2.LeafSize
	proof := rhp2.BuildProof(sector, proofStart, proofEnd, nil)
	log.Debug("built proof", zap.Duration("duration", time.Since(proofStartTime)))
	return sector[offset : offset+length], proof, nil
}

func (pe *programExecutor) executeSwapSector(instr *rhp3.InstrSwapSector, log *zap.Logger) ([]byte, []types.Hash256, error) {
	// read the swap params
	a, err := pe.programData.Uint64(instr.Sector1Offset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector 1 index: %w", err)
	}
	b, err := pe.programData.Uint64(instr.Sector2Offset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector 2 index: %w", err)
	} else if a > b {
		a, b = b, a
	}

	// pay for execution
	cost := pe.priceTable.SwapSectorCost()
	if err := pe.payForExecution(cost, costToAccountUsage(cost)); err != nil {
		return nil, nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	var output []byte
	var proof []types.Hash256
	if instr.ProofRequired {
		proofStart := time.Now()
		var oldLeafHashes []types.Hash256
		// build the proof before updating the roots
		proof, oldLeafHashes = rhp2.BuildDiffProof([]rhp2.RPCWriteAction{{Type: rhp2.RPCWriteActionSwap, A: a, B: b}}, pe.updater.SectorRoots()) // TODO: add rhp3 proof methods
		// encode the old leaf hashes
		var buf bytes.Buffer
		enc := types.NewEncoder(&buf)
		types.EncodeSliceFn(enc, oldLeafHashes, func(enc *types.Encoder, h types.Hash256) {
			enc.Write(h[:])
		})
		if err := enc.Flush(); err != nil {
			return nil, nil, fmt.Errorf("failed to encode old leaf hashes: %w", err)
		}
		output = buf.Bytes()
		log.Debug("built proof", zap.Duration("duration", time.Since(proofStart)))
	}

	if err := pe.updater.SwapSectors(a, b); err != nil {
		return nil, nil, fmt.Errorf("failed to swap sectors: %w", err)
	}
	return output, proof, nil
}

func (pe *programExecutor) executeUpdateSector(instr *rhp3.InstrUpdateSector, _ *zap.Logger) ([]byte, []types.Hash256, error) {
	offset, length := instr.Offset, instr.Length
	// read the patch
	patch, err := pe.programData.Bytes(instr.DataOffset, length)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read patch: %w", err)
	}

	// pay for execution
	cost := pe.priceTable.UpdateSectorCost(instr.Length)
	if err := pe.payForExecution(cost, costToAccountUsage(cost)); err != nil {
		return nil, nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	sectorIndex := offset / rhp2.SectorSize
	relOffset := offset % rhp2.SectorSize

	oldRoot, err := pe.updater.SectorRoot(sectorIndex)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get root: %w", err)
	}

	sector, err := pe.storage.Read(oldRoot)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector: %w", err)
	}

	// validate and apply the patch
	if relOffset+length > rhp2.SectorSize {
		return nil, nil, fmt.Errorf("update offset %v length %v is out of bounds", relOffset, length)
	}
	copy(sector[relOffset:], patch)

	// store the new sector
	newRoot := rhp2.SectorRoot((*[rhp2.SectorSize]byte)(sector))
	release, err := pe.storage.Write(newRoot, sector)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to write sector: %w", err)
	}
	pe.releaseFuncs = append(pe.releaseFuncs, release)
	if err := pe.updater.UpdateSector(newRoot, sectorIndex); err != nil {
		return nil, nil, fmt.Errorf("failed to update sector: %w", err)
	}
	return newRoot[:], nil, nil
}

func (pe *programExecutor) executeStoreSector(instr *rhp3.InstrStoreSector, log *zap.Logger) ([]byte, error) {
	sector, err := pe.programData.Sector(instr.DataOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read sector: %w", err)
	}
	rootCalcStart := time.Now()
	root := rhp2.SectorRoot(sector)
	log.Debug("calculated sector root", zap.Duration("duration", time.Since(rootCalcStart)))

	// pay for execution
	cost := pe.priceTable.StoreSectorCost(instr.Duration)
	if err := pe.payForExecution(cost, costToAccountUsage(cost)); err != nil {
		return nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	if instr.Duration == 0 {
		return nil, fmt.Errorf("duration cannot be 0")
	} else if instr.Duration > storage.MaxTempSectorBlocks {
		return nil, fmt.Errorf("duration cannot be greater than %d", storage.MaxTempSectorBlocks)
	}

	// store the sector
	release, err := pe.storage.Write(root, sector)
	if err != nil {
		return nil, fmt.Errorf("failed to write sector: %w", err)
	}
	pe.releaseFuncs = append(pe.releaseFuncs, release)

	// add the sector to the program state
	pe.tempSectors = append(pe.tempSectors, storage.TempSector{
		Root:       root,
		Expiration: pe.priceTable.HostBlockHeight + instr.Duration,
	})
	return root[:], nil
}

func (pe *programExecutor) executeRevision(*rhp3.InstrRevision) ([]byte, error) {
	// pay for execution
	cost := pe.priceTable.RevisionCost()
	if err := pe.payForExecution(cost, costToAccountUsage(cost)); err != nil {
		return nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	var buf bytes.Buffer
	enc := types.NewEncoder(&buf)
	revisionTxn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{pe.revision.Revision},
		Signatures:            pe.revision.Signatures(),
	}
	revisionTxn.EncodeTo(enc)
	if err := enc.Flush(); err != nil {
		return nil, fmt.Errorf("failed to encode revision: %w", err)
	}
	return buf.Bytes(), nil
}

func (pe *programExecutor) executeReadRegistry(instr *rhp3.InstrReadRegistry) ([]byte, error) {
	if instr.Version != readRegistryNoType && instr.Version != readRegistryType {
		return nil, fmt.Errorf("unsupported registry version: %v", instr.Version)
	}

	unlockKey, err := pe.programData.UnlockKey(instr.PublicKeyOffset, instr.PublicKeyLength)
	if err != nil {
		return nil, fmt.Errorf("failed to read unlock key: %w", err)
	} else if unlockKey.Algorithm != types.SpecifierEd25519 {
		return nil, fmt.Errorf("unsupported unlock key algorithm: %v", unlockKey.Algorithm)
	} else if len(unlockKey.Key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid unlock key length: %v", len(unlockKey.Key))
	}
	publicKey := *(*types.PublicKey)(unlockKey.Key)

	tweak, err := pe.programData.Hash(instr.TweakOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read tweak: %w", err)
	}

	// pay for execution
	cost := pe.priceTable.ReadRegistryCost()
	usage := accounts.Usage{
		RPCRevenue:     cost.Base,
		RegistryRead:   cost.Storage,
		IngressRevenue: cost.Ingress,
		EgressRevenue:  cost.Egress,
	}
	if err := pe.payForExecution(cost, usage); err != nil {
		return nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	key := rhp3.RegistryKey{
		PublicKey: publicKey,
		Tweak:     tweak,
	}

	value, err := pe.registry.Get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to read registry value: %w", err)
	}

	var buf bytes.Buffer
	enc := types.NewEncoder(&buf)
	value.Signature.EncodeTo(enc)
	enc.WriteUint64(value.Revision)
	enc.Write(value.Data)
	if instr.Version == readRegistryType {
		enc.Write([]byte{value.Type})
	}
	if err := enc.Flush(); err != nil {
		return nil, fmt.Errorf("failed to encode registry value: %w", err)
	}
	return buf.Bytes(), nil
}

func (pe *programExecutor) executeUpdateRegistry(instr *rhp3.InstrUpdateRegistry) ([]byte, error) {
	tweak, err := pe.programData.Hash(instr.TweakOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read tweak: %w", err)
	}
	revision, err := pe.programData.Uint64(instr.RevisionOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read revision: %w", err)
	}
	signature, err := pe.programData.Signature(instr.SignatureOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read signature: %w", err)
	}
	uk, err := pe.programData.UnlockKey(instr.PublicKeyOffset, instr.PublicKeyLength)
	if err != nil {
		return nil, fmt.Errorf("failed to read unlock key: %w", err)
	} else if uk.Algorithm != types.SpecifierEd25519 {
		return nil, fmt.Errorf("unsupported unlock key algorithm: %v", uk.Algorithm)
	} else if len(uk.Key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid unlock key length: %v", len(uk.Key))
	}
	publicKey := *(*types.PublicKey)(uk.Key)

	data, err := pe.programData.Bytes(instr.DataOffset, instr.DataLength)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	// pay for execution
	cost := pe.priceTable.ReadRegistryCost()
	usage := accounts.Usage{
		RPCRevenue:     cost.Base,
		RegistryWrite:  cost.Storage,
		IngressRevenue: cost.Ingress,
		EgressRevenue:  cost.Egress,
	}
	if err := pe.payForExecution(cost, usage); err != nil {
		return nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	value := rhp3.RegistryEntry{
		RegistryKey: rhp3.RegistryKey{
			PublicKey: publicKey,
			Tweak:     tweak,
		},
		RegistryValue: rhp3.RegistryValue{
			Revision:  revision,
			Type:      instr.EntryType,
			Data:      data,
			Signature: signature,
		},
	}
	expiration := pe.priceTable.HostBlockHeight + (144 * 365) // expires in 1 year
	updated, err := pe.registry.Put(value, expiration)
	if err != nil && strings.Contains(err.Error(), "invalid registry update") {
		// if the update failed send the old signature and data
		return append(updated.Signature[:], updated.Data...), err
	} else if err != nil {
		return nil, err
	}
	// successful update has no output
	return nil, nil
}

func (pe *programExecutor) executeProgram(ctx context.Context) <-chan rhp3.RPCExecuteProgramResponse {
	outputs := make(chan rhp3.RPCExecuteProgramResponse, len(pe.instructions))
	go func() {
		defer close(outputs)

		var output []byte
		var proof []types.Hash256
		var err error
		for i, instruction := range pe.instructions {
			select {
			case <-ctx.Done():
				outputs <- pe.instructionOutput(nil, nil, ctx.Err())
				return
			default:
			}

			log := pe.log.Named(instrLabel(instruction)).With(zap.Int("instruction", i+1), zap.Int("total", len(pe.instructions)))

			start := time.Now()
			// execute the instruction
			switch instr := instruction.(type) {
			case *rhp3.InstrAppendSector:
				output, proof, err = pe.executeAppendSector(instr, log)
			case *rhp3.InstrAppendSectorRoot:
				output, proof, err = pe.executeAppendSectorRoot(instr, log)
			case *rhp3.InstrDropSectors:
				output, proof, err = pe.executeDropSectors(instr, log)
			case *rhp3.InstrHasSector:
				output, proof, err = pe.executeHasSector(instr)
			case *rhp3.InstrReadOffset:
				output, proof, err = pe.executeReadOffset(instr, log)
			case *rhp3.InstrReadSector:
				output, proof, err = pe.executeReadSector(instr, log)
			case *rhp3.InstrSwapSector:
				output, proof, err = pe.executeSwapSector(instr, log)
			case *rhp3.InstrUpdateSector:
				output, proof, err = pe.executeUpdateSector(instr, log)
			case *rhp3.InstrStoreSector:
				output, err = pe.executeStoreSector(instr, log)
			case *rhp3.InstrRevision:
				output, err = pe.executeRevision(instr)
			case *rhp3.InstrReadRegistry:
				output, err = pe.executeReadRegistry(instr)
			case *rhp3.InstrReadRegistryNoVersion:
				instr.Version = 1 // override the version
				output, err = pe.executeReadRegistry(&instr.InstrReadRegistry)
			case *rhp3.InstrUpdateRegistry:
				output, err = pe.executeUpdateRegistry(instr)
			case *rhp3.InstrUpdateRegistryNoType:
				output, err = pe.executeUpdateRegistry(&instr.InstrUpdateRegistry)
			default:
				// immediately return an error if the instruction is unknown
				outputs <- pe.instructionOutput(nil, nil, fmt.Errorf("unknown instruction: %T", instr))
				return
			}
			if err != nil {
				outputs <- pe.instructionOutput(nil, nil, fmt.Errorf("failed to execute instruction %q: %w", instrLabel(instruction), err))
				return
			}
			log.Debug("executed instruction", zap.Duration("elapsed", time.Since(start)))
			outputs <- pe.instructionOutput(output, proof, err)
		}
	}()
	return outputs
}

func (pe *programExecutor) release() error {
	for len(pe.releaseFuncs) > 0 {
		release := pe.releaseFuncs[0]
		if err := release(); err != nil {
			return err
		}
		pe.releaseFuncs = pe.releaseFuncs[1:]
	}
	return nil
}

func (pe *programExecutor) rollback() error {
	if pe.committed {
		return nil
	}
	pe.committed = true

	defer func() {
		// release all of the locked sectors. Any sectors not referenced by a
		// contract or temporary storage will eventually be garbage collected.
		if err := pe.release(); err != nil {
			pe.log.Error("failed to release sectors", zap.Error(err))
		}
	}()

	if pe.updater != nil {
		pe.updater.Close()
	}

	// refund only the storage spending
	pe.budget.Refund(accounts.Usage{StorageRevenue: pe.usage.StorageRevenue})
	if err := pe.budget.Commit(); err != nil {
		return fmt.Errorf("failed to commit budget: %w", err)
	}
	// zero out the usage
	pe.usage.StorageRevenue = types.ZeroCurrency
	pe.cost.Collateral = types.ZeroCurrency
	return nil
}

func (pe *programExecutor) commit(s *rhp3.Stream) error {
	if pe.committed {
		panic("commit called multiple times")
	}
	pe.committed = true

	defer func() {
		// release all of the locked sectors. Any sectors not referenced by a
		// contract or temporary storage will eventually be garbage collected.
		if err := pe.release(); err != nil {
			pe.log.Error("failed to release sectors", zap.Error(err))
		}
	}()

	if err := pe.storage.Sync(); err != nil {
		s.WriteResponseErr(fmt.Errorf("failed to commit storage: %w", ErrHostInternalError))
		return fmt.Errorf("failed to sync storage: %w", err)
	}

	if pe.updater != nil {
		defer pe.updater.Close() // close the updater
	}

	// finalize the program
	if pe.finalize {
		s.SetDeadline(time.Now().Add(time.Minute))
		start := time.Now()

		// read the finalize request
		var req rhp3.RPCFinalizeProgramRequest
		if err := s.ReadResponse(&req, maxRequestSize); err != nil {
			return fmt.Errorf("failed to read finalize request: %w", err)
		}

		// revise the contract with the values received from the renter
		existing := pe.revision.Revision
		revision, err := rhp.Revise(existing, req.RevisionNumber, req.ValidProofValues, req.MissedProofValues)
		if err != nil {
			err = fmt.Errorf("failed to revise contract: %w", err)
			s.WriteResponseErr(err)
			return err
		}

		_, err = rhp.ValidateProgramRevision(existing, revision, pe.cost.Storage, pe.cost.Collateral)
		if err != nil {
			err = fmt.Errorf("failed to validate program revision: %w", err)
			s.WriteResponseErr(err)
			return err
		}

		// update the size and root of the contract
		revision.FileMerkleRoot = pe.updater.MerkleRoot()
		revision.Filesize = rhp2.SectorSize * pe.updater.SectorCount()

		// verify the renter signature
		sigHash := rhp.HashRevision(revision)
		if !pe.revision.RenterKey().VerifyHash(sigHash, req.Signature) {
			err = ErrInvalidRenterSignature
			s.WriteResponseErr(err)
			return err
		}

		// sign and commit the revision
		signedRevision := contracts.SignedRevision{
			Revision:        revision,
			HostSignature:   pe.hostKey.SignHash(sigHash),
			RenterSignature: req.Signature,
		}
		// adjust the contract's usage to include the risked collateral. Storage
		// and bandwidth revenue will be credited to the account
		usage := contracts.Usage{
			RiskedCollateral: pe.cost.Collateral,
		}

		if err := pe.updater.Commit(signedRevision, usage); err != nil {
			s.WriteResponseErr(ErrHostInternalError)
			return fmt.Errorf("failed to commit revision: %w", err)
		}

		// send the signature to the renter
		resp := rhp3.RPCFinalizeProgramResponse{
			Signature: signedRevision.HostSignature,
		}
		if err := s.WriteResponse(&resp); err != nil {
			return fmt.Errorf("failed to write finalize response: %w", err)
		}
		pe.log.Debug("finalized contract", zap.String("contract", revision.ParentID.String()), zap.Duration("elapsed", time.Since(start)))
	}

	// commit the renter's spending
	if err := pe.budget.Commit(); err != nil {
		return fmt.Errorf("failed to commit budget: %w", err)
	}

	// commit the temporary sectors
	if len(pe.tempSectors) > 0 {
		if err := pe.storage.AddTemporarySectors(pe.tempSectors); err != nil {
			return fmt.Errorf("failed to commit temporary sectors: %w", err)
		}
	}
	return nil
}

// Sector returns a sector and its root from the program's data.
func (pd programData) Sector(offset uint64) (*[rhp2.SectorSize]byte, error) {
	if offset+rhp2.SectorSize > uint64(len(pd)) {
		return nil, fmt.Errorf("sector offset %v is out of bounds", offset)
	}

	sector := (*[rhp2.SectorSize]byte)(pd[offset : offset+rhp2.SectorSize])
	return sector, nil
}

// Bytes returns a slice of bytes from the program's data.
func (pd programData) Bytes(offset, length uint64) ([]byte, error) {
	if offset+length > uint64(len(pd)) {
		return nil, fmt.Errorf("bytes offset %v and length %v are out of bounds", offset, length)
	}
	return pd[offset : offset+length], nil
}

// Uint64 returns a little-endian uint64 from the program's data.
func (pd programData) Uint64(offset uint64) (uint64, error) {
	if offset+8 > uint64(len(pd)) {
		return 0, fmt.Errorf("uint64 offset %v is out of bounds", offset)
	}
	return binary.LittleEndian.Uint64(pd[offset:]), nil
}

// Hash returns a hash from the program's data.
func (pd programData) Hash(offset uint64) (types.Hash256, error) {
	if offset+32 > uint64(len(pd)) {
		return types.Hash256{}, fmt.Errorf("hash offset %v is out of bounds", offset)
	}
	return *(*types.Hash256)(pd[offset:]), nil
}

func (pd programData) UnlockKey(offset, length uint64) (types.UnlockKey, error) {
	if offset+length > uint64(len(pd)) {
		return types.UnlockKey{}, fmt.Errorf("unlock key offset %v is out of bounds", offset)
	}

	var key types.UnlockKey
	key.Algorithm = *(*types.Specifier)(pd[offset : offset+16])
	key.Key = pd[offset+16 : offset+length]
	return key, nil
}

func (pd programData) Signature(offset uint64) (types.Signature, error) {
	if offset+64 > uint64(len(pd)) {
		return types.Signature{}, fmt.Errorf("signature offset %v is out of bounds", offset)
	}
	return *(*types.Signature)(pd[offset:]), nil
}

// Execute executes the program's instructions
func (pe *programExecutor) Execute(ctx context.Context, s *rhp3.Stream) error {
	// create a cancellation context to stop the executeProgram goroutine
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// rollback any changes
	defer pe.rollback()

	for output := range pe.executeProgram(ctx) {
		start := time.Now()
		err := s.WriteResponse(&output)
		pe.log.Debug("wrote program output", zap.Int("outputLen", len(output.Output)), zap.Error(output.Error), zap.Duration("elapsed", time.Since(start)))
		if err != nil {
			return fmt.Errorf("failed to write program output: %w", err)
		} else if output.Error != nil {
			return output.Error
		}
	}
	if err := pe.commit(s); err != nil {
		return fmt.Errorf("failed to commit program: %w", err)
	}
	return nil
}

// Usage returns the program's usage.
func (pe *programExecutor) Usage() (usage contracts.Usage) {
	usage.RPCRevenue = pe.usage.RPCRevenue
	usage.StorageRevenue = pe.usage.StorageRevenue
	usage.IngressRevenue = pe.usage.IngressRevenue
	usage.EgressRevenue = pe.usage.EgressRevenue
	usage.RegistryRead = pe.usage.RegistryRead
	usage.RegistryWrite = pe.usage.RegistryWrite
	usage.RiskedCollateral = pe.cost.Collateral
	return usage
}

func (sh *SessionHandler) newExecutor(instructions []rhp3.Instruction, data []byte, pt rhp3.HostPriceTable, budget *accounts.Budget, revision *contracts.SignedRevision, finalize bool, log *zap.Logger) (*programExecutor, error) {
	ex := &programExecutor{
		hostKey: sh.privateKey,

		instructions: instructions,
		programData:  programData(data),

		priceTable: pt,
		budget:     budget,

		revision: revision,
		finalize: finalize,

		log:       log,
		contracts: sh.contracts,
		storage:   sh.storage,
		registry:  sh.registry,
	}

	if revision != nil {
		ex.remainingDuration = revision.Revision.WindowEnd - pt.HostBlockHeight
		updater, err := sh.contracts.ReviseContract(revision.Revision.ParentID)
		if err != nil {
			return nil, fmt.Errorf("failed to create contract updater: %w", err)
		}
		ex.updater = updater
	}
	return ex, nil
}

func instrLabel(instr rhp3.Instruction) string {
	switch instr.(type) {
	case *rhp3.InstrAppendSector:
		return "AppendSector"
	case *rhp3.InstrAppendSectorRoot:
		return "AppendSectorRoot"
	case *rhp3.InstrDropSectors:
		return "DropSectors"
	case *rhp3.InstrHasSector:
		return "HasSector"
	case *rhp3.InstrReadOffset:
		return "ReadOffset"
	case *rhp3.InstrReadSector:
		return "ReadSector"
	case *rhp3.InstrSwapSector:
		return "SwapSector"
	case *rhp3.InstrUpdateSector:
		return "UpdateSector"
	case *rhp3.InstrStoreSector:
		return "StoreSector"
	case *rhp3.InstrRevision:
		return "Revision"
	case *rhp3.InstrReadRegistry, *rhp3.InstrReadRegistryNoVersion:
		return "ReadRegistry"
	case *rhp3.InstrUpdateRegistry, *rhp3.InstrUpdateRegistryNoType:
		return "UpdateRegistry"
	default:
		panic(fmt.Sprintf("unknown instruction type %T", instr))
	}
}

func costToAccountUsage(cost rhp3.ResourceCost) accounts.Usage {
	return accounts.Usage{
		RPCRevenue:     cost.Base,
		StorageRevenue: cost.Storage,
		IngressRevenue: cost.Ingress,
		EgressRevenue:  cost.Egress,
	}
}
