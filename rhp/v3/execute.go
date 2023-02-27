package rhp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/rhp"
	"go.uber.org/zap"
)

type (
	programData []byte

	programExecutor struct {
		hostKey   types.PrivateKey
		renterKey types.PublicKey

		instructions []rhpv3.Instruction
		programData  programData
		priceTable   rhpv3.HostPriceTable

		budget *accounts.Budget
		cost   rhpv3.ResourceCost

		revision          types.FileContractRevision
		remainingDuration uint64
		updater           *contracts.ContractUpdater

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

func (pe *programExecutor) errorOutput(err error) rhpv3.RPCExecuteProgramResponse {
	output := rhpv3.RPCExecuteProgramResponse{
		AdditionalCollateral: pe.cost.Collateral,
		TotalCost:            pe.cost.Base.Add(pe.cost.Storage).Add(pe.cost.Egress).Add(pe.cost.Ingress),
		FailureRefund:        pe.cost.Storage,
		Error:                err,
	}
	if pe.updater != nil {
		output.NewMerkleRoot = pe.updater.MerkleRoot()
		output.NewSize = pe.updater.SectorCount() * rhpv2.SectorSize
	}
	return output
}

func (pe *programExecutor) instructionOutput(output []byte, proof []types.Hash256) rhpv3.RPCExecuteProgramResponse {
	resp := rhpv3.RPCExecuteProgramResponse{
		AdditionalCollateral: pe.cost.Collateral,
		TotalCost:            pe.cost.Base.Add(pe.cost.Storage).Add(pe.cost.Egress).Add(pe.cost.Ingress),
		FailureRefund:        pe.cost.Storage,
		OutputLength:         uint64(len(output)),
		Proof:                proof,
		Output:               output,
	}
	if pe.updater != nil {
		resp.NewMerkleRoot = pe.updater.MerkleRoot()
		resp.NewSize = pe.updater.SectorCount() * rhpv2.SectorSize
	}
	return resp
}

func (pe *programExecutor) payForExecution(cost rhpv3.ResourceCost) error {
	executeCost := cost.Base.Add(cost.Storage).Add(cost.Egress).Add(cost.Ingress)
	if err := pe.budget.Spend(executeCost); err != nil {
		return err
	}
	pe.cost = pe.cost.Add(cost)
	return nil
}

func (pe *programExecutor) executeAppendSector(instr *rhpv3.InstrAppendSector) ([]byte, []types.Hash256, error) {
	root, sector, err := pe.programData.Sector(instr.SectorDataOffset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector: %w", err)
	}
	// pay for execution
	if err := pe.payForExecution(pe.priceTable.AppendSectorCost(pe.remainingDuration)); err != nil {
		return nil, nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	release, err := pe.storage.Write(root, sector)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to write sector: %w", err)
	}
	pe.releaseFuncs = append(pe.releaseFuncs, release)
	pe.updater.AppendSector(root)
	var proof []types.Hash256
	return nil, proof, nil
}

func (pe *programExecutor) executeAppendSectorRoot(instr *rhpv3.InstrAppendSectorRoot) ([]byte, []types.Hash256, error) {
	root, err := pe.programData.Hash(instr.MerkleRootOffset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector root: %w", err)
	}
	// pay for execution
	if err := pe.payForExecution(pe.priceTable.AppendSectorRootCost(pe.remainingDuration)); err != nil {
		return nil, nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	// lock the sector to prevent it from being garbage collected
	release, err := pe.storage.LockSector(root)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector: %w", err)
	}
	pe.releaseFuncs = append(pe.releaseFuncs, release)
	pe.updater.AppendSector(root)
	var proof []types.Hash256
	return nil, proof, nil
}

func (pe *programExecutor) executeDropSectors(instr *rhpv3.InstrDropSectors) ([]byte, []types.Hash256, error) {
	count, err := pe.programData.Uint64(instr.SectorCountOffset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector count: %w", err)
	}
	// pay for execution
	if err := pe.payForExecution(pe.priceTable.DropSectorsCost(count)); err != nil {
		return nil, nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	// trim the sectors
	if err := pe.updater.TrimSectors(count); err != nil {
		return nil, nil, fmt.Errorf("failed to drop sectors: %w", err)
	}

	var proof []types.Hash256
	if instr.ProofRequired {
		n := pe.updater.SectorCount()
		proof = rhpv2.BuildSectorRangeProof(pe.updater.SectorRoots(), n+count, n)
	}
	return nil, proof, nil
}

func (pe *programExecutor) executeHasSector(instr *rhpv3.InstrHasSector) ([]byte, []types.Hash256, error) {
	root, err := pe.programData.Hash(instr.MerkleRootOffset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector root: %w", err)
	}
	// pay for execution
	if err := pe.payForExecution(pe.priceTable.HasSectorCost()); err != nil {
		return nil, nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	var has bool
	release, err := pe.storage.LockSector(root)
	if err != nil && !errors.Is(err, storage.ErrSectorNotFound) {
		return nil, nil, fmt.Errorf("failed to locate sector: %w", err)
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

func (pe *programExecutor) executeReadOffset(instr *rhpv3.InstrReadOffset) ([]byte, []types.Hash256, error) {
	offset, err := pe.programData.Uint64(instr.OffsetOffset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read offset: %w", err)
	}
	length, err := pe.programData.Uint64(instr.LengthOffset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read length: %w", err)
	}
	// pay for execution
	if err := pe.payForExecution(pe.priceTable.ReadOffsetCost(length)); err != nil {
		return nil, nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	sectorIndex := offset / rhpv2.SectorSize
	relOffset := offset % rhpv2.SectorSize

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

	var proof []types.Hash256
	return sector[relOffset : relOffset+length], proof, nil
}

func (pe *programExecutor) executeReadSector(instr *rhpv3.InstrReadSector) ([]byte, []types.Hash256, error) {
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
	if offset+length > rhpv2.SectorSize {
		return nil, nil, fmt.Errorf("read length %v is out of bounds", length)
	}

	// pay for execution
	if err := pe.payForExecution(pe.priceTable.ReadSectorCost(length)); err != nil {
		return nil, nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	sector, err := pe.storage.Read(root)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector: %w", err)
	}

	// if no proof was requested, return the data
	if !instr.ProofRequired {
		return sector[offset : offset+length], nil, nil
	}

	var proof []types.Hash256
	return sector[offset : offset+length], proof, nil
}

func (pe *programExecutor) swapSector(instr *rhpv3.InstrSwapSector) ([]byte, []types.Hash256, error) {
	// read the swap params
	a, err := pe.programData.Uint64(instr.Sector1Offset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector 1 index: %w", err)
	}
	b, err := pe.programData.Uint64(instr.Sector2Offset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector 2 index: %w", err)
	}

	// pay for execution
	if err := pe.payForExecution(pe.priceTable.SwapSectorCost()); err != nil {
		return nil, nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	if err := pe.updater.SwapSectors(a, b); err != nil {
		return nil, nil, fmt.Errorf("failed to swap sectors: %w", err)
	}

	var proof []types.Hash256
	return nil, proof, nil
}

func (pe *programExecutor) executeUpdateSector(instr *rhpv3.InstrUpdateSector) ([]byte, []types.Hash256, error) {
	offset, length := instr.Offset, instr.Length
	// read the patch
	patch, err := pe.programData.Bytes(instr.DataOffset, length)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read patch: %w", err)
	}

	// pay for execution
	if err := pe.payForExecution(pe.priceTable.UpdateSectorCost(instr.Length)); err != nil {
		return nil, nil, fmt.Errorf("failed to pay for instruction: %w", err)
	}

	sectorIndex := offset / rhpv2.SectorSize
	relOffset := offset % rhpv2.SectorSize

	oldRoot, err := pe.updater.SectorRoot(sectorIndex)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get root: %w", err)
	}

	sector, err := pe.storage.Read(oldRoot)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sector: %w", err)
	}

	// validate and apply the patch
	if relOffset+length > rhpv2.SectorSize {
		return nil, nil, fmt.Errorf("update offset %v length %v is out of bounds", relOffset, length)
	}
	copy(sector[relOffset:], patch)

	// store the new sector
	newRoot := rhpv2.SectorRoot((*[rhpv2.SectorSize]byte)(sector))
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

func (pe *programExecutor) executeProgram(ctx context.Context) <-chan rhpv3.RPCExecuteProgramResponse {
	outputs := make(chan rhpv3.RPCExecuteProgramResponse, len(pe.instructions))
	go func() {
		defer close(outputs)

		var output []byte
		var proof []types.Hash256
		var err error
		for _, instruction := range pe.instructions {
			select {
			case <-ctx.Done():
				outputs <- pe.errorOutput(ctx.Err())
				return
			default:
			}

			start := time.Now()
			var logLabel string
			// execute the instruction
			switch instr := instruction.(type) {
			case *rhpv3.InstrAppendSector:
				output, proof, err = pe.executeAppendSector(instr)
				logLabel = "append sector"
			case *rhpv3.InstrAppendSectorRoot:
				output, proof, err = pe.executeAppendSectorRoot(instr)
				logLabel = "append sector root"
			case *rhpv3.InstrDropSectors:
				output, proof, err = pe.executeDropSectors(instr)
				logLabel = "drop sectors"
			case *rhpv3.InstrHasSector:
				output, proof, err = pe.executeHasSector(instr)
				logLabel = "has sector"
			case *rhpv3.InstrReadOffset:
				output, proof, err = pe.executeReadOffset(instr)
				logLabel = "read offset"
			case *rhpv3.InstrReadSector:
				output, proof, err = pe.executeReadSector(instr)
				logLabel = "read sector"
			case *rhpv3.InstrSwapSector:
				output, proof, err = pe.swapSector(instr)
				logLabel = "swap sector"
			case *rhpv3.InstrUpdateSector:
				output, proof, err = pe.executeUpdateSector(instr)
				logLabel = "update sector"
			case *rhpv3.InstrStoreSector:
				//output, proof, err = pe.executeStoreSector(instr)
			case *rhpv3.InstrRevision:
			case *rhpv3.InstrReadRegistry, *rhpv3.InstrReadRegistryNoVersion:
			case *rhpv3.InstrUpdateRegistry, *rhpv3.InstrUpdateRegistryNoType:
			}
			if err != nil {
				outputs <- pe.errorOutput(fmt.Errorf("failed to execute instruction %v: %w", logLabel, err))
				return
			}
			outputs <- pe.instructionOutput(output, proof)
			pe.log.Debug("executed instruction", zap.String("instruction", logLabel), zap.Duration("elapsed", time.Since(start)))
		}
	}()
	return outputs
}

func (pe *programExecutor) release() error {
	for _, release := range pe.releaseFuncs {
		if err := release(); err != nil {
			return err
		}
	}
	return nil
}

func (pe *programExecutor) rollback() error {
	if pe.committed {
		return nil
	}

	if pe.updater != nil {
		pe.updater.Close()
	}

	// release all of the locked sectors. Any sectors not referenced by a
	// contract or temporary storage will eventually be garbage collected.
	if err := pe.release(); err != nil {
		return fmt.Errorf("failed to release storage: %w", err)
	}
	// refund the storage spending
	pe.budget.Refund(pe.cost.Storage)
	if err := pe.budget.Commit(); err != nil {
		return fmt.Errorf("failed to commit budget: %w", err)
	}
	return nil
}

func (pe *programExecutor) commit(s *rhpv3.Stream) error {
	if pe.committed {
		panic("commit called multiple times")
	}

	pe.committed = true

	// commit the renter's spending
	if err := pe.budget.Commit(); err != nil {
		return fmt.Errorf("failed to commit budget: %w", err)
	}

	// finalize the program
	if pe.finalize {
		start := time.Now()
		defer pe.updater.Close() // close the updater
		// read the finalize request
		var req rhpv3.RPCFinalizeProgramRequest
		if err := s.ReadResponse(&req, 1024); err != nil {
			return fmt.Errorf("failed to read finalize request: %w", err)
		}
		pe.log.Debug("received finalize request", zap.Uint64("revision number", req.RevisionNumber), zap.String("contract", pe.revision.ParentID.String()), zap.Duration("elapsed", time.Since(start)))
		start = time.Now()

		// revise the contract with the values received from the renter
		existing := pe.revision
		revision, err := rhp.Revise(existing, req.RevisionNumber, req.ValidProofValues, req.MissedProofValues)
		if err != nil {
			err = fmt.Errorf("failed to revise contract: %w", err)
			s.WriteResponseErr(err)
			return err
		} else if err := rhp.ValidateProgramRevision(existing, revision, pe.cost.Storage, pe.cost.Collateral); err != nil {
			err = fmt.Errorf("failed to validate program revision: %w", err)
			s.WriteResponseErr(err)
			return err
		}

		// update the size and root of the contract
		revision.FileMerkleRoot = pe.updater.MerkleRoot()
		revision.Filesize = rhpv2.SectorSize * pe.updater.SectorCount()
		pe.log.Debug("revised contract", zap.Uint64("revision number", revision.RevisionNumber), zap.String("root", revision.FileMerkleRoot.String()), zap.Uint64("size", revision.Filesize), zap.String("contract", revision.ParentID.String()), zap.Duration("elapsed", time.Since(start)))
		start = time.Now()

		// verify the renter signature
		sigHash := rhp.HashRevision(revision)
		if !pe.renterKey.VerifyHash(sigHash, req.Signature) {
			err = ErrInvalidRenterSignature
			s.WriteResponseErr(err)
			return err
		}
		pe.log.Debug("verified renter signature", zap.String("contract", revision.ParentID.String()), zap.Duration("elapsed", time.Since(start)))

		// sign and commit the revision
		signedRevision := contracts.SignedRevision{
			Revision:        revision,
			HostSignature:   pe.hostKey.SignHash(sigHash),
			RenterSignature: req.Signature,
		}
		if err := pe.updater.Commit(signedRevision); err != nil {
			s.WriteResponseErr(ErrHostInternalError)
			return fmt.Errorf("failed to commit revision: %w", err)
		}
		pe.log.Debug("committed revision", zap.String("contract", revision.ParentID.String()), zap.Duration("elapsed", time.Since(start)))

		// send the signature to the renter
		resp := rhpv3.RPCFinalizeProgramResponse{
			Signature: signedRevision.HostSignature,
		}
		if err := s.WriteResponse(&resp); err != nil {
			return fmt.Errorf("failed to write finalize response: %w", err)
		}
		pe.log.Debug("sent finalize response", zap.String("contract", revision.ParentID.String()), zap.Duration("elapsed", time.Since(start)))
	}

	// release all of the locked sectors. Any sectors not referenced by a
	// contract or temporary storage will eventually be garbage collected.
	if err := pe.release(); err != nil {
		return fmt.Errorf("failed to release storage: %w", err)
	}
	return nil
}

// Sector returns a sector and its root from the program's data.
func (pd programData) Sector(offset uint64) (types.Hash256, *[rhpv2.SectorSize]byte, error) {
	if offset+rhpv2.SectorSize > uint64(len(pd)) {
		return types.Hash256{}, nil, fmt.Errorf("sector offset %v is out of bounds", offset)
	}

	sector := (*[rhpv2.SectorSize]byte)(pd[offset : offset+rhpv2.SectorSize])
	root := rhpv2.SectorRoot(sector)
	return root, sector, nil
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

// Execute executes the program's instructions
func (pe *programExecutor) Execute(ctx context.Context, s *rhpv3.Stream) error {
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

func (sh *SessionHandler) newExecutor(instructions []rhpv3.Instruction, data []byte, pt rhpv3.HostPriceTable, budget *accounts.Budget, revision contracts.SignedRevision, finalize bool, log *zap.Logger) (*programExecutor, error) {
	ex := &programExecutor{
		hostKey:   sh.privateKey,
		renterKey: revision.RenterKey(),

		instructions: instructions,
		programData:  programData(data),

		priceTable: pt,
		budget:     budget,

		revision: revision.Revision,
		finalize: finalize,

		log:       log,
		contracts: sh.contracts,
		storage:   sh.storage,
		registry:  sh.registry,
	}

	if finalize {
		updater, err := sh.contracts.ReviseContract(revision.Revision.ParentID)
		if err != nil {
			return nil, fmt.Errorf("failed to create contract updater: %w", err)
		}
		ex.updater = updater
	}
	return ex, nil
}
