package rhp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const maxProgramRequestSize = 20 << 20 // 20 MiB

var (
	// ErrTxnMissingContract is returned if the transaction set does not contain
	// any transactions or if the transaction does not contain exactly one
	// contract.
	ErrTxnMissingContract = errors.New("transaction set does not contain a file contract")
	// ErrHostInternalError is returned if the host encountered an error during
	// an RPC that doesn't need to be broadcast to the renter (e.g. insufficient
	// funds).
	ErrHostInternalError = errors.New("internal error")
	// ErrInvalidRenterSignature is returned when a contract's renter signature
	// is invalid.
	ErrInvalidRenterSignature = errors.New("invalid renter signature")
)

func readRequest(s *rhpv3.Stream, req rhpv3.ProtocolObject, maxLen uint64, timeout time.Duration) error {
	s.SetDeadline(time.Now().Add(30 * time.Second))
	if err := s.ReadRequest(req, maxLen); err != nil {
		return err
	}
	s.SetDeadline(time.Time{})
	return nil
}

// handleRPCPriceTable sends the host's price table to the renter.
func (sh *SessionHandler) handleRPCPriceTable(s *rhpv3.Stream) error {
	pt, err := sh.PriceTable()
	if err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to get price table: %w", err)
	}
	buf, err := json.Marshal(pt)
	if err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to marshal price table: %w", err)
	}

	resp := &rhpv3.RPCUpdatePriceTableResponse{
		PriceTableJSON: buf,
	}
	if err := s.WriteResponse(resp); err != nil {
		return fmt.Errorf("failed to send price table: %w", err)
	}

	// process the payment, catch connection closed errors since the renter
	// likely did not intend to pay
	budget, err := sh.processPayment(s)
	if err != nil {
		err = fmt.Errorf("failed to process payment: %w", err)
		s.WriteResponseErr(err)
		return err
	}
	defer budget.Rollback()

	if err := budget.Spend(pt.UpdatePriceTableCost); err != nil {
		err = fmt.Errorf("failed to pay %v for price table: %w", pt.UpdatePriceTableCost, err)
		s.WriteResponseErr(err)
		return err
	}

	if err := budget.Commit(); err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to commit payment: %w", err)
	}
	// register the price table for future use
	sh.priceTables.Register(pt)
	if err := s.WriteResponse(&rhpv3.RPCPriceTableResponse{}); err != nil {
		return fmt.Errorf("failed to send tracking response: %w", err)
	}
	return nil
}

func (sh *SessionHandler) handleRPCFundAccount(s *rhpv3.Stream) error {
	// read the price table ID from the stream
	pt, err := sh.readPriceTable(s)
	if err != nil {
		err = fmt.Errorf("failed to read price table: %w", err)
		s.WriteResponseErr(err)
		return err
	}

	// read the fund request from the stream
	var fundReq rhpv3.RPCFundAccountRequest
	if err := readRequest(s, &fundReq, 32, 30*time.Second); err != nil {
		return fmt.Errorf("failed to read fund account request: %w", err)
	}

	// process the payment for funding the account
	fundAmount, balance, err := sh.processFundAccountPayment(pt, s, fundReq.Account)
	if err != nil {
		err = fmt.Errorf("failed to process payment: %w", err)
		s.WriteResponseErr(err)
		return err
	}

	fundResp := &rhpv3.RPCFundAccountResponse{
		Balance: balance,
		Receipt: rhpv3.FundAccountReceipt{
			Host:      sh.HostKey(),
			Account:   fundReq.Account,
			Amount:    fundAmount,
			Timestamp: time.Now(),
		},
	}
	h := types.NewHasher()
	fundResp.Receipt.EncodeTo(h.E)
	fundResp.Signature = sh.privateKey.SignHash(h.Sum())

	// send the response
	if err := s.WriteResponse(fundResp); err != nil {
		return fmt.Errorf("failed to send fund account response: %w", err)
	}
	return nil
}

func (sh *SessionHandler) handleRPCAccountBalance(s *rhpv3.Stream) error {
	// get the price table to use for payment
	pt, err := sh.readPriceTable(s)
	if err != nil {
		err = fmt.Errorf("failed to read price table: %w", err)
		s.WriteResponseErr(err)
		return err
	}

	// read the payment from the stream
	budget, err := sh.processPayment(s)
	if err != nil {
		err = fmt.Errorf("failed to process payment: %w", err)
		s.WriteResponseErr(err)
		return err
	}
	defer budget.Rollback()

	// subtract the cost of the RPC
	if err := budget.Spend(pt.AccountBalanceCost); err != nil {
		err = fmt.Errorf("failed to pay %v for account balance: %w", pt.AccountBalanceCost, err)
		s.WriteResponseErr(err)
		return err
	}

	// read the account balance request from the stream
	var req rhpv3.RPCAccountBalanceRequest
	if err := readRequest(s, &req, 32, 30*time.Second); err != nil {
		return fmt.Errorf("failed to read account balance request: %w", err)
	}

	// get the account balance
	balance, err := sh.accounts.Balance(req.Account)
	if err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to get account balance: %w", err)
	}

	resp := &rhpv3.RPCAccountBalanceResponse{
		Balance: balance,
	}
	if err := budget.Commit(); err != nil {
		return fmt.Errorf("failed to commit payment: %w", err)
	} else if err := s.WriteResponse(resp); err != nil {
		return fmt.Errorf("failed to send account balance response: %w", err)
	}
	return nil
}

func (sh *SessionHandler) handleRPCLatestRevision(s *rhpv3.Stream) error {
	var req rhpv3.RPCLatestRevisionRequest
	if err := s.ReadRequest(&req, 4096); err != nil {
		return fmt.Errorf("failed to read latest revision request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	contract, err := sh.contracts.Lock(ctx, req.ContractID)
	if errors.Is(err, contracts.ErrNotFound) {
		s.WriteResponseErr(contracts.ErrNotFound)
		return fmt.Errorf("failed to lock contract: %w", err)
	}
	defer sh.contracts.Unlock(contract.Revision.ParentID)

	resp := &rhpv3.RPCLatestRevisionResponse{
		Revision: contract.Revision,
	}
	if err := s.WriteResponse(resp); err != nil {
		return fmt.Errorf("failed to send latest revision response: %w", err)
	}

	pt, err := sh.readPriceTable(s)
	if err != nil {
		err = fmt.Errorf("failed to read price table: %w", err)
		s.WriteResponseErr(err)
		return err
	}

	budget, err := sh.processPayment(s)
	if err != nil {
		err = fmt.Errorf("failed to process payment: %w", err)
		s.WriteResponseErr(err)
		return err
	}
	defer budget.Rollback()

	if err := budget.Spend(pt.LatestRevisionCost); err != nil {
		err = fmt.Errorf("failed to pay %v for latest revision: %w", pt.LatestRevisionCost, err)
		s.WriteResponseErr(err)
		return err
	} else if err := budget.Commit(); err != nil {
		return fmt.Errorf("failed to commit payment: %w", err)
	}
	return nil
}

func (sh *SessionHandler) handleRPCRenew(s *rhpv3.Stream) error {
	return nil
}

// handleRPCExecute handles an RPCExecuteProgram request.
func (sh *SessionHandler) handleRPCExecute(s *rhpv3.Stream) error {
	// read the price table
	pt, err := sh.readPriceTable(s)
	if err != nil {
		err = fmt.Errorf("failed to read price table: %w", err)
		s.WriteResponseErr(err)
		return err
	}

	// create the program budget
	budget, err := sh.processPayment(s)
	if err != nil {
		err = fmt.Errorf("failed to process payment: %w", err)
		s.WriteResponseErr(err)
		return err
	}
	// note: the budget is committed by the executor, no need to commit it in the handler.
	defer budget.Rollback()

	// read the program request
	var executeReq rhpv3.RPCExecuteProgramRequest
	if err := s.ReadRequest(&executeReq, maxProgramRequestSize); err != nil {
		return fmt.Errorf("failed to read execute request: %w", err)
	}
	instructions := executeReq.Program

	// pay for the execution
	if err := budget.Spend(pt.InitBaseCost); err != nil {
		err = fmt.Errorf("failed to pay program init cost: %w", err)
		s.WriteResponseErr(err)
		return err
	}

	var requiresContract, requiresFinalization bool
	for _, instr := range instructions {
		requiresContract = requiresContract || instr.RequiresContract()
		requiresFinalization = requiresFinalization || instr.RequiresFinalization()
	}

	// if the program requires a contract, lock it
	var contract contracts.SignedRevision
	if requiresContract || requiresFinalization {
		if executeReq.FileContractID == (types.FileContractID{}) {
			err = ErrContractRequired
			s.WriteResponseErr(err)
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		contract, err = sh.contracts.Lock(ctx, executeReq.FileContractID)
		if err != nil {
			err = fmt.Errorf("failed to lock contract: %w", err)
			s.WriteResponseErr(err)
			return err
		}
		defer sh.contracts.Unlock(contract.Revision.ParentID)
	}

	// generate a cancellation token and write it to the stream. Currently just
	// a placeholder.
	cancelToken := types.Specifier(frand.Entropy128())
	if err := s.WriteResponse(&cancelToken); err != nil {
		return fmt.Errorf("failed to write cancel token: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// create the program executor
	log := sh.log.Named("mdm")
	log.Debug("executing program", zap.Int("instructions", len(instructions)), zap.String("budget", budget.Remaining().ExactString()), zap.String("contract", contract.Revision.ParentID.String()), zap.Bool("requiresFinalization", requiresFinalization))
	// note: the budget is committed by the executor, no need to commit it in the handler.
	executor, err := sh.newExecutor(instructions, executeReq.ProgramData, pt, budget, contract, requiresFinalization, log)
	if err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to create program executor: %w", err)
	} else if err := executor.Execute(ctx, s); err != nil {
		return fmt.Errorf("failed to execute program: %w", err)
	}

	return nil
}
