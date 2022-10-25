package derive

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/hashicorp/go-multierror"

	"github.com/ethereum-optimism/optimism/op-node/eth"
	"github.com/ethereum-optimism/optimism/op-node/rollup"
)

var (
	ConfigUpdateEventABI      = "ConfigUpdate(uint256,uint8,bytes)"
	ConfigUpdateEventABIHash  = crypto.Keccak256Hash([]byte(ConfigUpdateEventABI))
	ConfigUpdateEventVersion0 = common.Hash{}
)

// UpdateSystemConfigWithL1Receipts filters all L1 receipts to find config updates and applies the config updates to the given sysCfg
func UpdateSystemConfigWithL1Receipts(sysCfg *eth.SystemConfig, receipts []*types.Receipt, cfg *rollup.Config) error {
	var result error
	for i, rec := range receipts {
		if rec.Status != types.ReceiptStatusSuccessful {
			continue
		}
		for j, log := range rec.Logs {
			if log.Address == cfg.L1SystemConfigAddress && len(log.Topics) > 0 && log.Topics[0] == ConfigUpdateEventABIHash {
				if err := ProcessSystemConfigUpdateLogEvent(sysCfg, log); err != nil {
					result = multierror.Append(result, fmt.Errorf("malformatted L1 system sysCfg log in receipt %d, log %d: %w", i, j, err))
				}
			}
		}
	}
	return result
}

// ProcessSystemConfigUpdateLogEvent decodes an EVM log entry emitted by the system config contract and applies it as a system config change.
//
// parse log data for:
//
//	event ConfigUpdate(
//	    uint256 indexed version,
//	    UpdateType indexed updateType,
//	    bytes data
//	);
func ProcessSystemConfigUpdateLogEvent(destSysCfg *eth.SystemConfig, ev *types.Log) error {
	if len(ev.Topics) != 3 {
		return fmt.Errorf("expected 3 event topics (event identity, indexed version, indexed updateType), got %d", len(ev.Topics))
	}
	if ev.Topics[0] != ConfigUpdateEventABIHash {
		return fmt.Errorf("invalid deposit event selector: %s, expected %s", ev.Topics[0], DepositEventABIHash)
	}

	// indexed 0
	version := ev.Topics[1]
	if version != ConfigUpdateEventVersion0 {
		return fmt.Errorf("unrecognized L1 sysCfg update event version: %s", version)
	}
	// indexed 1
	updateType := ev.Topics[2]
	// unindexed data
	switch updateType {
	case common.Hash{}:
		destSysCfg.BatcherAddr.SetBytes(ev.Data)
		return nil
	case common.Hash{31: 0x01}: // left padded uint8
		if len(ev.Data) != 32*2 {
			return fmt.Errorf("expected 32*2 bytes in GPO params update data, but got %d", len(ev.Data))
		}
		copy(destSysCfg.Overhead[:], ev.Data[:32])
		copy(destSysCfg.Scalar[:], ev.Data[32:])
		return nil
	default:
		return fmt.Errorf("unrecognized L1 sysCfg update type: %s", updateType)
	}
}
