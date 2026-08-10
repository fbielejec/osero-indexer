//! Typed `sol!` bindings for the on-chain sources this indexer reads.
//!
//! Every event/function signature below was taken verbatim from the Etherscan-verified
//! ABI (chainid=1). The five ERC-1967 proxies (USDS, sUSDS, Spark Pool, spUSDS aToken,
//! variableDebtUSDS) were resolved to their implementation ABIs first; AllocatorVault is
//! not a proxy. `#[sol(rpc)]` is applied only to interfaces we make calls against.

use alloy::sol;

// AllocatorVault (vault) — not a proxy. Events only; both are `(address indexed sender,
// uint256 wad)`, confirmed against the verified ABI.
sol! {
    interface IAllocatorVault {
        event Draw(address indexed sender, uint256 wad);
        event Wipe(address indexed sender, uint256 wad);
    }
}

// USDS token (usds) — proxy; impl ABI is standard ERC-20. Only the Transfer event is
// consumed.
sol! {
    interface IUsds {
        event Transfer(address indexed from, address indexed to, uint256 value);
    }
}

// sUSDS (susds) — proxy. The SSR is changed via `File(bytes32 indexed what, uint256 data)`
// (the `what` key is "ssr"); there is no ssr-specific event. Current SSR is read via
// `ssr()`. A `Drip(uint256 chi, uint256 diff)` event also exists but does not carry the
// SSR value, so `File` is the change signal.
sol! {
    #[sol(rpc)]
    interface ISUsds {
        event File(bytes32 indexed what, uint256 data);
        function ssr() external view returns (uint256);
    }
}

// Spark Pool (venue_pool, Aave-V3 fork) — proxy. Event/function match the verified impl ABI.
// `getReserveData` returns the full Aave-V3 `ReserveData` struct; we read
// `currentLiquidityRate` (supply APR, RAY) from it for the live spot supply yield.
sol! {
    #[sol(rpc)]
    interface ISparkPool {
        struct ReserveDataLegacy {
            uint256 configuration;
            uint128 liquidityIndex;
            uint128 currentLiquidityRate;
            uint128 variableBorrowIndex;
            uint128 currentVariableBorrowRate;
            uint128 currentStableBorrowRate;
            uint40 lastUpdateTimestamp;
            uint16 id;
            address aTokenAddress;
            address stableDebtTokenAddress;
            address variableDebtTokenAddress;
            address interestRateStrategyAddress;
            uint128 accruedToTreasury;
            uint128 unbacked;
            uint128 isolationModeTotalDebt;
        }
        event ReserveDataUpdated(
            address indexed reserve,
            uint256 liquidityRate,
            uint256 stableBorrowRate,
            uint256 variableBorrowRate,
            uint256 liquidityIndex,
            uint256 variableBorrowIndex
        );
        function getReserveNormalizedIncome(address asset) external view returns (uint256);
        function getReserveData(address asset) external view returns (ReserveDataLegacy memory);
    }
}

// Vat (Sky/Maker CDP engine) — the ilk's normalized debt lives here. `urns(ilk,urn).art`
// times `ilks(ilk).rate / 1e27` is the drawn USDS. Read only by the live smoke test.
sol! {
    #[sol(rpc)]
    interface IVat {
        function urns(bytes32 ilk, address urn) external view returns (uint256 ink, uint256 art);
        function ilks(bytes32 ilk)
            external
            view
            returns (uint256 Art, uint256 rate, uint256 spot, uint256 line, uint256 dust);
    }
}

// spUSDS aToken (venue_atoken) — proxy. Scaled-balance token; all return uint256.
sol! {
    #[sol(rpc)]
    interface ISpUsds {
        function totalSupply() external view returns (uint256);
        function scaledBalanceOf(address user) external view returns (uint256);
        function balanceOf(address user) external view returns (uint256);
    }
}

// variableDebtUSDS (venue_vdebt) — proxy. Total variable debt.
sol! {
    #[sol(rpc)]
    interface IVariableDebtUsds {
        function totalSupply() external view returns (uint256);
    }
}
