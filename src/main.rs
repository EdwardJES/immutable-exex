use alloy_sol_types::{sol};
use reth_primitives::{address, Address};

sol!(RootERC20Bridge, "root_erc20_bridge_abi.json");
use RootERC20Bridge::{ChildChainERC20Deposit, IMXDeposit, WETHDeposit, NativeEthDeposit, RootChainERC20Withdraw, RootChainETHWithdraw};

const IMMUTABLE_BRIDGE: Address = address!("Ba5E35E26Ae59c7aea6F029B68c6460De2d13eB6");

fn main() -> eyre::Result<()> {
    println!("Hello, world!");

    Ok(())
}
