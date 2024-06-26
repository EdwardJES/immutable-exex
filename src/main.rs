use std::sync::{Arc, Mutex, MutexGuard};

use alloy_sol_types::sol;
use reth_exex::ExExContext;
use reth_node_api::FullNodeComponents;
use reth_primitives::{address, Address};
use rusqlite::Connection;

// DB
const DB_PATH: &'static str = "bridge.db";

// ABI
sol!(RootERC20Bridge, "root_erc20_bridge_abi.json");
use RootERC20Bridge::{
    ChildChainERC20Deposit, IMXDeposit, NativeEthDeposit, RootChainERC20Withdraw,
    RootChainETHWithdraw, WETHDeposit,
};

// L1 contract addr
const IMMUTABLE_BRIDGE: Address = address!("Ba5E35E26Ae59c7aea6F029B68c6460De2d13eB6");

pub struct Database {
    connection: Arc<Mutex<Connection>>,
}

impl Database {
    pub fn new(connection: Connection) -> eyre::Result<Self> {
        let db = Self {
            connection: Arc::new(Mutex::new(connection)),
        };
        db.create_tables()?;
        Ok(db)
    }

    fn connection(&self) -> MutexGuard<'_, Connection> {
        self.connection.lock().expect("failed to aquire db lock")
    }

    fn create_tables(&self) -> eyre::Result<()> {
        // Create deposits and withdrawals tables
        self.connection().execute(
            r#"
            CREATE TABLE IF NOT EXISTS deposits (
                id               INTEGER PRIMARY KEY,
                block_number     INTEGER NOT NULL,
                tx_hash          TEXT NOT NULL UNIQUE,
                contract_address TEXT NOT NULL,
                "from"           TEXT NOT NULL,
                "to"             TEXT NOT NULL,
                amount           TEXT NOT NULL
            );
            "#,
            (),
        )?;
        self.connection().execute(
            r#"
            CREATE TABLE IF NOT EXISTS withdrawals (
                id               INTEGER PRIMARY KEY,
                block_number     INTEGER NOT NULL,
                tx_hash          TEXT NOT NULL UNIQUE,
                contract_address TEXT NOT NULL,
                "from"           TEXT NOT NULL,
                "to"             TEXT NOT NULL,
                amount           TEXT NOT NULL
            );
            "#,
            (),
        )?;

        Ok(())
    }
}

pub struct ImmutableBridgeReader<Node: FullNodeComponents> {
    ctx: ExExContext<Node>,
    db: Database,
}

fn main() -> eyre::Result<()> {
    Ok(())
}
