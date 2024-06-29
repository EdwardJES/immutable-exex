use std::sync::{Arc, Mutex, MutexGuard};

use alloy_sol_types::sol;
use reth_exex::{ExExContext, ExExNotification};
use reth_node_api::FullNodeComponents;
use reth_primitives::{address, revm_primitives::FixedBytes, ruint::Uint, Address};
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

    fn insert_event(&mut self, event: BridgeEvent) -> eyre::Result<()> {
        let mut connection = self.connection();
        let tx = connection.transaction()?;
        match event.source {
            SourceChain::Root => {
                tx.execute(
                        r#"
                            INSERT INTO deposits (block_number, tx_hash, root_token, child_token, "from", "to", amount)
                            VALUES (?, ?, ?, ?, ?, ?)
                            "#,
                    (
                        event.block_number,
                        event.tx_hash.to_string(),
                        event.root_token.to_string(),
                        event.child_token.to_string(),
                        event.from.to_string(),
                        event.to.to_string(),
                        event.amount.to_string(),
                    ),
                )?;
            }
            SourceChain::Child => {
                tx.execute(
                    r#"
                        INSERT INTO withdrawals (block_number, tx_hash, root_token, child_token, "from", "to", amount)
                        VALUES (?, ?, ?, ?, ?, ?)
                        "#,
                (
                    event.block_number,
                    event.tx_hash.to_string(),
                    event.root_token.to_string(),
                    event.child_token.to_string(),
                    event.from.to_string(),
                    event.to.to_string(),
                    event.amount.to_string(),
                ),
            )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn create_tables(&self) -> eyre::Result<()> {
        // Create deposits and withdrawals tables
        self.connection().execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS deposits (
                id               INTEGER PRIMARY KEY,
                block_number     INTEGER NOT NULL,
                tx_hash          TEXT NOT NULL UNIQUE,
                root_token       TEXT NOT NULL,
                child_token      TEXT NOT NULL,
                "from"           TEXT NOT NULL,
                "to"             TEXT NOT NULL,
                amount           TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS withdrawals (
                id               INTEGER PRIMARY KEY,
                block_number     INTEGER NOT NULL,
                tx_hash          TEXT NOT NULL UNIQUE,
                root_token       TEXT NOT NULL,
                child_token      TEXT NOT NULL,
                "from"           TEXT NOT NULL,
                "to"             TEXT NOT NULL,
                amount           TEXT NOT NULL
            );
            "#,
        )?;
        Ok(())
    }
}

enum SourceChain {
    Root,
    Child,
}
struct BridgeEvent {
    source: SourceChain,
    block_number: u64,
    tx_hash: FixedBytes<32>,
    root_token: Address,
    child_token: Address,
    from: Address,
    to: Address,
    amount: Uint<256, 4>,
}

pub struct ImmutableBridgeReader<Node: FullNodeComponents> {
    ctx: ExExContext<Node>,
    db: Database,
}

impl<Node: FullNodeComponents> ImmutableBridgeReader<Node> {
    fn new(ctx: ExExContext<Node>, db: Database) -> eyre::Result<Self> {
        Ok(Self { ctx, db })
    }

    async fn run(&mut self) -> eyre::Result<()> {
        while let Some(notification) = self.ctx.notifications.recv().await {
            match &notification {
                ExExNotification::ChainCommitted { new } => {
                    // do something
                }
                ExExNotification::ChainReorged { old, new } => {
                    // do something
                }
                ExExNotification::ChainReverted { old } => {
                    // do something
                    todo!()
                }
            };
        }
        Ok(())
    }
}

fn main() -> eyre::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy_sol_types::{abi::Token, SolEvent};
    use reth_exex_test_utils::{test_exex_context, PollOnce};
    use reth_primitives::{
        Address, Block, Header, Log, Receipt, Transaction, TransactionSigned, TxEip1559, TxKind,
        TxType, U256,
    };
    use reth_testing_utils::generators::sign_tx_with_random_key_pair;
    use std::pin::pin;
    use RootERC20Bridge::RootERC20BridgeEvents;

    use super::*;

    fn construct_tx_and_receipt<E: SolEvent>(
        to: Address,
        event: E,
    ) -> eyre::Result<(TransactionSigned, Receipt)> {
        // Construct dynamic tx
        let tx: Transaction = Transaction::Eip1559(TxEip1559 {
            to: TxKind::Call(to),
            ..Default::default()
        });

        // Construct log
        let log = Log::new(
            to,
            event
                .encode_topics()
                .into_iter()
                .map(|topic| topic.0)
                .collect(),
            event.encode_data().into(),
        )
        .ok_or_else(|| eyre::eyre!("failed to encode event"))?;
        #[allow(clippy::needless_update)] // side-effect of optimism fields

        // Construct receipt
        let receipt = Receipt {
            tx_type: TxType::Eip1559,
            success: true,
            cumulative_gas_used: 0,
            logs: vec![log],
            ..Default::default()
        };
        Ok((
            sign_tx_with_random_key_pair(&mut rand::thread_rng(), tx),
            receipt,
        ))
    }

    #[tokio::test]
    async fn test_exec() -> eyre::Result<()> {
        let (ctx, handle) = test_exex_context().await?;

        // Create a temporary database file, so we can access it later for assertions
        let db_file = tempfile::NamedTempFile::new()?;

        // Initialize the ExEx
        let mut exex = pin!(super::init(ctx, Connection::open(&db_file)?).await?);

        // Generate random "from" and "to" addresses for deposit and withdrawal events
        let from_address = Address::random();
        let to_address = Address::random();
        let root_token = Address::random();
        let child_token = Address::random();

        // Construct deposit event, transaction and receipt
        let deposit_event = RootERC20Bridge::NativeEthDeposit {
            rootToken: root_token,
            childToken: child_token,
            depositor: from_address,
            receiver: to_address,
            amount: U256::from(10),
        };

        // let (deposit_tx, deposit_tx_receipt) =
        // construct_tx_and_receipt(IMMUTABLE_BRIDGE, deposit_event.clone())?;

        // // Construct withdrawal event, transaction and receipt
        // let withdrawal_event = L1StandardBridge::ETHBridgeFinalized {
        //     from: from_address,
        //     to: to_address,
        //     amount: U256::from(200),
        //     extraData: Default::default(),
        // };
        // let (withdrawal_tx, withdrawal_tx_receipt) =
        //     construct_tx_and_receipt(OP_BRIDGES[1], withdrawal_event.clone())?;

        // Construct a block
        // let block = Block {
        //     header: Header::default(),
        //     body: vec![deposit_tx, withdrawal_tx],
        //     ..Default::default()
        // }
        // .seal_slow()
        // .seal_with_senders()
        // .ok_or_else(|| eyre::eyre!("failed to recover senders"))?;

        // // Construct a chain
        // let chain = Chain::new(
        //     vec![block.clone()],
        //     ExecutionOutcome::new(
        //         BundleState::default(),
        //         vec![deposit_tx_receipt, withdrawal_tx_receipt].into(),
        //         block.number,
        //         vec![block.requests.clone().unwrap_or_default()],
        //     ),
        //     None,
        // );
    }
}
