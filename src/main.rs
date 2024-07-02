use std::sync::{Arc, Mutex, MutexGuard};

use alloy_sol_types::{sol, SolEventInterface};
use eyre::eyre;
use futures::Future;
use reth_exex::{ExExContext, ExExEvent};
use reth_node_api::FullNodeComponents;
use reth_node_ethereum::EthereumNode;
use reth_primitives::{
    address, revm_primitives::FixedBytes, ruint::Uint, Address, SealedBlockWithSenders,
    TransactionSigned,
};
use reth_provider::Chain;
use reth_tracing::tracing::info;
use rusqlite::Connection;

// DB
const DB_PATH: &'static str = "bridge.db";

// ABI
sol!(RootERC20Bridge, "root_erc20_bridge_abi.json");
use RootERC20Bridge::RootERC20BridgeEvents;

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

    fn insert_event(&mut self, event: BridgeEvent) -> rusqlite::Result<()> {
        let mut connection = self.connection();
        let tx = connection.transaction()?;
        match event.source {
            EventSource::Root => {
                tx.execute(
                        r#"
                            INSERT INTO deposits (block_number, tx_hash, root_token, child_token, "from", "to", amount)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
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
            EventSource::Child => {
                tx.execute(
                    r#"
                        INSERT INTO withdrawals (block_number, tx_hash, root_token, child_token, "from", "to", amount)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
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

    fn delete_event(&mut self, event: BridgeEvent) -> rusqlite::Result<()> {
        let mut connection = self.connection();
        let tx = connection.transaction()?;
        match event.source {
            EventSource::Root => {
                tx.execute(
                    "DELETE FROM deposits WHERE tx_hash = ?;",
                    (event.tx_hash.to_string(),),
                )?;
            }
            EventSource::Child => {
                tx.execute(
                    "DELETE FROM withdrawals WHERE tx_hash = ?;",
                    (event.tx_hash.to_string(),),
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

enum EventSource {
    Root,
    Child,
}
struct BridgeEvent {
    source: EventSource,
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
    async fn init(
        ctx: ExExContext<Node>,
        connection: Connection,
    ) -> eyre::Result<impl Future<Output = eyre::Result<()>>> {
        let db = Database::new(connection)?;
        Ok(Self { ctx, db }.run())
    }

    async fn run(mut self) -> eyre::Result<()> {
        while let Some(notification) = self.ctx.notifications.recv().await {
            if let Some(comitted_chain) = notification.committed_chain() {
                info!("Chain committed");
                let events = parse_chain_into_events(&comitted_chain);
                for (block, tx, event) in events {
                    let bridge_event = to_bridge_event(block, tx, event)?;
                    self.db.insert_event(bridge_event)?;
                    info!("Inserted committed events to db");
                }

                // Signal to exex to not notify on any comitted blocks below this height
                self.ctx
                    .events
                    .send(ExExEvent::FinishedHeight(comitted_chain.tip().number))?;
            }

            if let Some(reverted_chain) = notification.reverted_chain() {
                info!("Chain reverted");

                let events = parse_chain_into_events(&reverted_chain);
                for (block, tx, event) in events {
                    let bridge_event = to_bridge_event(block, tx, event)?;
                    self.db.delete_event(bridge_event)?;

                    info!("Deleted reverted events from db");
                }
            }

            // Reorg case is handled by above. The reorged chain will be a reverted notification
            // and new chain will be a committed notification.
        }
        Ok(())
    }
}

fn to_bridge_event(
    block: &SealedBlockWithSenders,
    tx: &TransactionSigned,
    event: RootERC20BridgeEvents,
) -> eyre::Result<BridgeEvent> {
    match event {
        RootERC20BridgeEvents::RootChainETHWithdraw(e) => Ok(BridgeEvent {
            source: EventSource::Child,
            block_number: block.number,
            tx_hash: tx.hash,
            root_token: e.rootToken,
            child_token: e.childToken,
            from: e.withdrawer,
            to: e.receiver,
            amount: e.amount,
        }),
        RootERC20BridgeEvents::NativeEthDeposit(e) => Ok(BridgeEvent {
            source: EventSource::Root,
            block_number: block.number,
            tx_hash: tx.hash,
            root_token: e.rootToken,
            child_token: e.childToken,
            from: e.depositor,
            to: e.receiver,
            amount: e.amount,
        }),
        _ => Err(eyre!("Unkown bridge event")),
    }
}

fn parse_chain_into_events(
    chain: &Chain,
) -> Vec<(
    &SealedBlockWithSenders,
    &TransactionSigned,
    RootERC20BridgeEvents,
)> {
    chain
        // Get blocks and receipts from the chain Iterator<Block, Vec<Receipts>>
        .blocks_and_receipts()
        // Flatten vector to produce (block, receipt) pairs
        .flat_map(|(block, receipts)| {
            block
                .body
                // Itterator over txs
                .iter()
                // Zip up tx's and receipts
                .zip(receipts.iter().flatten())
                // Move the zipped itterator into tuple with blocck
                .map(move |(tx, receipt)| (block, tx, receipt))
            // [(block1, tx1, receipt1), (block1, tx2, receipt2), ..., (blockN, txM, receiptM)]
        })
        // Filter to the bridge
        .filter(|(_, tx, _)| tx.to() == Some(IMMUTABLE_BRIDGE))
        // Flat map the logs from the receipts
        // [(block1, tx1, log1), (block1, tx1, log2), ..., (blockX, txY, logZ)]
        .flat_map(|(block, tx, receipt)| receipt.logs.iter().map(move |log| (block, tx, log)))
        // Filter map to only include the logs which decode to a RootERC20Bridge events
        .filter_map(|(block, tx, log)| {
            RootERC20BridgeEvents::decode_raw_log(log.topics(), &log.data.data, true)
                // Convert result to option
                .ok()
                .map(move |event| (block, tx, event))
        })
        // Collect into Vec
        .collect()
}

fn main() -> eyre::Result<()> {
    reth::cli::Cli::parse_args().run(|builder, _| async move {
        let handle = builder
            .node(EthereumNode::default())
            .install_exex("ImmutableBridgeReader", |ctx| async move {
                let connection = Connection::open(DB_PATH)?;
                ImmutableBridgeReader::init(ctx, connection).await
            })
            .launch()
            .await?;

        handle.wait_for_node_exit().await
    })
}

#[cfg(test)]
mod tests {
    use alloy_sol_types::SolEvent;
    use eyre::eyre;
    use reth::revm::db::BundleState;
    use reth_execution_types::{Chain, ExecutionOutcome};
    use reth_exex_test_utils::{test_exex_context, PollOnce};
    use reth_primitives::{
        Address, Block, Header, Log, Receipt, Transaction, TransactionSigned, TxEip1559, TxKind,
        TxType, U256,
    };
    use reth_testing_utils::generators::sign_tx_with_random_key_pair;
    use std::pin::pin;

    use super::*;

    fn construct_tx_and_receipt<E: SolEvent>(
        to: Address,
        event: E,
    ) -> eyre::Result<(TransactionSigned, Receipt)> {
        // Construct dynamic tx, specificy tx kind call (transfer or contract call)
        let tx: Transaction = Transaction::Eip1559(TxEip1559 {
            to: TxKind::Call(to),
            ..Default::default()
        });

        // Construct log, injecting event data
        let log = Log::new(
            to,
            event
                .encode_topics()
                .into_iter()
                .map(|topic| topic.0)
                .collect(),
            event.encode_data().into(),
        )
        .ok_or_else(|| eyre::eyre!("failed to construct topics from event"))?;
        #[allow(clippy::needless_update)] // side-effect of optimism fields

        // Construct receipt for tx containing log
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

    fn construct_events() -> (
        Address,
        Address,
        Address,
        Address,
        RootERC20Bridge::NativeEthDeposit,
        RootERC20Bridge::RootChainETHWithdraw,
    ) {
        // Generate random addresses for event
        let from_address = Address::random();
        let to_address = Address::random();
        let root_token = Address::random();
        let child_token = Address::random();

        // Construct deposit event
        let deposit_event = RootERC20Bridge::NativeEthDeposit {
            rootToken: root_token,
            childToken: child_token,
            depositor: from_address,
            receiver: to_address,
            amount: U256::from(10),
        };

        // Construct withdrawal event
        let withdrawal_event = RootERC20Bridge::RootChainETHWithdraw {
            rootToken: root_token,
            childToken: child_token,
            withdrawer: from_address,
            receiver: to_address,
            amount: U256::from(10),
        };

        (
            from_address,
            to_address,
            root_token,
            child_token,
            deposit_event,
            withdrawal_event,
        )
    }

    #[tokio::test]
    async fn test_exec() -> eyre::Result<()> {
        // Create test exex
        let (ctx, handle) = test_exex_context().await?;

        // Create tmp file for db
        let db_file = tempfile::NamedTempFile::new()?;

        // Initialize the ExEx
        let mut exex = pin!(ImmutableBridgeReader::init(
            ctx,
            Connection::open(&db_file).unwrap()
        ).await?);

        // Construct events
        let (from_address, to_address, root_token, child_token, deposit_event, withdrawal_event) =
            construct_events();

        // Deposit log
        let (deposit_tx, deposit_tx_receipt) =
            construct_tx_and_receipt(IMMUTABLE_BRIDGE, deposit_event.clone())?;

        // Withdraw log
        let (withdrawal_tx, withdrawal_tx_receipt) =
            construct_tx_and_receipt(IMMUTABLE_BRIDGE, withdrawal_event.clone())?;

        // Construct a block
        let block = Block {
            header: Header::default(),
            body: vec![deposit_tx, withdrawal_tx],
            ..Default::default()
        }
        .seal_slow()
        .seal_with_senders()
        .ok_or_else(|| eyre!("failed to recover senders"))?;

        // Construct a chain, that holds the post execution state as a result of the above two events
        let chain = Chain::new(
            vec![block.clone()],
            ExecutionOutcome::new(
                BundleState::default(),
                vec![deposit_tx_receipt, withdrawal_tx_receipt].into(),
                block.number,
                vec![block.requests.clone().unwrap_or_default()],
            ),
            None,
        );

        // Notify exex
        handle
            .send_notification_chain_committed(chain.clone())
            .await
            .unwrap();

        // Poll the exex to consume notification
        exex.poll_once().await.unwrap();

        let connection = Connection::open(&db_file)?;

        // Assert deposit
        let deposits: Vec<(u64, String, String, String, String, String, String)> = connection
              .prepare(r#"SELECT block_number, tx_hash, root_token, child_token, "from", "to", amount FROM deposits"#)?
              .query_map([], |row| {
                  Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?))
              })?
              .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(deposits.len(), 1);
        assert_eq!(
            deposits[0],
            (
                block.number,
                block.body[0].hash.to_string(),
                root_token.to_string(),
                child_token.to_string(),
                from_address.to_string(),
                to_address.to_string(),
                deposit_event.amount.to_string(),
            )
        );

        // Assert withdrawal
        let withdrawals: Vec<(u64, String, String, String, String, String, String)> = connection
        .prepare(r#"SELECT block_number, tx_hash, root_token, child_token, "from", "to", amount FROM withdrawals"#)?
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(withdrawals.len(), 1);
        assert_eq!(
            withdrawals[0],
            (
                block.number,
                block.body[1].hash.to_string(),
                root_token.to_string(),
                child_token.to_string(),
                from_address.to_string(),
                to_address.to_string(),
                withdrawal_event.amount.to_string(),
            )
        );

        // Trigger chain revert
        handle.send_notification_chain_reverted(chain).await?;

        // Poll
        exex.poll_once().await?;

        // Assert that the previous events are deleted
        let withdrawals = connection
        .prepare(r#"SELECT block_number, tx_hash, root_token, child_token, "from", "to", amount FROM withdrawals"#)?
        .query_map([], |_| {
            Ok(())
        })?
        .count();
        assert_eq!(withdrawals, 0);

        let deposits = connection
        .prepare(r#"SELECT block_number, tx_hash, root_token, child_token, "from", "to", amount FROM withdrawals"#)?
        .query_map([], |_| {
            Ok(())
        })?
        .count();
        assert_eq!(deposits, 0);

        Ok(())
    }
}
