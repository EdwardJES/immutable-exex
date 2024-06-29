use std::sync::{Arc, Mutex, MutexGuard};

use alloy_sol_types::{sol, SolEventInterface};
use futures::Future;
use reth::blockchain_tree::chain;
use reth_exex::{ExExContext, ExExNotification};
use reth_node_api::FullNodeComponents;
use reth_primitives::{address, revm_primitives::FixedBytes, ruint::Uint, Address, SealedBlockWithSenders, TransactionSigned};
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
            EventSource::Child => {
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
    fn init(
        ctx: ExExContext<Node>,
        connection: Connection,
    ) -> eyre::Result<impl Future<Output = eyre::Result<()>>> {
        let db = Database::new(connection)?;
        Ok(Self { ctx, db }.run())
    }

    async fn run(mut self) -> eyre::Result<()> {
        while let Some(notification) = self.ctx.notifications.recv().await {
            match &notification {
                ExExNotification::ChainCommitted { new } => {
                    // do something
                    info!("Chain committed");
                    println!("Chain Committed")
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

fn decode_chain_into_events(
    chain: &Chain,
) -> Vec<(
    &SealedBlockWithSenders,
    &TransactionSigned,
    RootERC20BridgeEvents,
)> {
    chain
    // Step 1: Get all blocks and receipts from the chain
    .blocks_and_receipts()
    // Step 2: For each (block, receipts) pair, flatten the receipts and pair each transaction with its corresponding receipt
    .flat_map(|(block, receipts)| {
        block
            .body
            .iter()                  // Iterate over all transactions in the block
            .zip(receipts.iter().flatten())  // Pair each transaction with its corresponding receipt
            .map(move |(tx, receipt)| (block, tx, receipt))  // Create a tuple (block, tx, receipt) for each pair
    })
    // [(block1, tx1, receipt1), (block1, tx2, receipt2), ..., (blockN, txM, receiptM)]
    // Step 3: Filter transactions that are directed to the rollup contract
    .filter(|(_, tx, _)| tx.to() == Some(IMMUTABLE_BRIDGE))
    // [(block1, tx1, receipt1), ..., (blockX, txY, receiptY)] (only transactions to IMMUTABLE_BRIDGE)
    // Step 4: For each filtered tuple, iterate over the logs in the receipt and create a tuple (block, tx, log) for each log
    .flat_map(|(block, tx, receipt)| receipt.logs.iter().map(move |log| (block, tx, log)))
    // [(block1, tx1, log1), (block1, tx1, log2), ..., (blockX, txY, logZ)]
    // Step 5: Decode each log into a RootERC20BridgeEvent and filter out logs that do not decode
    .filter_map(|(block, tx, log)| {
        RootERC20BridgeEvents::decode_raw_log(log.topics(), &log.data.data, true)
            .ok()
            .map(|event| (block, tx, event))  // Create a tuple (block, tx, event) for successfully decoded logs
    })
    // [(block1, tx1, event1), (block1, tx1, event2), ..., (blockX, txY, eventN)]
    // Step 6: Collect all the tuples into a vector
    .collect()
    // Final Output: Vec of tuples (block, transaction, decoded event)
}
fn main() -> eyre::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy_sol_types::{abi::Token, SolEvent};
    use eyre::ErrReport;
    use futures::Future;
    use reth::revm::db::BundleState;
    use reth_execution_types::{Chain, ExecutionOutcome};
    use reth_exex_test_utils::{test_exex_context, PollOnce, TestExExHandle};
    use reth_primitives::{
        Address, Block, Header, Log, Receipt, Transaction, TransactionSigned, TxEip1559, TxKind,
        TxType, U256,
    };
    use reth_testing_utils::generators::sign_tx_with_random_key_pair;
    use std::pin::{pin, Pin};
    use RootERC20Bridge::RootERC20BridgeEvents;

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

    #[tokio::test]
    async fn test_exec() {
        // Create test exex
        let (ctx, handle) = test_exex_context().await.unwrap();

        // Create tmp file for db
        let db_file = tempfile::NamedTempFile::new().unwrap();

        // Initialize the ExEx
        let mut exex =
            pin!(ImmutableBridgeReader::init(ctx, Connection::open(&db_file).unwrap(),).unwrap());

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

        // Construct tx and receipt
        let (deposit_tx, deposit_tx_receipt) =
            construct_tx_and_receipt(IMMUTABLE_BRIDGE, deposit_event.clone()).unwrap();

        // Construct withdrawal event
        let withdrawal_event = RootERC20Bridge::RootChainETHWithdraw {
            rootToken: root_token,
            childToken: child_token,
            withdrawer: from_address,
            receiver: to_address,
            amount: U256::from(10),
        };

        // ...
        let (withdrawal_tx, withdrawal_tx_receipt) =
            construct_tx_and_receipt(IMMUTABLE_BRIDGE, withdrawal_event.clone()).unwrap();

        // Construct a block
        let block = Block {
            header: Header::default(),
            body: vec![deposit_tx, withdrawal_tx],
            ..Default::default()
        }
        .seal_slow()
        .seal_with_senders()
        .unwrap();

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
    }
}
