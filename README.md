# Immutable Exex

Reth [execution extensions](https://www.paradigm.xyz/2024/05/reth-exex) (ExEx) that listens to bridge events on the Immutable ERC20 bridge on Ethereum.
Currently this only listens to native eth deposits from the rootchain and eth withdrawals from the child chain.

## Running
TODO

```
cargo build --release
./target/release/immutable-exex node --http --http.api=debug,eth,reth --ws --full --chain=mainnet
```