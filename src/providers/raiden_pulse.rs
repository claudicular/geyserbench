use futures::{SinkExt, channel::mpsc::unbounded};
use futures_util::stream::StreamExt;
use solana_pubkey::Pubkey;
use std::{collections::HashMap, error::Error, sync::atomic::Ordering};
use tokio::task;
use tracing::{Level, info, warn};

use crate::{
    config::{Config, Endpoint},
    utils::{TransactionData, get_current_timestamp, open_log_file, write_log_entry},
};

use super::{
    GeyserProvider, ProviderContext,
    common::{TransactionAccumulator, fatal_connection_error},
};

#[allow(clippy::all, dead_code)]
pub mod shreder_binary {
    include!(concat!(env!("OUT_DIR"), "/shreder_binary.rs"));
}

use shreder_binary::{
    SubscribeBinaryTransactionsRequest, SubscribeRequestFilterBinaryTransactions,
    shreder_binary_service_client::ShrederBinaryServiceClient,
};

pub struct RaidenPulseProvider;

fn subscription_request(account: String) -> SubscribeBinaryTransactionsRequest {
    let transactions = HashMap::from([(
        "account".to_string(),
        SubscribeRequestFilterBinaryTransactions {
            account_exclude: vec![],
            account_include: vec![],
            account_required: vec![account],
        },
    )]);
    SubscribeBinaryTransactionsRequest { transactions }
}

impl GeyserProvider for RaidenPulseProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move { process_raiden_pulse_endpoint(endpoint, config, context).await })
    }
}

async fn process_raiden_pulse_endpoint(
    endpoint: Endpoint,
    config: Config,
    context: ProviderContext,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let ProviderContext {
        shutdown_tx,
        mut shutdown_rx,
        start_wallclock_secs,
        start_instant,
        comparator,
        shared_counter,
        shared_shutdown,
        target_transactions,
        total_producers,
        progress,
    } = context;
    config.account.parse::<Pubkey>()?;
    let endpoint_name = endpoint.name.clone();

    let mut log_file = if tracing::enabled!(Level::TRACE) {
        Some(open_log_file(&endpoint_name)?)
    } else {
        None
    };

    let endpoint_url = endpoint.url.clone();

    info!(endpoint = %endpoint_name, url = %endpoint_url, "Connecting");

    let mut client = ShrederBinaryServiceClient::connect(endpoint_url.clone())
        .await
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    info!(endpoint = %endpoint_name, "Connected");

    let request = subscription_request(config.account);
    let (mut subscribe_tx, subscribe_rx) =
        unbounded::<shreder_binary::SubscribeBinaryTransactionsRequest>();
    subscribe_tx.send(request).await?;
    let mut stream = client
        .subscribe_binary_transactions(subscribe_rx)
        .await?
        .into_inner();

    let mut accumulator = TransactionAccumulator::new();
    let mut transaction_count = 0usize;

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                break;
            }

            message = stream.next() => {
                let msg = match message {
                    Some(Ok(msg)) => msg,
                    Some(Err(err)) => return Err(err.into()),
                    None => {
                        warn!(endpoint = %endpoint_name, "Transaction stream closed");
                        break;
                    }
                };
                let Some(tx_update) = msg.transaction.as_ref() else { continue };
                let Some(tx) = tx_update.transaction.as_ref() else { continue };
                let Some(signature_bytes) = tx.signatures.first() else { continue };

                let wallclock = get_current_timestamp();
                let elapsed = start_instant.elapsed();
                let signature = bs58::encode(signature_bytes).into_string();

                if let Some(file) = log_file.as_mut() {
                    write_log_entry(file, wallclock, &endpoint_name, &signature)?;
                }

                let tx_data = TransactionData {
                    wallclock_secs: wallclock,
                    elapsed_since_start: elapsed,
                    start_wallclock_secs,
                    slot: Some(tx_update.slot),
                };

                let updated = accumulator.record(signature.clone(), tx_data.clone());

                if updated
                    && comparator.record_observation(&endpoint_name, &signature, tx_data, total_producers)
                    && let Some(target) = target_transactions
                {
                    let shared = shared_counter.fetch_add(1, Ordering::AcqRel) + 1;
                    if let Some(tracker) = progress.as_ref() {
                        tracker.record(shared);
                    }
                    if shared >= target && !shared_shutdown.swap(true, Ordering::AcqRel) {
                        info!(endpoint = %endpoint_name, target, "Reached shared signature target; broadcasting shutdown");
                        let _ = shutdown_tx.send(());
                    }
                }

                transaction_count += 1;
            }
        }
    }

    let unique_signatures = accumulator.len();
    let collected = accumulator.into_inner();
    comparator.add_batch(&endpoint_name, collected);
    info!(
        endpoint = %endpoint_name,
        total_transactions = transaction_count,
        unique_signatures,
        "Stream closed after dispatching transactions"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::subscription_request;

    #[test]
    fn subscription_requires_the_configured_account() {
        let account = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
        let request = subscription_request(account.to_string());
        let filter = request.transactions.get("account").unwrap();

        assert_eq!(filter.account_required, [account]);
        assert!(filter.account_include.is_empty());
        assert!(filter.account_exclude.is_empty());
    }
}
