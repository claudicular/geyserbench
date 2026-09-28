use std::{collections::HashMap, error::Error, sync::atomic::Ordering};

use futures_util::{sink::SinkExt, stream::StreamExt};
use tokio::task;
use tonic::transport::ClientTlsConfig;
use tracing::{Level, error, info, warn};

use crate::proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactionAccounts,
    SubscribeRequestPing, subscribe_update::UpdateOneof,
};

use crate::{
    config::{Config, Endpoint},
    utils::{TransactionData, get_current_timestamp, open_log_file, write_log_entry},
};

use super::{
    GeyserProvider, ProviderContext,
    common::{TransactionAccumulator, fatal_connection_error},
    yellowstone_client::GeyserGrpcClient,
};

pub struct YellowstoneTxAccountsProvider;

impl GeyserProvider for YellowstoneTxAccountsProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move {
            process_yellowstone_tx_accounts_endpoint(endpoint, config, context).await
        })
    }
}

/// Subscribes to grouped `transaction_accounts` updates for transactions that
/// touch an account owned by `owner`. The fork plugin carries this on protobuf
/// field 100 (see `proto/geyser.proto`).
fn tx_accounts_subscribe_request(owner: String, commitment: CommitmentLevel) -> SubscribeRequest {
    let mut transaction_accounts = HashMap::new();
    transaction_accounts.insert(
        "account".to_string(),
        SubscribeRequestFilterTransactionAccounts {
            owner: vec![owner],
            account: vec![],
            // Owner-matched accounts only, as the bot requests; all-accounts messages carry multi-MB non-matching accounts that inflate latency.
            include_all_accounts: None,
            readonly_mints_only: Some(false),
        },
    );

    SubscribeRequest {
        transaction_accounts,
        commitment: Some(commitment as i32),
        ..Default::default()
    }
}

async fn process_yellowstone_tx_accounts_endpoint(
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

    let target_owner = config.account.clone();
    let endpoint_name = endpoint.name.clone();
    let mut log_file = if tracing::enabled!(Level::TRACE) {
        Some(open_log_file(&endpoint_name)?)
    } else {
        None
    };

    let endpoint_url = endpoint.url.clone();
    let endpoint_token = endpoint
        .x_token
        .clone()
        .filter(|token| !token.trim().is_empty());

    info!(endpoint = %endpoint_name, url = %endpoint_url, "Connecting");

    let builder = GeyserGrpcClient::build_from_shared(endpoint_url.clone())
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    let builder = if let Some(token) = endpoint_token {
        builder
            .x_token(Some(token))
            .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err))
    } else {
        builder
    };
    let builder = builder
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    let mut client = builder
        .connect()
        .await
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));

    info!(endpoint = %endpoint_name, "Connected");

    let (mut subscribe_tx, mut stream) = client.subscribe().await?;
    let commitment: CommitmentLevel = config.commitment.into();

    subscribe_tx
        .send(tx_accounts_subscribe_request(target_owner, commitment))
        .await?;

    let mut accumulator = TransactionAccumulator::new();
    let mut transaction_count = 0usize;

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                break;
            }

            message = stream.next() => {
                match message {
                    Some(Ok(msg)) => {
                        match msg.update_oneof {
                            Some(UpdateOneof::TransactionAccounts(tx_msg)) => {
                                let wallclock = get_current_timestamp();
                                let elapsed = start_instant.elapsed();
                                let signature = bs58::encode(tx_msg.signature).into_string();

                                if signature.is_empty() {
                                    warn!(endpoint = %endpoint_name, "Missing signature in transaction_accounts update");
                                    continue;
                                }

                                if let Some(file) = log_file.as_mut() {
                                    write_log_entry(file, wallclock, &endpoint_name, &signature)?;
                                }

                                let tx_data = TransactionData {
                                    wallclock_secs: wallclock,
                                    elapsed_since_start: elapsed,
                                    start_wallclock_secs,
                                    slot: Some(tx_msg.slot),
                                };

                                let updated = accumulator.record(signature.clone(), tx_data.clone());

                                if updated
                                    && comparator.record_observation(&endpoint_name, &signature, tx_data, total_producers) {
                                        if let Some(target) = target_transactions {
                                            let shared = shared_counter.fetch_add(1, Ordering::AcqRel) + 1;
                                            if let Some(tracker) = progress.as_ref() {
                                                tracker.record(shared);
                                            }
                                            if shared >= target
                                                && !shared_shutdown.swap(true, Ordering::AcqRel)
                                            {
                                                info!(endpoint = %endpoint_name, target, "Reached shared signature target; broadcasting shutdown");
                                                let _ = shutdown_tx.send(());
                                            }
                                        }
                                    }

                                transaction_count += 1;
                            }
                            Some(UpdateOneof::Ping(_)) => {
                                subscribe_tx
                                    .send(SubscribeRequest {
                                        ping: Some(SubscribeRequestPing { id: 1 }),
                                        ..Default::default()
                                    })
                                    .await?;
                            }
                            _ => {}
                        }
                    }
                    Some(Err(e)) => {
                        error!(endpoint = %endpoint_name, error = ?e, "Error receiving message from stream");
                        break;
                    }
                    None => {
                        info!(endpoint = %endpoint_name, "Stream closed by server");
                        break;
                    }
                }
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
    use std::collections::HashMap;

    use prost::Message;

    use super::tx_accounts_subscribe_request;
    use crate::proto::geyser::{
        CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactionAccounts,
        SubscribeUpdate, SubscribeUpdateTransactionAccounts, subscribe_update::UpdateOneof,
    };

    /// Field 100, wire type 2 (length-delimited): varint(100 << 3 | 2) = varint(802).
    const FIELD_100_TAG: [u8; 2] = [0xA2, 0x06];

    /// Top-level field numbers of an encoded protobuf message.
    fn top_level_fields(mut bytes: &[u8]) -> Vec<u64> {
        fn varint(bytes: &mut &[u8]) -> u64 {
            let (mut value, mut shift) = (0u64, 0);
            loop {
                let byte = bytes[0];
                *bytes = &bytes[1..];
                value |= u64::from(byte & 0x7f) << shift;
                if byte & 0x80 == 0 {
                    return value;
                }
                shift += 7;
            }
        }
        let mut fields = Vec::new();
        while !bytes.is_empty() {
            let key = varint(&mut bytes);
            fields.push(key >> 3);
            match key & 7 {
                0 => {
                    varint(&mut bytes);
                }
                1 => bytes = &bytes[8..],
                2 => {
                    let len = varint(&mut bytes) as usize;
                    bytes = &bytes[len..];
                }
                5 => bytes = &bytes[4..],
                wire => panic!("unexpected wire type {wire}"),
            }
        }
        fields
    }

    /// The agave 4.3.0 fork plugin (add-transaction-accounts-sub-v13) carries
    /// `transaction_accounts` on field 100; field 12 is upstream `block_footer`
    /// there, so a field-12 subscription matches nothing.
    #[test]
    fn transaction_accounts_request_uses_field_100() {
        let mut transaction_accounts = HashMap::new();
        transaction_accounts.insert(
            "account".to_string(),
            SubscribeRequestFilterTransactionAccounts {
                owner: vec!["pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA".to_string()],
                ..Default::default()
            },
        );
        let request = SubscribeRequest {
            transaction_accounts,
            ..Default::default()
        };
        let bytes = request.encode_to_vec();
        assert_eq!(bytes[..2], FIELD_100_TAG, "{bytes:02x?}");
        assert_eq!(top_level_fields(&bytes), vec![100]);

        let provider_request = tx_accounts_subscribe_request(
            "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA".to_string(),
            CommitmentLevel::Processed,
        );
        let fields = top_level_fields(&provider_request.encode_to_vec());
        assert!(fields.contains(&100) && !fields.contains(&12), "{fields:?}");
        assert!(
            provider_request
                .transaction_accounts
                .values()
                .all(|filter| filter.include_all_accounts.is_none())
        );
    }

    #[test]
    fn transaction_accounts_update_uses_field_100() {
        let update = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::TransactionAccounts(
                SubscribeUpdateTransactionAccounts {
                    signature: vec![7; 64],
                    slot: 42,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let bytes = update.encode_to_vec();
        assert_eq!(bytes[..2], FIELD_100_TAG, "{bytes:02x?}");
        assert_eq!(top_level_fields(&bytes), vec![100]);

        let decoded = SubscribeUpdate::decode(bytes.as_slice()).unwrap();
        assert!(matches!(
            decoded.update_oneof,
            Some(UpdateOneof::TransactionAccounts(ref tx)) if tx.slot == 42
        ));
    }
}
