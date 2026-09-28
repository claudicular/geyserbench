use std::{collections::HashMap, error::Error, sync::atomic::Ordering};

use futures_util::{sink::SinkExt, stream::StreamExt};
use tokio::task;
use tonic::transport::ClientTlsConfig;
use tracing::{Level, error, info, warn};

use crate::proto::geyser::{
    SubscribeDeshredRequest, SubscribeRequestFilterDeshredTransactions, SubscribeRequestPing,
    SubscribeUpdateDeshredTransaction, subscribe_update_deshred::UpdateOneof,
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

pub struct YellowstoneDeshredProvider;

impl GeyserProvider for YellowstoneDeshredProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(
            async move { process_yellowstone_deshred_endpoint(endpoint, config, context).await },
        )
    }
}

/// One non-vote filter on `account`. The fork plugin matches `account_include`
/// against static keys plus ALT addresses resolved on the rooted bank. Update-parent
/// markers and slot updates are not requested, so the stream carries only
/// transactions plus server pings.
fn deshred_subscribe_request(account: String) -> SubscribeDeshredRequest {
    let mut deshred_transactions = HashMap::new();
    deshred_transactions.insert(
        "account".to_string(),
        SubscribeRequestFilterDeshredTransactions {
            vote: Some(false),
            account_include: vec![account],
            account_exclude: vec![],
            account_required: vec![],
            include_update_parent: None,
        },
    );

    SubscribeDeshredRequest {
        deshred_transactions,
        ping: None,
        slots: HashMap::default(),
    }
}

/// First signature of a deshred transaction; the plugin fills `info.signature`
/// with it, the signed transaction is the fallback.
fn deshred_signature(update: &SubscribeUpdateDeshredTransaction) -> Option<String> {
    let info = update.transaction.as_ref()?;
    let signature = if info.signature.is_empty() {
        info.transaction.as_ref()?.signatures.first()?.as_slice()
    } else {
        info.signature.as_slice()
    };
    (!signature.is_empty()).then(|| bs58::encode(signature).into_string())
}

async fn process_yellowstone_deshred_endpoint(
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

    // Deshred is pre-execution and commitment-free: `config.commitment` does not apply.
    let (mut subscribe_tx, mut stream) = client
        .subscribe_deshred(deshred_subscribe_request(config.account.clone()))
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
                            Some(UpdateOneof::DeshredTransaction(tx_msg)) => {
                                let wallclock = get_current_timestamp();
                                let elapsed = start_instant.elapsed();

                                let Some(signature) = deshred_signature(&tx_msg) else {
                                    warn!(endpoint = %endpoint_name, "Missing signature in deshred transaction");
                                    continue;
                                };

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
                                // The server pings every 10s and does not require a reply; answering
                                // keeps the client half active through proxies. A ping-only request
                                // gets a pong and leaves the filter unchanged.
                                subscribe_tx
                                    .send(SubscribeDeshredRequest {
                                        ping: Some(SubscribeRequestPing { id: 1 }),
                                        ..Default::default()
                                    })
                                    .await?;
                            }
                            _ => {}
                        }
                    }
                    Some(Err(e)) => {
                        error!(endpoint = %endpoint_name, error = ?e, "Error receiving message from deshred stream");
                        break;
                    }
                    None => {
                        error!(endpoint = %endpoint_name, "Deshred stream closed by server");
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
    use prost::Message;

    use super::{deshred_signature, deshred_subscribe_request};
    use crate::proto::geyser::{
        SubscribeDeshredRequest, SubscribeUpdateDeshred, subscribe_update_deshred::UpdateOneof,
    };

    const ACCOUNT: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

    fn varint(value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        let mut value = value;
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                return out;
            }
            out.push(byte | 0x80);
        }
    }

    fn len_field(field: u64, payload: &[u8]) -> Vec<u8> {
        let mut out = varint(field << 3 | 2);
        out.extend(varint(payload.len() as u64));
        out.extend_from_slice(payload);
        out
    }

    #[test]
    fn deshred_request_is_one_non_vote_account_filter() {
        let request = deshred_subscribe_request(ACCOUNT.to_string());
        let bytes = request.encode_to_vec();
        // Only field 1 (deshred_transactions map): no ping (2), no slots (3).
        assert_eq!(bytes[0], 0x0A, "{bytes:02x?}");

        let decoded = SubscribeDeshredRequest::decode(bytes.as_slice()).unwrap();
        assert!(decoded.ping.is_none() && decoded.slots.is_empty());
        assert_eq!(decoded.deshred_transactions.len(), 1);
        let filter = &decoded.deshred_transactions["account"];
        assert_eq!(filter.vote, Some(false));
        assert_eq!(filter.account_include, vec![ACCOUNT.to_string()]);
        assert!(filter.account_exclude.is_empty() && filter.account_required.is_empty());
        assert_eq!(filter.include_update_parent, None);
    }

    /// Bytes laid out the way the fork plugin's hand-written `FilteredUpdateDeshred`
    /// encoder writes them: filters=1, deshred_transaction=2 {info=1 {signature=1}, slot=2},
    /// ping=3 (empty), created_at=5.
    #[test]
    fn decodes_plugin_encoded_deshred_transaction_and_ping() {
        let signature = [7u8; 64];
        let info = len_field(1, &signature);
        let mut tx = len_field(1, &info);
        tx.extend(varint(2 << 3));
        tx.extend(varint(123_456_789));
        let mut update = len_field(1, b"account");
        update.extend(len_field(2, &tx));
        update.extend(len_field(5, &[]));

        let decoded = SubscribeUpdateDeshred::decode(update.as_slice()).unwrap();
        assert_eq!(decoded.filters, vec!["account".to_string()]);
        let Some(UpdateOneof::DeshredTransaction(tx_msg)) = decoded.update_oneof else {
            panic!("expected deshred transaction");
        };
        assert_eq!(tx_msg.slot, 123_456_789);
        assert_eq!(
            deshred_signature(&tx_msg),
            Some(bs58::encode(signature).into_string())
        );

        let ping = SubscribeUpdateDeshred::decode([0x1Au8, 0x00].as_slice()).unwrap();
        assert!(matches!(ping.update_oneof, Some(UpdateOneof::Ping(_))));
    }

    /// The generated client must call `/geyser.Geyser/SubscribeDeshred` with the fork's types.
    #[test]
    fn service_declares_subscribe_deshred() {
        let descriptors = prost_types::FileDescriptorSet::decode(
            &include_bytes!(concat!(env!("OUT_DIR"), "/proto_descriptors.bin"))[..],
        )
        .unwrap();
        let method = descriptors
            .file
            .iter()
            .filter(|file| file.package() == "geyser")
            .flat_map(|file| &file.service)
            .filter(|service| service.name() == "Geyser")
            .flat_map(|service| &service.method)
            .find(|method| method.name() == "SubscribeDeshred")
            .expect("SubscribeDeshred rpc");
        assert_eq!(method.input_type(), ".geyser.SubscribeDeshredRequest");
        assert_eq!(method.output_type(), ".geyser.SubscribeUpdateDeshred");
        assert!(method.client_streaming() && method.server_streaming());
    }
}
