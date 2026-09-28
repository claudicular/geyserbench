use {
    bytes::Bytes,
    futures::{
        channel::mpsc,
        sink::{Sink, SinkExt},
        stream::Stream,
    },
    std::convert::TryInto,
    tonic::{
        Request, Response, Status,
        codec::Streaming,
        metadata::{AsciiMetadataValue, errors::InvalidMetadataValue},
        service::interceptor::InterceptedService,
        transport::{ClientTlsConfig, Endpoint, channel::Channel},
    },
};

use super::common::GRPC_MAX_MESSAGE_SIZE;
use crate::proto::geyser::{
    SubscribeDeshredRequest, SubscribeRequest, SubscribeUpdate, SubscribeUpdateDeshred,
    geyser_client::GeyserClient,
};

#[derive(Clone, Debug)]
pub struct InterceptorXToken {
    pub x_token: Option<AsciiMetadataValue>,
}

impl tonic::service::Interceptor for InterceptorXToken {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(token) = self.x_token.clone() {
            request.metadata_mut().insert("x-token", token);
        }
        Ok(request)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GeyserGrpcClientError {
    #[error("gRPC status: {0}")]
    TonicStatus(#[from] Status),
    #[error("Failed to send subscribe request: {0}")]
    SubscribeSendError(#[from] mpsc::SendError),
}

pub type GeyserGrpcClientResult<T> = Result<T, GeyserGrpcClientError>;

pub struct GeyserGrpcClient {
    geyser: GeyserClient<InterceptedService<Channel, InterceptorXToken>>,
}

impl GeyserGrpcClient {
    pub fn build_from_shared(
        endpoint: impl Into<Bytes>,
    ) -> GeyserGrpcBuilderResult<GeyserGrpcBuilder> {
        Ok(GeyserGrpcBuilder::new(Endpoint::from_shared(endpoint)?))
    }

    pub async fn subscribe(
        &mut self,
    ) -> GeyserGrpcClientResult<(
        impl Sink<SubscribeRequest, Error = mpsc::SendError>,
        impl Stream<Item = Result<SubscribeUpdate, Status>>,
    )> {
        self.subscribe_with_request(None).await
    }

    pub async fn subscribe_with_request(
        &mut self,
        request: Option<SubscribeRequest>,
    ) -> GeyserGrpcClientResult<(
        impl Sink<SubscribeRequest, Error = mpsc::SendError>,
        impl Stream<Item = Result<SubscribeUpdate, Status>>,
    )> {
        let (mut subscribe_tx, subscribe_rx) = mpsc::unbounded();
        if let Some(request) = request {
            subscribe_tx
                .send(request)
                .await
                .map_err(GeyserGrpcClientError::SubscribeSendError)?;
        }
        let response: Response<Streaming<SubscribeUpdate>> =
            self.geyser.subscribe(subscribe_rx).await?;
        Ok((subscribe_tx, response.into_inner()))
    }

    /// Opens the `SubscribeDeshred` stream (pre-execution transactions),
    /// queueing `request` as the first stream message. Message-size limits come from
    /// the shared client built in `GeyserGrpcBuilder::build`.
    pub async fn subscribe_deshred(
        &mut self,
        request: SubscribeDeshredRequest,
    ) -> GeyserGrpcClientResult<(
        impl Sink<SubscribeDeshredRequest, Error = mpsc::SendError>,
        impl Stream<Item = Result<SubscribeUpdateDeshred, Status>>,
    )> {
        let (mut subscribe_tx, subscribe_rx) = mpsc::unbounded();
        subscribe_tx
            .send(request)
            .await
            .map_err(GeyserGrpcClientError::SubscribeSendError)?;
        let response: Response<Streaming<SubscribeUpdateDeshred>> =
            self.geyser.subscribe_deshred(subscribe_rx).await?;
        Ok((subscribe_tx, response.into_inner()))
    }

    fn new(geyser: GeyserClient<InterceptedService<Channel, InterceptorXToken>>) -> Self {
        Self { geyser }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GeyserGrpcBuilderError {
    #[error("Failed to parse x-token: {0}")]
    MetadataValueError(#[from] InvalidMetadataValue),
    #[error("gRPC transport error: {0}")]
    TonicError(#[from] tonic::transport::Error),
}

pub type GeyserGrpcBuilderResult<T> = Result<T, GeyserGrpcBuilderError>;

pub struct GeyserGrpcBuilder {
    endpoint: Endpoint,
    x_token: Option<AsciiMetadataValue>,
}

impl GeyserGrpcBuilder {
    fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            x_token: None,
        }
    }

    pub async fn connect(self) -> GeyserGrpcBuilderResult<GeyserGrpcClient> {
        let channel = self.endpoint.connect().await?;
        self.build(channel)
    }

    fn build(self, channel: Channel) -> GeyserGrpcBuilderResult<GeyserGrpcClient> {
        let interceptor = InterceptorXToken {
            x_token: self.x_token,
        };
        let geyser = GeyserClient::with_interceptor(channel, interceptor)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE);
        Ok(GeyserGrpcClient::new(geyser))
    }

    pub fn x_token<T>(mut self, x_token: Option<T>) -> GeyserGrpcBuilderResult<Self>
    where
        T: TryInto<AsciiMetadataValue, Error = InvalidMetadataValue>,
    {
        self.x_token = x_token.map(|value| value.try_into()).transpose()?;
        Ok(self)
    }

    pub fn tls_config(mut self, tls_config: ClientTlsConfig) -> GeyserGrpcBuilderResult<Self> {
        self.endpoint = self.endpoint.tls_config(tls_config)?;
        Ok(self)
    }
}
