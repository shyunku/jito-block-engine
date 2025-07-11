use async_trait::async_trait;
use tokio_stream::wrappers::BroadcastStream;
use futures_util::stream::TryStreamExt;
use tonic::{transport::Server, Request, Response, Status};
use std::pin::Pin;
use futures_util::stream::Stream;

use be_proto::auth::auth_service_server::{AuthService, AuthServiceServer};
use be_proto::block_engine::block_engine_validator_server::{
    BlockEngineValidator, BlockEngineValidatorServer,
};
use be_proto::relayer::relayer_server::{Relayer, RelayerServer};
use be_proto::block_engine::block_engine_relayer_server::{
    BlockEngineRelayer, BlockEngineRelayerServer,
};
// Explicitly import generated types
use be_proto::auth::{
    GenerateAuthChallengeRequest, GenerateAuthChallengeResponse,
    GenerateAuthTokensRequest, GenerateAuthTokensResponse,
    Token, RefreshAccessTokenRequest, RefreshAccessTokenResponse,
};
use be_proto::block_engine::{
    BlockBuilderFeeInfoRequest, BlockBuilderFeeInfoResponse,
    SubscribeBundlesRequest, SubscribeBundlesResponse,
    SubscribePacketsRequest, SubscribePacketsResponse,
    AccountsOfInterestRequest, AccountsOfInterestUpdate,
    ProgramsOfInterestRequest, ProgramsOfInterestUpdate,
    PacketBatchUpdate, StartExpiringPacketStreamResponse,
};
use be_proto::relayer::{
    GetTpuConfigsRequest, GetTpuConfigsResponse,
    SubscribePacketsRequest as RelayerSubscribePacketsRequest,
    SubscribePacketsResponse as RelayerSubscribePacketsResponse,
    subscribe_packets_response::Msg as RelayerSubscribePacketsResponseMsg,
};
use be_proto::bundle::BundleUuid;
use be_proto::shared::{Header, Socket};
use be_proto::packet::PacketBatch;

pub async fn run(addr: std::net::SocketAddr) -> anyhow::Result<()> {
    // 브로드캐스트 채널: stub 데이터
    let (bundle_tx, _) = tokio::sync::broadcast::channel(128);
    let (packet_tx, _) = tokio::sync::broadcast::channel(128);

    let auth = AuthSvc::default();
    let validator = ValidatorSvc::new(bundle_tx.clone(), packet_tx.clone());
    let relayer = RelayerSvc::default();
    let block_engine_relayer = BlockEngineRelayerSvc::default();

    tracing::info!("Serving gRPC on {addr}");
    Server::builder()
        .add_service(AuthServiceServer::new(auth))
        .add_service(BlockEngineValidatorServer::new(validator))
        .add_service(RelayerServer::new(relayer))
        .add_service(BlockEngineRelayerServer::new(block_engine_relayer))
        .serve(addr)
        .await?;

    Ok(())
}

// -------------------- Auth --------------------

#[derive(Default)]
struct AuthSvc;

#[async_trait]
impl AuthService for AuthSvc {
    async fn generate_auth_challenge(
        &self,
        _req: Request<GenerateAuthChallengeRequest>,
    ) -> Result<Response<GenerateAuthChallengeResponse>, Status> {
        use base64::{engine::general_purpose, Engine as _};
// 32-byte 랜덤 challenge
        let challenge = rand::random::<[u8; 32]>().to_vec();
        // Base64 encode the challenge
        let challenge_str = general_purpose::STANDARD_NO_PAD.encode(challenge);
        Ok(Response::new(GenerateAuthChallengeResponse { challenge: challenge_str }))
    }

    async fn generate_auth_tokens(
        &self,
        _req: Request<GenerateAuthTokensRequest>,
    ) -> Result<Response<GenerateAuthTokensResponse>, Status> {
        // TODO: Implement actual token generation logic
        let now = std::time::SystemTime::now();
        let access_token_expires_at = now + std::time::Duration::from_secs(600);
        let refresh_token_expires_at = now + std::time::Duration::from_secs(3600);

        let access_token = Token {
            value: "demo_access_token".into(),
            expires_at_utc: Some(prost_types::Timestamp::from(access_token_expires_at)),
        };
        let refresh_token = Token {
            value: "demo_refresh_token".into(),
            expires_at_utc: Some(prost_types::Timestamp::from(refresh_token_expires_at)),
        };

        Ok(Response::new(GenerateAuthTokensResponse {
            access_token: Some(access_token),
            refresh_token: Some(refresh_token),
        }))
    }

    async fn refresh_access_token(
        &self,
        _req: Request<RefreshAccessTokenRequest>,
    ) -> Result<Response<RefreshAccessTokenResponse>, Status> {
        // TODO: Implement actual token refresh logic
        let now = std::time::SystemTime::now();
        let access_token_expires_at = now + std::time::Duration::from_secs(600);

        let access_token = Token {
            value: "refreshed_access_token".into(),
            expires_at_utc: Some(prost_types::Timestamp::from(access_token_expires_at)),
        };

        Ok(Response::new(RefreshAccessTokenResponse {
            access_token: Some(access_token),
        }))
    }
}

// ---------------- Validator -------------------

struct ValidatorSvc {
    bundle_tx: tokio::sync::broadcast::Sender<BundleUuid>,
    packet_tx: tokio::sync::broadcast::Sender<SubscribePacketsResponse>,
}

impl ValidatorSvc {
    fn new(
        bundle_tx: tokio::sync::broadcast::Sender<BundleUuid>,
        packet_tx: tokio::sync::broadcast::Sender<SubscribePacketsResponse>,
    ) -> Self {
        Self { bundle_tx, packet_tx }
    }
}



#[async_trait]
impl BlockEngineValidator for ValidatorSvc {
    async fn get_block_builder_fee_info(
        &self,
        _req: Request<BlockBuilderFeeInfoRequest>,
    ) -> Result<Response<BlockBuilderFeeInfoResponse>, Status> {
        Ok(Response::new(BlockBuilderFeeInfoResponse {
            pubkey: "".to_string(),
            commission: 0,
        }))
    }

    type SubscribeBundlesStream = Pin<Box<dyn Stream<Item = Result<SubscribeBundlesResponse, Status>> + Send>>;
    async fn subscribe_bundles(
        &self,
        _req: Request<SubscribeBundlesRequest>,
    ) -> Result<Response<Self::SubscribeBundlesStream>, Status> {
        let stream = BroadcastStream::new(self.bundle_tx.subscribe())
            .map_ok(|uuid| SubscribeBundlesResponse {
                bundles: vec![uuid],
            })
            .map_err(|e| Status::internal(format!("Bundle stream error: {}", e)));
        Ok(Response::new(Box::pin(stream)))
    }

    type SubscribePacketsStream = Pin<Box<dyn Stream<Item = Result<SubscribePacketsResponse, Status>> + Send>>;
    async fn subscribe_packets(
        &self,
        _req: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        let stream = BroadcastStream::new(self.packet_tx.subscribe())
            .map_err(|e| Status::internal(format!("Packet stream error: {}", e)));
        Ok(Response::new(Box::pin(stream)))
    }
}

// -------------------- Relayer --------------------

#[derive(Default)]
struct RelayerSvc;

impl RelayerSvc {}

#[async_trait]
impl Relayer for RelayerSvc {
    async fn get_tpu_configs(
        &self,
        _req: Request<GetTpuConfigsRequest>,
    ) -> Result<Response<GetTpuConfigsResponse>, Status> {
        Ok(Response::new(GetTpuConfigsResponse {
            tpu: Some(Socket {
                ip: "127.0.0.1".to_string(),
                port: 8000,
            }),
            tpu_forward: Some(Socket {
                ip: "127.0.0.1".to_string(),
                port: 8001,
            }),
        }))
    }

    type SubscribePacketsStream = Pin<Box<dyn Stream<Item = Result<RelayerSubscribePacketsResponse, Status>> + Send>>;
    async fn subscribe_packets(
        &self,
        _req: Request<RelayerSubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        // TODO: Implement actual packet subscription logic for relayer
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        tokio::spawn(async move {
            // Example: send a dummy packet every second
            for _i in 0..5 {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                let packet_batch = PacketBatch {
                    packets: vec![], // Fill with actual packets
                };
                let header = Header {
                    ts: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                };
                let response = RelayerSubscribePacketsResponse {
                    header: Some(header),
                    msg: Some(RelayerSubscribePacketsResponseMsg::Batch(packet_batch)),
                };
                if tx.send(Ok(response)).await.is_err() {
                    break;
                }
            }
        });
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }
}

// -------------------- BlockEngineRelayer --------------------

#[derive(Default)]
struct BlockEngineRelayerSvc;

#[async_trait]
impl BlockEngineRelayer for BlockEngineRelayerSvc {
    async fn subscribe_accounts_of_interest(
        &self,
        _req: Request<AccountsOfInterestRequest>,
    ) -> Result<Response<Self::SubscribeAccountsOfInterestStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            // Send a dummy update every 5 seconds
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                let update = AccountsOfInterestUpdate {
                    accounts: vec!["dummy_account_1".to_string(), "dummy_account_2".to_string()],
                };
                if tx.send(Ok(update)).await.is_err() {
                    break;
                }
            }
        });
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    type SubscribeAccountsOfInterestStream = Pin<Box<dyn Stream<Item = Result<AccountsOfInterestUpdate, Status>> + Send>>;

    async fn subscribe_programs_of_interest(
        &self,
        _req: Request<ProgramsOfInterestRequest>,
    ) -> Result<Response<Self::SubscribeProgramsOfInterestStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            // Send a dummy update every 5 seconds
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                let update = ProgramsOfInterestUpdate {
                    programs: vec!["dummy_program_1".to_string(), "dummy_program_2".to_string()],
                };
                if tx.send(Ok(update)).await.is_err() {
                    break;
                }
            }
        });
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    type SubscribeProgramsOfInterestStream = Pin<Box<dyn Stream<Item = Result<ProgramsOfInterestUpdate, Status>> + Send>>;

    async fn start_expiring_packet_stream(
        &self,
        mut _req: Request<tonic::Streaming<PacketBatchUpdate>>,
    ) -> Result<Response<Self::StartExpiringPacketStreamStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            // Consume incoming messages (PacketBatchUpdate)
            while let Some(msg) = _req.get_mut().message().await.unwrap() {
                // Process incoming messages if needed
                tracing::info!("Received PacketBatchUpdate: {:?}", msg);
            }
        });
        tokio::spawn(async move {
            // Send a dummy heartbeat every 1 second
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                let heartbeat = StartExpiringPacketStreamResponse {
                    heartbeat: Some(be_proto::shared::Heartbeat {
                        count: 0, // Add the count field
                        ts: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                    }),
                };
                if tx.send(Ok(heartbeat)).await.is_err() {
                    break;
                }
            }
        });
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    type StartExpiringPacketStreamStream = Pin<Box<dyn Stream<Item = Result<StartExpiringPacketStreamResponse, Status>> + Send>>;
}


