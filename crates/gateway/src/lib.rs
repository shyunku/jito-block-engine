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
use be_proto::searcher::searcher_service_server::{SearcherService, SearcherServiceServer};
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
    AccountsOfInterestRequest, AccountsOfInterestUpdate,
    ProgramsOfInterestRequest, ProgramsOfInterestUpdate,
    PacketBatchUpdate, StartExpiringPacketStreamResponse,
    packet_batch_update::Msg as PacketBatchUpdateMsg,
};
use be_proto::relayer::{
    GetTpuConfigsRequest, GetTpuConfigsResponse,
    SubscribePacketsRequest as RelayerSubscribePacketsRequest,
    SubscribePacketsResponse as RelayerSubscribePacketsResponse,
    subscribe_packets_response::Msg as RelayerSubscribePacketsResponseMsg,
};
use be_proto::bundle::{BundleUuid, BundleResult};
use be_proto::shared::{Header, Socket};
use be_proto::packet::PacketBatch;

pub async fn run(addr: std::net::SocketAddr) -> anyhow::Result<()> {
    // 브로드캐스트 채널: stub 데이터
    let (bundle_tx, _) = tokio::sync::broadcast::channel(128);
    let (packet_tx, _) = tokio::sync::broadcast::channel(128);
    let (bundle_result_tx, _) = tokio::sync::broadcast::channel(128);

    let auth = AuthSvc::default();
    let validator = ValidatorSvc::new(bundle_tx.clone(), packet_tx.clone());
    let relayer = RelayerSvc::default();
    let searcher = SearcherSvc::new(bundle_tx.clone(), bundle_result_tx.clone());
    let block_engine_relayer = BlockEngineRelayerSvc::new(packet_tx.clone());

    let bundle_processor = BundleProcessor::new(
        bundle_tx.subscribe(),
        packet_tx.subscribe(),
        bundle_result_tx.clone(),
    );
    tokio::spawn(bundle_processor.run());

    tracing::info!("Serving gRPC on {addr}");
    Server::builder()
        .add_service(AuthServiceServer::new(auth))
        .add_service(BlockEngineValidatorServer::new(validator))
        .add_service(RelayerServer::new(relayer))
        .add_service(SearcherServiceServer::new(searcher))
        .add_service(BlockEngineRelayerServer::new(block_engine_relayer))
        .serve(addr)
        .await?;

    Ok(())
}

struct BundleProcessor {
    bundle_rx: tokio::sync::broadcast::Receiver<BundleUuid>,
    packet_rx: tokio::sync::broadcast::Receiver<be_proto::block_engine::SubscribePacketsResponse>,
    bundle_result_tx: tokio::sync::broadcast::Sender<BundleResult>,
}

impl BundleProcessor {
    fn new(
        bundle_rx: tokio::sync::broadcast::Receiver<BundleUuid>,
        packet_rx: tokio::sync::broadcast::Receiver<be_proto::block_engine::SubscribePacketsResponse>,
        bundle_result_tx: tokio::sync::broadcast::Sender<BundleResult>,
    ) -> Self {
        Self { bundle_rx, packet_rx, bundle_result_tx }
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                bundle_uuid = self.bundle_rx.recv() => {
                    match bundle_uuid {
                        Ok(bundle_uuid) => {
                            tracing::info!("Processing bundle: {}", bundle_uuid.uuid);
                            // Simulate bundle processing
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                            let result = if rand::random::<f32>() < 0.8 { // 80% chance of success
                                BundleResult {
                                    bundle_id: bundle_uuid.uuid.clone(),
                                    result: Some(be_proto::bundle::bundle_result::Result::Accepted(be_proto::bundle::Accepted {
                                        slot: 0, // Dummy slot
                                        validator_identity: "dummy_validator".to_string(),
                                    })),
                                }
                            } else {
                                BundleResult {
                                    bundle_id: bundle_uuid.uuid.clone(),
                                    result: Some(be_proto::bundle::bundle_result::Result::Rejected(be_proto::bundle::Rejected {
                                        reason: Some(be_proto::bundle::rejected::Reason::SimulationFailure(be_proto::bundle::SimulationFailure {
                                            tx_signature: "dummy_signature".to_string(),
                                            msg: Some("Simulated failure".to_string()),
                                        })),
                                    })),
                                }
                            };

                            if let Err(e) = self.bundle_result_tx.send(result) {
                                tracing::error!("Failed to send bundle result: {:?}", e);
                            }
                        },
                        Err(e) => tracing::error!("Error receiving bundle: {:?}", e),
                    }
                }
                packet_batch = self.packet_rx.recv() => {
                    match packet_batch {
                        Ok(packet_batch) => {
                            tracing::info!("Processing packet batch: {:?}", packet_batch);
                            // Packets are already forwarded to ValidatorSvc, no further action here for now
                        },
                        Err(e) => tracing::error!("Error receiving packet batch: {:?}", e),
                    }
                }
            }
        }
    }
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
    packet_tx: tokio::sync::broadcast::Sender<be_proto::block_engine::SubscribePacketsResponse>,
}

impl ValidatorSvc {
    fn new(
        bundle_tx: tokio::sync::broadcast::Sender<BundleUuid>,
        packet_tx: tokio::sync::broadcast::Sender<be_proto::block_engine::SubscribePacketsResponse>,
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

    type SubscribePacketsStream = Pin<Box<dyn Stream<Item = Result<be_proto::block_engine::SubscribePacketsResponse, Status>> + Send>>;
    async fn subscribe_packets(
        &self,
        _req: Request<be_proto::block_engine::SubscribePacketsRequest>,
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

struct BlockEngineRelayerSvc {
    packet_tx: tokio::sync::broadcast::Sender<be_proto::block_engine::SubscribePacketsResponse>,
}

impl BlockEngineRelayerSvc {
    fn new(
        packet_tx: tokio::sync::broadcast::Sender<be_proto::block_engine::SubscribePacketsResponse>,
    ) -> Self {
        Self { packet_tx }
    }
}

#[async_trait]
impl BlockEngineRelayer for BlockEngineRelayerSvc {
    async fn subscribe_accounts_of_interest(
        &self,
        _req: Request<AccountsOfInterestRequest>,
    ) -> Result<Response<Self::SubscribeAccountsOfInterestStream>, Status> {
        tracing::info!("SubscribeAccountsOfInterest RPC called.");
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let update = AccountsOfInterestUpdate {
                accounts: vec![], // Empty vector means all accounts allowed
            };
            if tx.send(Ok(update)).await.is_err() {
                // If the receiver is dropped, we can stop sending updates.
                return;
            }

            // Continue sending updates periodically if needed, or remove this loop
            // if only an initial update is required.
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                let update = AccountsOfInterestUpdate {
                    accounts: vec![], // Empty vector means all accounts allowed
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
        tracing::info!("SubscribeProgramsOfInterest RPC called.");
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        // ▶ 첫 업데이트 즉시 발송
        let initial = ProgramsOfInterestUpdate {
            programs: vec![
                "11111111111111111111111111111111".to_string(), // SystemProgram
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
                "SwapsVeCiPHMUAtzQWZw7RjsKjgCjhwU55QGu4U1Szw".to_string(),
            ],
        };
        tx.send(Ok(initial)).await.ok();

        // (선택) 이후 5초마다 갱신
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                let upd = ProgramsOfInterestUpdate {
                    programs: vec![
                        "11111111111111111111111111111111".to_string(),
                        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
                        "SwapsVeCiPHMUAtzQWZw7RjsKjgCjhwU55QGu4U1Szw".to_string(),
                    ],
                };
                if tx.send(Ok(upd)).await.is_err() { break }
            }
        });

        Ok(Response::new(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))))
    }

    type SubscribeProgramsOfInterestStream = Pin<Box<dyn Stream<Item = Result<ProgramsOfInterestUpdate, Status>> + Send>>;

    async fn start_expiring_packet_stream(
        &self,
        mut req: Request<tonic::Streaming<PacketBatchUpdate>>,
    ) -> Result<Response<Self::StartExpiringPacketStreamStream>, Status> {
        tracing::info!("StartExpiringPacketStream RPC called.");
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let packet_tx_clone = self.packet_tx.clone();

        tokio::spawn(async move {
            while let Ok(Some(msg)) = req.get_mut().message().await {
                tracing::trace!("Received PacketBatchUpdate: {:?}", msg);
                if let Some(packet_update_msg) = msg.msg {
                    match packet_update_msg {
                        PacketBatchUpdateMsg::Batches(expiring_batch) => {
                            let packet_batch = expiring_batch.batch.unwrap_or_default();
                            tracing::info!("Received packet batch with {} packets.", packet_batch.packets.len());
                            let header = expiring_batch.header;
                            let response = be_proto::block_engine::SubscribePacketsResponse {
                                header,
                                batch: Some(packet_batch),
                            };
                            if let Err(e) = packet_tx_clone.send(response) {
                                tracing::error!("Failed to send packet batch to broadcast channel: {:?}", e);
                            }
                        },
                        PacketBatchUpdateMsg::Heartbeat(heartbeat) => {
                            // Handle heartbeat if necessary, or just ignore
                            tracing::debug!("Received heartbeat from relayer: {:?}", heartbeat);
                        }
                    }
                }
            }
        });
        tokio::spawn(async move {
            // Send a dummy heartbeat every 1 second
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                let heartbeat = StartExpiringPacketStreamResponse {
                    heartbeat: Some(be_proto::shared::Heartbeat {
                        count: 0,
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

// -------------------- Searcher --------------------

struct SearcherSvc {
    bundle_tx: tokio::sync::broadcast::Sender<BundleUuid>,
    bundle_result_tx: tokio::sync::broadcast::Sender<BundleResult>,
}

impl SearcherSvc {
    fn new(
        bundle_tx: tokio::sync::broadcast::Sender<BundleUuid>,
        bundle_result_tx: tokio::sync::broadcast::Sender<BundleResult>,
    ) -> Self {
        Self { bundle_tx, bundle_result_tx }
    }
}

#[async_trait]
impl SearcherService for SearcherSvc {
    type SubscribeBundleResultsStream =
        Pin<Box<dyn Stream<Item = Result<BundleResult, Status>> + Send>>;

    async fn subscribe_bundle_results(
        &self,
        _req: Request<be_proto::searcher::SubscribeBundleResultsRequest>,
    ) -> Result<Response<Self::SubscribeBundleResultsStream>, Status> {
        let stream = tokio_stream::wrappers::BroadcastStream::new(self.bundle_result_tx.subscribe())
            .map_err(|e| Status::internal(format!("Bundle result stream error: {}", e)));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn send_bundle(
        &self,
        req: Request<be_proto::searcher::SendBundleRequest>,
    ) -> Result<Response<be_proto::searcher::SendBundleResponse>, Status> {
        let bundle_req = req.into_inner();
        let bundle = bundle_req.bundle.unwrap();
        let uuid = uuid::Uuid::new_v4().to_string();
        let bundle_uuid = BundleUuid {
            bundle: Some(bundle),
            uuid: uuid.clone(),
        };
        tracing::info!("Bundle received with UUID: {}", uuid);
        if let Err(e) = self.bundle_tx.send(bundle_uuid) {
            tracing::error!("Failed to send bundle to broadcast channel: {:?}", e);
            return Err(Status::internal(format!("Failed to send bundle: {}", e)));
        }
        Ok(Response::new(be_proto::searcher::SendBundleResponse {
            uuid,
        }))
    }

    async fn get_next_scheduled_leader(
        &self,
        _req: Request<be_proto::searcher::NextScheduledLeaderRequest>,
    ) -> Result<Response<be_proto::searcher::NextScheduledLeaderResponse>, Status> {
        Ok(Response::new(
            be_proto::searcher::NextScheduledLeaderResponse {
                current_slot: 0,
                next_leader_slot: 0,
                next_leader_identity: "".to_string(),
                next_leader_region: "".to_string(),
            },
        ))
    }

    async fn get_connected_leaders(
        &self,
        _req: Request<be_proto::searcher::ConnectedLeadersRequest>,
    ) -> Result<Response<be_proto::searcher::ConnectedLeadersResponse>, Status> {
        Ok(Response::new(
            be_proto::searcher::ConnectedLeadersResponse {
                connected_validators: Default::default(),
            },
        ))
    }

    async fn get_connected_leaders_regioned(
        &self,
        _req: Request<be_proto::searcher::ConnectedLeadersRegionedRequest>,
    ) -> Result<Response<be_proto::searcher::ConnectedLeadersRegionedResponse>, Status> {
        Ok(Response::new(
            be_proto::searcher::ConnectedLeadersRegionedResponse {
                connected_validators: Default::default(),
            },
        ))
    }

    async fn get_tip_accounts(
        &self,
        _req: Request<be_proto::searcher::GetTipAccountsRequest>,
    ) -> Result<Response<be_proto::searcher::GetTipAccountsResponse>, Status> {
        Ok(Response::new(be_proto::searcher::GetTipAccountsResponse {
            accounts: vec![],
        }))
    }

    async fn get_regions(
        &self,
        _req: Request<be_proto::searcher::GetRegionsRequest>,
    ) -> Result<Response<be_proto::searcher::GetRegionsResponse>, Status> {
        Ok(Response::new(be_proto::searcher::GetRegionsResponse {
            current_region: "".to_string(),
            available_regions: vec![],
        }))
    }
}


