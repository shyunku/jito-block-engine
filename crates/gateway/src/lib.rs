use async_trait::async_trait;
use be_proto::auth::auth_service_server::{AuthService, AuthServiceServer};
use be_proto::auth::{
    GenerateAuthChallengeRequest, GenerateAuthChallengeResponse, GenerateAuthTokensRequest,
    GenerateAuthTokensResponse, RefreshAccessTokenRequest, RefreshAccessTokenResponse, Token,
};
use be_proto::block_engine::block_engine_relayer_server::{
    BlockEngineRelayer, BlockEngineRelayerServer,
};
use be_proto::block_engine::block_engine_validator_server::{
    BlockEngineValidator, BlockEngineValidatorServer,
};
use be_proto::block_engine::{
    AccountsOfInterestRequest, AccountsOfInterestUpdate, BlockBuilderFeeInfoRequest,
    BlockBuilderFeeInfoResponse, PacketBatchUpdate, ProgramsOfInterestRequest,
    ProgramsOfInterestUpdate, StartExpiringPacketStreamResponse, SubscribeBundlesRequest,
    SubscribeBundlesResponse, packet_batch_update::Msg as PacketBatchUpdateMsg,
};
use be_proto::bundle::{BundleResult, BundleUuid};
use be_proto::packet::PacketBatch;
use be_proto::relayer::relayer_server::{Relayer, RelayerServer};
use be_proto::relayer::{
    GetTpuConfigsRequest, GetTpuConfigsResponse,
    SubscribePacketsRequest as RelayerSubscribePacketsRequest,
    SubscribePacketsResponse as RelayerSubscribePacketsResponse,
    subscribe_packets_response::Msg as RelayerSubscribePacketsResponseMsg,
};
use be_proto::searcher::searcher_service_server::{SearcherService, SearcherServiceServer};
use be_proto::shared::{Header, Socket};
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use futures_util::future;
use futures_util::stream::Stream;
use futures_util::stream::TryStreamExt;
use jsonwebtoken::DecodingKey;
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, TextEncoder};
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::wrappers::BroadcastStream;
use tonic::{Request, Response, Status, transport::Server};
use warp::Filter;

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub rpc_url: String,
    pub tpu_ip: String,
    pub tpu_port: u16,
    pub tpu_forward_ip: String,
    pub tpu_forward_port: u16,
    pub block_builder_pubkey: String,
    pub block_builder_commission: u64,
    pub min_bundle_size: usize,
    pub min_tip_lamports: u64,
    pub programs_of_interest: Vec<String>,
    pub validator_keypair_path: String,
}

// Wrapper struct for solana_sdk::packet::Packet to implement From trait
struct WrappedSolanaPacket(solana_sdk::packet::Packet);

impl From<WrappedSolanaPacket> for be_proto::packet::Packet {
    fn from(p: WrappedSolanaPacket) -> Self {
        be_proto::packet::Packet {
            data: p.0.data(..).unwrap().to_vec(),
            meta: Some(be_proto::packet::Meta {
                size: p.0.meta().size as u64,
                addr: p.0.meta().addr.to_string(),
                port: p.0.meta().port as u32,
                flags: Some(be_proto::packet::PacketFlags {
                    discard: p.0.meta().discard(),
                    forwarded: p.0.meta().forwarded(),
                    repair: p.0.meta().repair(),
                    simple_vote_tx: p.0.meta().is_simple_vote_tx(),
                    tracer_packet: p.0.meta().is_perf_track_packet(),
                    from_staked_node: p.0.meta().is_from_staked_node(),
                }),
                sender_stake: 0,
            }),
        }
    }
}

pub async fn run(config: Config, addr: std::net::SocketAddr) -> anyhow::Result<()> {
    let (bundle_tx, _) = tokio::sync::broadcast::channel(128);
    let (packet_tx, _) = tokio::sync::broadcast::channel(128);
    let (bundle_result_tx, _) = tokio::sync::broadcast::channel(128);

    let (_for_leader_tx, for_leader_rx) = tokio::sync::mpsc::channel(128);

    let rpc_client = Arc::new(RpcClient::new(config.rpc_url.clone()));
    let validator_keypair = Keypair::read_from_file(&config.validator_keypair_path)
        .expect("Failed to load validator keypair");
    let validator_identity = validator_keypair.pubkey().to_string();

    tracing::info!("Connected to RPC with {:?}", config.rpc_url);

    let bundle_accepted_counter =
        IntCounter::new("bundle_accepted_total", "Total number of accepted bundles")?;
    let bundle_rejected_counter =
        IntCounter::new("bundle_rejected_total", "Total number of rejected bundles")?;
    let hist_opts = HistogramOpts::new(
        "bundle_processing_duration_seconds",
        "Bundle processing duration",
    )
    .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]);
    let bundle_processing_duration = Histogram::with_opts(hist_opts)?;

    prometheus::default_registry().register(Box::new(bundle_accepted_counter.clone()))?;
    prometheus::default_registry().register(Box::new(bundle_rejected_counter.clone()))?;
    prometheus::default_registry().register(Box::new(bundle_processing_duration.clone()))?;

    let auth = AuthSvc::new();
    let verifying_bytes = auth.signing.verifying_key().to_bytes();
    let jwt_decoding_key = DecodingKey::from_ed_der(&verifying_bytes);

    let validator = ValidatorSvc::new(
        bundle_tx.clone(),
        packet_tx.clone(),
        config.block_builder_pubkey.clone(),
        config.block_builder_commission,
    );
    let relayer = RelayerSvc::new(
        config.tpu_ip.clone(),
        config.tpu_port,
        config.tpu_forward_ip.clone(),
        config.tpu_forward_port,
    );
    let searcher = SearcherSvc::new(
        bundle_tx.clone(),
        bundle_result_tx.clone(),
        jwt_decoding_key,
        config.min_bundle_size,
        config.min_tip_lamports,
        rpc_client.clone(),
    );
    let block_engine_relayer = BlockEngineRelayerSvc::new(packet_tx.clone(), Arc::new(config));

    let bundle_processor = BundleProcessor::new(
        bundle_tx.subscribe(),
        packet_tx.subscribe(),
        bundle_result_tx.clone(),
        rpc_client.clone(),
        bundle_accepted_counter,
        bundle_rejected_counter,
        bundle_processing_duration,
        validator_identity.clone(),
    );
    tokio::spawn(bundle_processor.run());

    let for_leader_queue = ForLeaderQueue::new(for_leader_rx, rpc_client.clone());
    tokio::spawn(for_leader_queue.run());

    let metrics_route = warp::path!("metrics").map(|| {
        let encoder = TextEncoder::new();
        let metric_families = prometheus::default_registry().gather();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    });

    tracing::info!("Serving gRPC on {addr}");
    tracing::info!("Serving Prometheus metrics on 0.0.0.0:9000/metrics");

    let grpc_server = Server::builder()
        .add_service(AuthServiceServer::new(auth))
        .add_service(BlockEngineValidatorServer::new(validator))
        .add_service(RelayerServer::new(relayer))
        .add_service(SearcherServiceServer::new(searcher))
        .add_service(BlockEngineRelayerServer::new(block_engine_relayer))
        .serve(addr);

    let metrics_server = warp::serve(metrics_route).run(([0, 0, 0, 0], 9000));

    tokio::select! {
        _ = grpc_server => {},
        _ = metrics_server => {},
    }

    Ok(())
}

use solana_sdk::{
    commitment_config::CommitmentConfig, signature::EncodableKey, signature::Keypair,
    signature::Signature, signer::Signer, transaction::VersionedTransaction,
};
use solana_client::rpc_config::RpcSimulateTransactionConfig;
use tokio_util::time::DelayQueue;

struct BundleProcessor {
    bundle_rx: tokio::sync::broadcast::Receiver<BundleUuid>,
    packet_rx: tokio::sync::broadcast::Receiver<be_proto::block_engine::SubscribePacketsResponse>,
    bundle_result_tx: tokio::sync::broadcast::Sender<BundleResult>,
    rpc_client: Arc<RpcClient>,
    bundle_accepted_counter: IntCounter,
    bundle_rejected_counter: IntCounter,
    bundle_processing_duration: Histogram,
    validator_identity: String,
}

impl BundleProcessor {
    #[allow(clippy::too_many_arguments)]
    fn new(
        bundle_rx: tokio::sync::broadcast::Receiver<BundleUuid>,
        packet_rx: tokio::sync::broadcast::Receiver<
            be_proto::block_engine::SubscribePacketsResponse,
        >,
        bundle_result_tx: tokio::sync::broadcast::Sender<BundleResult>,
        rpc_client: Arc<RpcClient>,
        bundle_accepted_counter: IntCounter,
        bundle_rejected_counter: IntCounter,
        bundle_processing_duration: Histogram,
        validator_identity: String,
    ) -> Self {
        Self {
            bundle_rx,
            packet_rx,
            bundle_result_tx,
            rpc_client,
            bundle_accepted_counter,
            bundle_rejected_counter,
            bundle_processing_duration,
            validator_identity,
        }
    }

    async fn simulate_bundle(
        rpc: &RpcClient,
        bundle: &be_proto::bundle::Bundle,
    ) -> Result<(), (Signature, String)> {
        for packet in &bundle.packets {
            let tx: VersionedTransaction = bincode::deserialize(&packet.data).unwrap();
            let sig = tx.signatures[0];
            tracing::info!(
                "Simulating transaction with blockhash: {:?}",
                tx.message.recent_blockhash()
            );
            let res = rpc
                .simulate_transaction_with_config(
                    &tx,
                    RpcSimulateTransactionConfig {
                        commitment: Some(CommitmentConfig::processed()),
                        replace_recent_blockhash: true,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| (sig, format!("RPC simulation call failed: {:?}", e)))?;
            if let Some(err) = res.value.err {
                return Err((sig, format!("tx {} failed: {:?}", sig, err)));
            }
        }
        Ok(())
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                bundle_uuid = self.bundle_rx.recv() => {
                    match bundle_uuid {
                        Ok(bundle_uuid) => {
                            tracing::info!("Processing bundle: {}", bundle_uuid.uuid);
                            let timer = self.bundle_processing_duration.start_timer();

                            let result = match Self::simulate_bundle(
                                &self.rpc_client,
                                &bundle_uuid.bundle.as_ref().unwrap(),
                            ).await {
                                Ok(_) => {
                                    self.bundle_accepted_counter.inc();
                                    tracing::info!("Bundle simulation successful for bundle: {}", bundle_uuid.uuid);
                                    BundleResult {
                                        bundle_id: bundle_uuid.uuid.clone(),
                                        result: Some(be_proto::bundle::bundle_result::Result::Accepted(be_proto::bundle::Accepted {
                                            slot: self.rpc_client.get_slot().await.unwrap_or(0),
                                            validator_identity: self.validator_identity.clone(),
                                        })),
                                    }
                                }
                                Err((sig, e)) => {
                                    self.bundle_rejected_counter.inc();
                                    tracing::warn!("Bundle simulation failed: {:?}", e);
                                    BundleResult {
                                        bundle_id: bundle_uuid.uuid.clone(),
                                        result: Some(be_proto::bundle::bundle_result::Result::Rejected(be_proto::bundle::Rejected {
                                            reason: Some(be_proto::bundle::rejected::Reason::SimulationFailure(be_proto::bundle::SimulationFailure {
                                                tx_signature: sig.to_string(),
                                                msg: Some(format!("Simulated failure: {}", e)),
                                            })),
                                        })),
                                    }
                                },
                            };
                            timer.stop_and_record();

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

struct ForLeaderQueue {
    bundle_rx: tokio::sync::mpsc::Receiver<BundleUuid>,
    rpc_client: Arc<RpcClient>,
}

impl ForLeaderQueue {
    fn new(bundle_rx: tokio::sync::mpsc::Receiver<BundleUuid>, rpc_client: Arc<RpcClient>) -> Self {
        Self {
            bundle_rx,
            rpc_client,
        }
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                bundle_uuid_option = self.bundle_rx.recv() => {
                    if let Some(bundle_uuid) = bundle_uuid_option {
                        tracing::info!("Bundle received for leader queue: {}", bundle_uuid.uuid);
                        // In a real implementation, you would get the actual leader schedule
                        // and send the bundle to the appropriate TPU port at the right time.
                        // For now, we'll just simulate sending it.

                        let current_slot = match self.rpc_client.get_slot().await {
                            Ok(slot) => slot,
                            Err(e) => {
                                tracing::error!("Failed to get current slot: {:?}", e);
                                continue;
                            }
                        };

                        // Simulate sending to TPU 13 slots ahead
                        let target_slot = current_slot + 13;
                        tracing::info!("Simulating sending bundle {} to TPU for slot {}", bundle_uuid.uuid, target_slot);

                        if let Some(bundle) = bundle_uuid.bundle {
                            for tx_bytes in bundle.packets {
                                // In a real scenario, you'd send this to the TPU port
                                // For now, just log it.
                                tracing::info!("Simulating sending transaction to TPU: {:?}", tx_bytes);
                            }
                        }
                    }
                }
            }
        }
    }
}

// -------------------- Auth --------------------

use ed25519_dalek::{SigningKey, Verifier, VerifyingKey};
use jsonwebtoken::{Header as JwtHeader, Validation, decode, encode};
use rand::rngs::OsRng;
use std::collections::HashSet;
use tokio::sync::Mutex;

// -------------------- Auth --------------------

struct AuthSvc {
    pending: Arc<Mutex<HashSet<String>>>, // 1회용 챌린지 문자열
    signing: SigningKey,
}

impl AuthSvc {
    fn new() -> Self {
        let mut csprng = OsRng;
        Self {
            pending: Arc::new(Mutex::new(HashSet::new())),
            signing: SigningKey::generate(&mut csprng),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String, // Subject (public key of the searcher)
    exp: u64,    // Expiration time
    iat: u64,    // Issued at
}

#[async_trait]
impl AuthService for AuthSvc {
    async fn generate_auth_challenge(
        &self,
        req: Request<GenerateAuthChallengeRequest>,
    ) -> Result<Response<GenerateAuthChallengeResponse>, Status> {
        use base64::{Engine as _, engine::general_purpose};
        use solana_sdk::pubkey::Pubkey;

        // relayer는 raw(32) 바이트 pubkey를 보낸다
        let raw = req.into_inner().pubkey;
        if raw.len() != 32 {
            return Err(Status::invalid_argument("pubkey must be 32 bytes"));
        }
        let b58 = Pubkey::new_from_array(raw.clone().try_into().unwrap()).to_string();

        let token: [u8; 32] = rand::random();
        let token_b64 = general_purpose::STANDARD_NO_PAD.encode(token);

        // "<b58-pubkey>-<base64-token>"
        let challenge_str = format!("{b58}-{token_b64}");

        // 1회용으로 저장
        self.pending.lock().await.insert(challenge_str.clone());

        Ok(Response::new(GenerateAuthChallengeResponse {
            challenge: token_b64, // ← Jito 프로토콜 그대로
        }))
    }

    async fn generate_auth_tokens(
        &self,
        req: Request<GenerateAuthTokensRequest>,
    ) -> Result<Response<GenerateAuthTokensResponse>, Status> {
        use base64::{Engine as _, engine::general_purpose};
        use solana_sdk::bs58;

        let inner = req.into_inner();
        let full_chal = inner.challenge; // "<pubkey>-<token>"
        let client_pk = inner.client_pubkey; // 32 raw
        let sig_field = inner.signed_challenge; // Vec<u8>

        /* ① challenge 존재 & 1회 사용 */
        {
            let mut set = self.pending.lock().await;
            if !set.remove(&full_chal) {
                return Err(Status::unauthenticated("Unknown or expired challenge"));
            }
        }

        /* ② challenge 파싱 & pubkey 일치 */
        let (b58_pk, _token) = full_chal
            .split_once('-')
            .ok_or_else(|| Status::invalid_argument("Malformed challenge"))?;
        let expected_pk = bs58::decode(b58_pk)
            .into_vec()
            .map_err(|_| Status::invalid_argument("Invalid pubkey in challenge"))?;
        if expected_pk != client_pk {
            return Err(Status::invalid_argument("pubkey mismatch"));
        }

        /* ③ 서명 디코딩 (raw / b58 / hex / b64) */
        let sig_bytes = decode_sig(sig_field)?;
        let signature = ed25519_dalek::Signature::from_slice(&sig_bytes)
            .map_err(|_| Status::invalid_argument("bad signature len"))?;

        /* ④ 서명 검증 (메시지 = full challenge 문자열 바이트) */
        let pk_arr: [u8; 32] = client_pk
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("pubkey len"))?;
        VerifyingKey::from_bytes(&pk_arr)
            .map_err(|_| Status::invalid_argument("bad pubkey"))?
            .verify(full_chal.as_bytes(), &signature)
            .map_err(|_| Status::unauthenticated("Signature verification failed"))?;

        /* ───── 5. JWT 발급 (기존 로직 그대로) ───── */
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let access_token_expires_at = now + 600; // 10 min
        let refresh_token_expires_at = now + 3600; // 1 h

        let access_claims = Claims {
            sub: general_purpose::STANDARD_NO_PAD.encode(&client_pk),
            exp: access_token_expires_at,
            iat: now,
        };
        let refresh_claims = Claims {
            sub: general_purpose::STANDARD_NO_PAD.encode(&client_pk),
            exp: refresh_token_expires_at,
            iat: now,
        };

        let der_priv = self
            .signing
            .to_pkcs8_der() // PKCS#8(v2) → DER
            .map_err(|_| Status::internal("pkcs8 encode"))?;
        let encoding_key = jsonwebtoken::EncodingKey::from_ed_der(der_priv.as_bytes());

        let header = JwtHeader {
            alg: jsonwebtoken::Algorithm::EdDSA,
            ..Default::default()
        };
        let access_token_value = encode(&header, &access_claims, &encoding_key).unwrap();
        let refresh_token_value = encode(&header, &refresh_claims, &encoding_key).unwrap();

        Ok(Response::new(GenerateAuthTokensResponse {
            access_token: Some(Token {
                value: access_token_value,
                expires_at_utc: Some(prost_types::Timestamp {
                    seconds: access_token_expires_at as i64,
                    nanos: 0,
                }),
            }),
            refresh_token: Some(Token {
                value: refresh_token_value,
                expires_at_utc: Some(prost_types::Timestamp {
                    seconds: refresh_token_expires_at as i64,
                    nanos: 0,
                }),
            }),
        }))
    }

    async fn refresh_access_token(
        &self,
        req: Request<RefreshAccessTokenRequest>,
    ) -> Result<Response<RefreshAccessTokenResponse>, Status> {
        let inner_req = req.into_inner();
        let refresh_token_value = inner_req.refresh_token;

        // ── ① pubkey → DER
        let der_pub = self
            .signing
            .verifying_key()
            .to_public_key_der()
            .map_err(|_| Status::internal("pkcs8 pub encode"))?;

        // ── ② DecodingKey 생성
        let decoding_key = DecodingKey::from_ed_der(der_pub.as_bytes());

        let token_data = match decode::<Claims>(
            &refresh_token_value,
            &decoding_key,
            &Validation::new(jsonwebtoken::Algorithm::EdDSA),
        ) {
            Ok(data) => data,
            Err(_) => return Err(Status::unauthenticated("Invalid or expired refresh token")),
        };

        let pubkey = token_data.claims.sub;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let access_token_expires_at = now + 600; // 10 minutes

        let access_claims = Claims {
            sub: pubkey.clone(),
            exp: access_token_expires_at,
            iat: now,
        };

        let der_priv = self
            .signing
            .to_pkcs8_der() // ← PKCS#8 priv → DER
            .map_err(|_| Status::internal("pkcs8 priv encode"))?;
        let encoding_key = jsonwebtoken::EncodingKey::from_ed_der(der_priv.as_bytes());
        let header = JwtHeader {
            alg: jsonwebtoken::Algorithm::EdDSA,
            ..Default::default()
        };
        let access_token_value = encode(&header, &access_claims, &encoding_key).unwrap();

        let access_token = Token {
            value: access_token_value,
            expires_at_utc: Some(prost_types::Timestamp {
                seconds: access_token_expires_at as i64,
                nanos: 0,
            }),
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
    block_builder_pubkey: String,
    block_builder_commission: u64,
}

impl ValidatorSvc {
    fn new(
        bundle_tx: tokio::sync::broadcast::Sender<BundleUuid>,
        packet_tx: tokio::sync::broadcast::Sender<be_proto::block_engine::SubscribePacketsResponse>,
        block_builder_pubkey: String,
        block_builder_commission: u64,
    ) -> Self {
        Self {
            bundle_tx,
            packet_tx,
            block_builder_pubkey,
            block_builder_commission,
        }
    }
}

#[async_trait]
impl BlockEngineValidator for ValidatorSvc {
    async fn get_block_builder_fee_info(
        &self,
        _req: Request<BlockBuilderFeeInfoRequest>,
    ) -> Result<Response<BlockBuilderFeeInfoResponse>, Status> {
        Ok(Response::new(BlockBuilderFeeInfoResponse {
            pubkey: self.block_builder_pubkey.clone(),
            commission: self.block_builder_commission,
        }))
    }

    type SubscribeBundlesStream =
        Pin<Box<dyn Stream<Item = Result<SubscribeBundlesResponse, Status>> + Send>>;
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

    type SubscribePacketsStream = Pin<
        Box<
            dyn Stream<Item = Result<be_proto::block_engine::SubscribePacketsResponse, Status>>
                + Send,
        >,
    >;
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

struct RelayerSvc {
    tpu_ip: String,
    tpu_port: u16,
    tpu_forward_ip: String,
    tpu_forward_port: u16,
}

impl RelayerSvc {
    fn new(tpu_ip: String, tpu_port: u16, tpu_forward_ip: String, tpu_forward_port: u16) -> Self {
        Self {
            tpu_ip,
            tpu_port,
            tpu_forward_ip,
            tpu_forward_port,
        }
    }
}

#[async_trait]
impl Relayer for RelayerSvc {
    async fn get_tpu_configs(
        &self,
        _req: Request<GetTpuConfigsRequest>,
    ) -> Result<Response<GetTpuConfigsResponse>, Status> {
        Ok(Response::new(GetTpuConfigsResponse {
            tpu: Some(Socket {
                ip: self.tpu_ip.clone(),
                port: self.tpu_port as i64,
            }),
            tpu_forward: Some(Socket {
                ip: self.tpu_forward_ip.clone(),
                port: self.tpu_forward_port as i64,
            }),
        }))
    }

    type SubscribePacketsStream =
        Pin<Box<dyn Stream<Item = Result<RelayerSubscribePacketsResponse, Status>> + Send>>;
    async fn subscribe_packets(
        &self,
        _req: Request<RelayerSubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        tokio::spawn(async move {
            let socket = match tokio::net::UdpSocket::bind("0.0.0.0:8001").await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Failed to bind UDP socket for relayer: {:?}", e);
                    return;
                }
            };
            tracing::info!("Relayer listening for packets on 0.0.0.0:8001");

            let mut buf = [0u8; 1232]; // PACKET_DATA_SIZE
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((len, addr)) => {
                        let packet = solana_sdk::packet::Packet::from_data(
                            Some(&addr), // ✅ IpAddr
                            &buf[..len],
                        )
                        .unwrap();
                        let packet_batch = PacketBatch {
                            packets: vec![WrappedSolanaPacket(packet).into()], // Convert solana_sdk::packet::Packet to be_proto::packet::Packet
                        };
                        let header = Header {
                            ts: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                        };
                        let response = RelayerSubscribePacketsResponse {
                            header: Some(header),
                            msg: Some(RelayerSubscribePacketsResponseMsg::Batch(packet_batch)),
                        };
                        if tx.send(Ok(response)).await.is_err() {
                            tracing::warn!("Failed to send packet batch to relayer subscriber");
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error receiving UDP packet: {:?}", e);
                        break;
                    }
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
    expiring_packets: Arc<Mutex<DelayQueue<String>>>,
    config: Arc<Config>,
}

impl BlockEngineRelayerSvc {
    fn new(
        packet_tx: tokio::sync::broadcast::Sender<be_proto::block_engine::SubscribePacketsResponse>,
        config: Arc<Config>,
    ) -> Self {
        Self {
            packet_tx,
            expiring_packets: Arc::new(Mutex::new(DelayQueue::new())),
            config,
        }
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

    type SubscribeAccountsOfInterestStream =
        Pin<Box<dyn Stream<Item = Result<AccountsOfInterestUpdate, Status>> + Send>>;

    async fn subscribe_programs_of_interest(
        &self,
        _req: Request<ProgramsOfInterestRequest>,
    ) -> Result<Response<Self::SubscribeProgramsOfInterestStream>, Status> {
        tracing::info!("SubscribeProgramsOfInterest RPC called.");
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let programs_of_interest = self.config.programs_of_interest.clone();

        // ▶ 첫 업데이트 즉시 발송
        let initial = ProgramsOfInterestUpdate {
            programs: programs_of_interest.clone(),
        };
        tx.send(Ok(initial)).await.ok();

        // (선택) 이후 5초마다 갱신
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                let upd = ProgramsOfInterestUpdate {
                    programs: programs_of_interest.clone(),
                };
                if tx.send(Ok(upd)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    type SubscribeProgramsOfInterestStream =
        Pin<Box<dyn Stream<Item = Result<ProgramsOfInterestUpdate, Status>> + Send>>;

    async fn start_expiring_packet_stream(
        &self,
        mut req: Request<tonic::Streaming<PacketBatchUpdate>>,
    ) -> Result<Response<Self::StartExpiringPacketStreamStream>, Status> {
        tracing::info!("StartExpiringPacketStream RPC called.");
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let packet_tx_clone = self.packet_tx.clone();
        let expiring_packets_clone_read = Arc::clone(&self.expiring_packets);
        let expiring_packets_clone_gc = Arc::clone(&self.expiring_packets);

        tokio::spawn({
            let expiring_packets_clone = expiring_packets_clone_read;
            async move {
                while let Ok(Some(msg)) = req.get_mut().message().await {
                    tracing::trace!("Received PacketBatchUpdate: {:?}", msg);
                    if let Some(packet_update_msg) = msg.msg {
                        match packet_update_msg {
                            PacketBatchUpdateMsg::Batches(expiring_batch) => {
                                let packet_batch = expiring_batch.batch.unwrap_or_default();
                                tracing::info!(
                                    "Received packet batch with {} packets.",
                                    packet_batch.packets.len()
                                );
                                let header = expiring_batch.header;
                                let response = be_proto::block_engine::SubscribePacketsResponse {
                                    header,
                                    batch: Some(packet_batch),
                                };
                                // Generate a unique ID for the packet batch to track its expiry
                                let packet_batch_id = uuid::Uuid::new_v4().to_string();
                                let expiry_duration = tokio::time::Duration::from_secs(2); // Example TTL: 2 seconds
                                expiring_packets_clone
                                    .lock()
                                    .await
                                    .insert(packet_batch_id.clone(), expiry_duration);

                                if let Err(e) = packet_tx_clone.send(response) {
                                    tracing::error!(
                                        "Failed to send packet batch to broadcast channel: {:?}",
                                        e
                                    );
                                }
                            }
                            PacketBatchUpdateMsg::Heartbeat(heartbeat) => {
                                // Handle heartbeat if necessary, or just ignore
                                tracing::debug!("Received heartbeat from relayer: {:?}", heartbeat);
                            }
                        }
                    }
                }
            }
        });
        tokio::spawn({
            let expiring_packets_clone = expiring_packets_clone_gc;
            async move {
                // Periodically clean up expired packets
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; // Check every 100ms
                    let mut expiring_packets = expiring_packets_clone.lock().await;
                    while let Some(expired) =
                        future::poll_fn(|cx| expiring_packets.poll_expired(cx)).await
                    {
                        tracing::info!("Packet batch expired: {}", expired.into_inner());
                        // Here you would typically remove the packet from any active queues
                        // or mark it as expired in your system.
                    }
                }
            }
        });
        tokio::spawn(async move {
            // Send a dummy heartbeat every 1 second
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                let heartbeat = StartExpiringPacketStreamResponse {
                    heartbeat: Some(be_proto::shared::Heartbeat { count: 0 }),
                };
                if tx.send(Ok(heartbeat)).await.is_err() {
                    break;
                }
            }
        });
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    type StartExpiringPacketStreamStream =
        Pin<Box<dyn Stream<Item = Result<StartExpiringPacketStreamResponse, Status>> + Send>>;
}

// -------------------- Searcher --------------------

struct SearcherSvc {
    bundle_tx: tokio::sync::broadcast::Sender<BundleUuid>,
    bundle_result_tx: tokio::sync::broadcast::Sender<BundleResult>,
    jwt_decoding_key: DecodingKey,
    min_bundle_size: usize,
    min_tip_lamports: u64,
    rpc_client: Arc<RpcClient>,
}

impl SearcherSvc {
    #[allow(clippy::too_many_arguments)]
    fn new(
        bundle_tx: tokio::sync::broadcast::Sender<BundleUuid>,
        bundle_result_tx: tokio::sync::broadcast::Sender<BundleResult>,
        jwt_decoding_key: DecodingKey,
        min_bundle_size: usize,
        min_tip_lamports: u64,
        rpc_client: Arc<RpcClient>,
    ) -> Self {
        Self {
            bundle_tx,
            bundle_result_tx,
            jwt_decoding_key,
            min_bundle_size,
            min_tip_lamports,
            rpc_client,
        }
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
        let stream =
            tokio_stream::wrappers::BroadcastStream::new(self.bundle_result_tx.subscribe())
                .map_err(|e| Status::internal(format!("Bundle result stream error: {}", e)));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn send_bundle(
        &self,
        req: Request<be_proto::searcher::SendBundleRequest>,
    ) -> Result<Response<be_proto::searcher::SendBundleResponse>, Status> {
        // 1. Access Token 검증
        let auth_header = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("Authorization token missing"))?;
        let token_str = auth_header.trim_start_matches("Bearer ");

        let token_data = match decode::<Claims>(
            token_str,
            &self.jwt_decoding_key,
            &Validation::new(jsonwebtoken::Algorithm::EdDSA),
        ) {
            Ok(data) => data,
            Err(_) => return Err(Status::unauthenticated("Invalid or expired access token")),
        };
        tracing::info!(
            "Bundle received from authenticated searcher: {}",
            token_data.claims.sub
        );

        let bundle_req = req.into_inner();
        let bundle = bundle_req.bundle.unwrap();

        // 2. Tip/priority fee 계산 및 번들 헤더 변경 (간단한 더미 계산)
        let mut total_tip_lamports = 0;
        let mut total_size = 0;
        for packet in &bundle.packets {
            // Simulate fee calculation (e.g., based on transaction size or some heuristic)
            total_tip_lamports += 5000; // Dummy tip
            total_size += packet.data.len();
        }
        // Update bundle header with calculated tip (assuming a field exists or can be added)
        // For now, we'll just log it.
        tracing::info!(
            "Calculated total tip: {} lamports, total size: {} bytes",
            total_tip_lamports,
            total_size
        );

        // 3. 최소 gas / size 제한 체크
        if total_size < self.min_bundle_size {
            return Err(Status::invalid_argument(format!(
                "Bundle size {} bytes is below minimum {} bytes",
                total_size, self.min_bundle_size
            )));
        }
        if total_tip_lamports < self.min_tip_lamports {
            return Err(Status::invalid_argument(format!(
                "Bundle tip {} lamports is below minimum {} lamports",
                total_tip_lamports, self.min_tip_lamports
            )));
        }

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
        let current_slot = self.rpc_client.get_slot().await.unwrap_or(0);
        let next_leader_slot = current_slot + 2; // Simulate next leader in 2 slots
        Ok(Response::new(
            be_proto::searcher::NextScheduledLeaderResponse {
                current_slot,
                next_leader_slot,
                next_leader_identity: "validator_A_identity".to_string(),
                next_leader_region: "us-east-1".to_string(),
            },
        ))
    }

    async fn get_connected_leaders(
        &self,
        _req: Request<be_proto::searcher::ConnectedLeadersRequest>,
    ) -> Result<Response<be_proto::searcher::ConnectedLeadersResponse>, Status> {
        // TODO: Fetch actual connected leaders from a validator set or similar source
        Ok(Response::new(
            be_proto::searcher::ConnectedLeadersResponse {
                connected_validators: [
                    (
                        "validator_B_pubkey".to_string(),
                        be_proto::searcher::SlotList {
                            slots: vec![100, 101],
                        },
                    ),
                    (
                        "validator_C_pubkey".to_string(),
                        be_proto::searcher::SlotList {
                            slots: vec![102, 103],
                        },
                    ),
                ]
                .into_iter()
                .collect(),
            },
        ))
    }

    async fn get_connected_leaders_regioned(
        &self,
        _req: Request<be_proto::searcher::ConnectedLeadersRegionedRequest>,
    ) -> Result<Response<be_proto::searcher::ConnectedLeadersRegionedResponse>, Status> {
        // TODO: Fetch actual connected leaders by region
        Ok(Response::new(
            be_proto::searcher::ConnectedLeadersRegionedResponse {
                connected_validators: [(
                    "us-east-1".to_string(),
                    be_proto::searcher::ConnectedLeadersResponse {
                        connected_validators: [(
                            "validator_D_pubkey".to_string(),
                            be_proto::searcher::SlotList {
                                slots: vec![104, 105],
                            },
                        )]
                        .into_iter()
                        .collect(),
                    },
                )]
                .into_iter()
                .collect(),
            },
        ))
    }

    async fn get_tip_accounts(
        &self,
        _req: Request<be_proto::searcher::GetTipAccountsRequest>,
    ) -> Result<Response<be_proto::searcher::GetTipAccountsResponse>, Status> {
        // TODO: Fetch actual tip accounts from a configuration or on-chain source
        Ok(Response::new(be_proto::searcher::GetTipAccountsResponse {
            accounts: vec![
                "TipAccount11111111111111111111111111111111".to_string(),
                "TipAccount22222222222222222222222222222222".to_string(),
            ],
        }))
    }

    async fn get_regions(
        &self,
        _req: Request<be_proto::searcher::GetRegionsRequest>,
    ) -> Result<Response<be_proto::searcher::GetRegionsResponse>, Status> {
        // TODO: Fetch actual regions from a configuration or discovery service
        Ok(Response::new(be_proto::searcher::GetRegionsResponse {
            current_region: "us-east-1".to_string(),
            available_regions: vec![
                "us-east-1".to_string(),
                "us-west-2".to_string(),
                "eu-central-1".to_string(),
                "ap-southeast-1".to_string(),
            ],
        }))
    }
}

fn decode_sig(field: Vec<u8>) -> Result<Vec<u8>, Status> {
    use base64::{Engine as _, engine::general_purpose};
    use hex;
    use solana_sdk::bs58;

    if field.len() == 64 {
        return Ok(field);
    }
    let s = std::str::from_utf8(&field).map_err(|_| Status::invalid_argument("sig bytes"))?;
    if let Ok(b) = bs58::decode(s).into_vec() {
        return Ok(b);
    }
    if let Ok(b) = general_purpose::STANDARD.decode(s) {
        return Ok(b);
    }
    if let Ok(b) = general_purpose::STANDARD_NO_PAD.decode(s) {
        return Ok(b);
    }
    if let Ok(b) = hex::decode(s.trim_start_matches("0x")) {
        return Ok(b);
    }
    Err(Status::invalid_argument("sig format"))
}
