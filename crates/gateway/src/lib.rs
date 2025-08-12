use anyhow::anyhow;
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
    packet_batch_update::Msg as PacketBatchUpdateMsg, AccountsOfInterestRequest,
    AccountsOfInterestUpdate, BlockBuilderFeeInfoRequest, BlockBuilderFeeInfoResponse,
    PacketBatchUpdate, ProgramsOfInterestRequest, ProgramsOfInterestUpdate,
    StartExpiringPacketStreamResponse, SubscribeBundlesRequest, SubscribeBundlesResponse,
};
use be_proto::bundle::{BundleResult, BundleUuid};
use be_proto::packet::PacketBatch;
use be_proto::relayer::relayer_server::{Relayer, RelayerServer};
use be_proto::relayer::{
    subscribe_packets_response::Msg as RelayerSubscribePacketsResponseMsg, GetTpuConfigsRequest,
    GetTpuConfigsResponse, SubscribePacketsRequest as RelayerSubscribePacketsRequest,
    SubscribePacketsResponse as RelayerSubscribePacketsResponse,
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
use solana_ledger::bank_forks_utils;
use solana_ledger::blockstore::Blockstore;
use solana_ledger::blockstore_options::{AccessType, BlockstoreOptions};
use solana_ledger::blockstore_processor::{self, ProcessOptions};
use solana_ledger::leader_schedule_cache::LeaderScheduleCache;
use solana_runtime::bank::Bank;
use solana_runtime::bank_forks::BankForks;
use solana_runtime::snapshot_config::SnapshotConfig;
use solana_sdk::genesis_config::GenesisConfig;
use solana_sdk::sanitize::Sanitize;
use solana_sdk::transaction::Transaction;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::BroadcastStream;
use tonic::{transport::Server, Request, Response, Status};
use warp::Filter;

/// 블록 엔진 게이트웨이의 설정을 나타냅니다.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    /// 솔라나 RPC 엔드포인트의 URL입니다.
    pub rpc_url: String,
    /// TPU (트랜잭션 처리 장치)의 IP 주소입니다.
    pub tpu_ip: String,
    /// TPU의 포트입니다.
    pub tpu_port: u16,
    /// TPU 패킷을 전달할 IP 주소입니다.
    pub tpu_forward_ip: String,
    /// TPU 패킷을 전달할 포트입니다.
    pub tpu_forward_port: u16,
    /// 블록 빌더의 공개 키입니다.
    pub block_builder_pubkey: String,
    /// 블록 빌더의 수수료율입니다.
    pub block_builder_commission: u64,
    /// 번들의 최소 크기입니다.
    pub min_bundle_size: usize,
    /// 번들의 최소 팁 (lamports 단위) 입니다.
    pub min_tip_lamports: u64,
    /// 관심 프로그램 목록입니다.
    pub programs_of_interest: Vec<String>,
    /// 밸리데이터의 키페어 파일 경로입니다.
    pub validator_keypair_path: String,
    /// 원장(ledger) 디렉토리 경로입니다.
    pub ledger_path: String,
    /// 스냅샷 디렉토리 경로입니다.
    pub snapshot_path: PathBuf,
}

/// `solana_sdk::packet::Packet`을 protobuf로 정의된 `be_proto::packet::Packet`으로
/// 변환하기 위한 래퍼 구조체입니다.
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

/// 블록 엔진 게이트웨이의 메인 진입점입니다.
///
/// 이 함수는 gRPC 서버, 프로메테우스 메트릭 엔드포인트, 그리고 번들 처리, 블록스토어 리플레이,
/// 슬롯 업데이트와 관련된 모든 백그라운드 서비스를 초기화하고 실행합니다.
///
/// # 인자
/// * `config` - 게이트웨이 설정입니다.
/// * `addr` - gRPC 서버를 바인딩할 소켓 주소입니다.
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
    let block_engine_relayer =
        BlockEngineRelayerSvc::new(packet_tx.clone(), Arc::new(config.clone()));

    let ledger_path = &config.ledger_path;

    let mut bs_opts = BlockstoreOptions::default();
    bs_opts.access_type = AccessType::Secondary;
    let blockstore = Arc::new(Blockstore::open_with_options(
        Path::new(ledger_path),
        bs_opts,
    )?);
    let genesis_config = read_genesis_config(ledger_path)?;

    let snapshot_config = SnapshotConfig {
        full_snapshot_archives_dir: config.snapshot_path.clone(),
        incremental_snapshot_archives_dir: config.snapshot_path,
        ..SnapshotConfig::default()
    };

    let account_paths = vec![PathBuf::from(format!("{}/accounts", ledger_path))];
    let (bank_forks, leader_cache, _snapshots) = bank_forks_utils::load_bank_forks(
        &genesis_config,
        &blockstore,
        account_paths,
        &snapshot_config,
        &ProcessOptions::default(),
        None,
        None,
        None,
        Arc::new(AtomicBool::new(false)),
    )?;

    let rpc_url = config.rpc_url.clone();
    let leader_cache = Arc::new(leader_cache);
    let shared_bank = Arc::new(tokio::sync::RwLock::new(
        bank_forks.read().unwrap().root_bank(),
    ));

    blockstore_processor::process_blockstore_from_root(
        &blockstore,
        &bank_forks,
        &leader_cache,
        &mk_process_opts(),
        None,
        None,
        None,
    )?;

    let bundle_processor = BundleProcessor::new(
        bundle_tx.subscribe(),
        packet_tx.subscribe(),
        bundle_result_tx.clone(),
        rpc_client.clone(),
        bundle_accepted_counter,
        bundle_rejected_counter,
        bundle_processing_duration,
        validator_identity.clone(),
        _for_leader_tx,
        shared_bank.clone(),
    );
    tokio::spawn(replay_blockstore_loop(
        bank_forks.clone(),
        leader_cache.clone(),
        blockstore.clone(),
    ));
    tokio::spawn(bundle_processor.run());
    tokio::spawn(slot_update_loop(
        shared_bank.clone(),
        bank_forks.clone(),
        leader_cache.clone(),
        rpc_url,
    ));

    let for_leader_queue =
        ForLeaderQueue::new(for_leader_rx, config.tpu_ip.clone(), config.tpu_port);
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
    signature::EncodableKey, signature::Keypair, signature::Signature, signer::Signer,
    transaction::VersionedTransaction,
};
use tokio_util::time::DelayQueue;

/// 들어오는 번들과 패킷을 처리합니다.
struct BundleProcessor {
    bundle_rx: tokio::sync::broadcast::Receiver<BundleUuid>,
    packet_rx: tokio::sync::broadcast::Receiver<be_proto::block_engine::SubscribePacketsResponse>,
    bundle_result_tx: tokio::sync::broadcast::Sender<BundleResult>,
    rpc_client: Arc<RpcClient>,
    bundle_accepted_counter: IntCounter,
    bundle_rejected_counter: IntCounter,
    bundle_processing_duration: Histogram,
    validator_identity: String,
    for_leader_tx: tokio::sync::mpsc::Sender<BundleUuid>,
    bank: Arc<tokio::sync::RwLock<Arc<Bank>>>,
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
        for_leader_tx: tokio::sync::mpsc::Sender<BundleUuid>,
        bank: Arc<tokio::sync::RwLock<Arc<Bank>>>,
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
            for_leader_tx,
            bank,
        }
    }

    /// 주어진 뱅크에 대해 번들을 시뮬레이션합니다.
    ///
    /// 부모 뱅크로부터 자식 뱅크를 생성하고 번들 안의 모든 트랜잭션을 처리합니다.
    /// 이 시뮬레이션은 부모 뱅크의 상태에 영향을 주지 않으므로 안전합니다.
    fn simulate_bundle_bank(
        parent: Arc<Bank>,
        bundle: &be_proto::bundle::Bundle,
    ) -> Result<(), (Signature, String)> {
        let child =
            Bank::new_from_parent(parent.clone(), &parent.collector_id(), parent.slot() + 1);

        for pkt in &bundle.packets {
            let tx: VersionedTransaction = bincode::deserialize(&pkt.data).unwrap();
            let sig = tx.signatures[0];

            let legacy = vtx_to_legacy(&tx).map_err(|e| (sig, e.to_string()))?;
            if let Err(e) = child.process_transaction(&legacy) {
                return Err((sig, format!("{:?}", e)));
            }
        }
        Ok(())
    }

    /// 번들 프로세서의 메인 루프입니다.
    ///
    /// 들어오는 번들을 수신하고, 시뮬레이션하며, 그 결과를 전송합니다.
    async fn run(mut self) {
        loop {
            tokio::select! {
                bundle_uuid = self.bundle_rx.recv() => {
                    match bundle_uuid {
                        Ok(bundle_uuid) => {
                            tracing::info!("Processing bundle: {}", bundle_uuid.uuid);
                            let timer = self.bundle_processing_duration.start_timer();

                            let bank_read = self.bank.read().await;
                            let parent_bank = bank_read.clone();
                            let result = match Self::simulate_bundle_bank(parent_bank, &bundle_uuid.bundle.as_ref().unwrap()) {
                                Ok(_) => {
                                    self.bundle_accepted_counter.inc();
                                    tracing::info!("Bundle simulation successful for bundle: {}", bundle_uuid.uuid);
                                    if let Err(e) = self.for_leader_tx.send(bundle_uuid.clone()).await {
                                        tracing::error!("Failed to send bundle to for_leader_queue: {:?}", e);
                                    }
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
                        },
                        Err(e) => tracing::error!("Error receiving packet batch: {:?}", e),
                    }
                }
            }
        }
    }
}

/// 현재 리더에게 전송될 준비가 된 번들을 위한 큐입니다.
struct ForLeaderQueue {
    bundle_rx: tokio::sync::mpsc::Receiver<BundleUuid>,
    tpu_ip: String,
    tpu_port: u16,
}

impl ForLeaderQueue {
    fn new(
        bundle_rx: tokio::sync::mpsc::Receiver<BundleUuid>,
        tpu_ip: String,
        tpu_port: u16,
    ) -> Self {
        Self {
            bundle_rx,
            tpu_ip,
            tpu_port,
        }
    }

    /// 리더를 위한 큐의 메인 루프입니다.
    ///
    /// 번들을 수신하고 설정된 TPU 주소로 패킷을 전달합니다.
    async fn run(mut self) {
        loop {
            tokio::select! {
                bundle_uuid_option = self.bundle_rx.recv() => {
                    if let Some(bundle_uuid) = bundle_uuid_option {
                        tracing::info!("Bundle received for leader queue: {}", bundle_uuid.uuid);
                        let tpu_addr = format!("{}:{}", self.tpu_ip, self.tpu_port);
                        let socket = match tokio::net::UdpSocket::bind("0.0.0.0:0").await {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::error!("Failed to bind UDP socket for sending to TPU: {:?}", e);
                                continue;
                            }
                        };

                        if let Some(bundle) = bundle_uuid.bundle {
                            for packet in bundle.packets {
                                match socket.send_to(&packet.data, &tpu_addr).await {
                                    Ok(bytes_sent) => {
                                        tracing::info!("Sent {} bytes to TPU at {}", bytes_sent, tpu_addr);
                                    },
                                    Err(e) => {
                                        tracing::error!("Failed to send packet to TPU: {:?}", e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

use ed25519_dalek::{SigningKey, Verifier, VerifyingKey};
use jsonwebtoken::{decode, encode, Header as JwtHeader, Validation};
use rand::rngs::OsRng;
use std::collections::HashSet;
use tokio::sync::Mutex;

/// JWT 생성 및 검증을 담당하는 인증 서비스입니다.
struct AuthSvc {
    /// 재전송 공격을 방지하기 위한 보류 중인 챌린지 문자열 집합입니다.
    pending: Arc<Mutex<HashSet<String>>>,
    /// JWT 발급에 사용되는 서명 키입니다.
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

/// JWT의 클레임을 정의합니다.
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    /// 토큰의 주체 (서처의 공개 키)입니다.
    sub: String,
    /// 토큰의 만료 시간입니다.
    exp: u64,
    /// 토큰이 발급된 시간입니다.
    iat: u64,
}

#[async_trait]
impl AuthService for AuthSvc {
    /// 클라이언트가 서명할 챌린지를 생성합니다.
    async fn generate_auth_challenge(
        &self,
        req: Request<GenerateAuthChallengeRequest>,
    ) -> Result<Response<GenerateAuthChallengeResponse>, Status> {
        use base64::{engine::general_purpose, Engine as _};
        use solana_sdk::pubkey::Pubkey;

        let raw = req.into_inner().pubkey;
        if raw.len() != 32 {
            return Err(Status::invalid_argument("pubkey must be 32 bytes"));
        }
        let b58 = Pubkey::new_from_array(raw.clone().try_into().unwrap()).to_string();

        let token: [u8; 32] = rand::random();
        let token_b64 = general_purpose::STANDARD_NO_PAD.encode(token);

        let challenge_str = format!("{b58}-{token_b64}");

        self.pending.lock().await.insert(challenge_str.clone());

        Ok(Response::new(GenerateAuthChallengeResponse {
            challenge: token_b64,
        }))
    }

    /// 클라이언트가 서명한 챌린지를 검증한 후 JWT를 생성합니다.
    async fn generate_auth_tokens(
        &self,
        req: Request<GenerateAuthTokensRequest>,
    ) -> Result<Response<GenerateAuthTokensResponse>, Status> {
        use base64::{engine::general_purpose, Engine as _};
        use solana_sdk::bs58;

        let inner = req.into_inner();
        let full_chal = inner.challenge;
        let client_pk = inner.client_pubkey;
        let sig_field = inner.signed_challenge;

        {
            let mut set = self.pending.lock().await;
            if !set.remove(&full_chal) {
                return Err(Status::unauthenticated("Unknown or expired challenge"));
            }
        }

        let (b58_pk, _token) = full_chal
            .split_once('-')
            .ok_or_else(|| Status::invalid_argument("Malformed challenge"))?;
        let expected_pk = bs58::decode(b58_pk)
            .into_vec()
            .map_err(|_| Status::invalid_argument("Invalid pubkey in challenge"))?;
        if expected_pk != client_pk {
            return Err(Status::invalid_argument("pubkey mismatch"));
        }

        let sig_bytes = decode_sig(sig_field)?;
        let signature = ed25519_dalek::Signature::from_slice(&sig_bytes)
            .map_err(|_| Status::invalid_argument("bad signature len"))?;

        let pk_arr: [u8; 32] = client_pk
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("pubkey len"))?;
        VerifyingKey::from_bytes(&pk_arr)
            .map_err(|_| Status::invalid_argument("bad pubkey"))?
            .verify(full_chal.as_bytes(), &signature)
            .map_err(|_| Status::unauthenticated("Signature verification failed"))?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let access_token_expires_at = now + 600; // 10분
        let refresh_token_expires_at = now + 3600; // 1시간

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
            .to_pkcs8_der()
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

    /// 유효한 리프레시 토큰을 사용하여 액세스 토큰을 갱신합니다.
    async fn refresh_access_token(
        &self,
        req: Request<RefreshAccessTokenRequest>,
    ) -> Result<Response<RefreshAccessTokenResponse>, Status> {
        let inner_req = req.into_inner();
        let refresh_token_value = inner_req.refresh_token;

        let der_pub = self
            .signing
            .verifying_key()
            .to_public_key_der()
            .map_err(|_| Status::internal("pkcs8 pub encode"))?;

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
        let access_token_expires_at = now + 600; // 10분

        let access_claims = Claims {
            sub: pubkey.clone(),
            exp: access_token_expires_at,
            iat: now,
        };

        let der_priv = self
            .signing
            .to_pkcs8_der()
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

/// 블록 엔진의 밸리데이터 대상 서비스입니다.
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
    /// 블록 빌더의 수수료 정보를 반환합니다.
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

    /// 번들 스트림을 구독합니다.
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

    /// 패킷 스트림을 구독합니다.
    async fn subscribe_packets(
        &self,
        _req: Request<be_proto::block_engine::SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        let stream = BroadcastStream::new(self.packet_tx.subscribe())
            .map_err(|e| Status::internal(format!("Packet stream error: {}", e)));
        Ok(Response::new(Box::pin(stream)))
    }
}

/// 블록 엔진의 릴레이어 대상 서비스입니다.
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
    /// TPU 및 TPU-forward 설정을 반환합니다.
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

    /// 릴레이어로부터 패킷 스트림을 구독합니다.
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

            let mut buf = [0u8; 1232];
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((len, addr)) => {
                        let packet =
                            solana_sdk::packet::Packet::from_data(Some(&addr), &buf[..len])
                                .unwrap();
                        let packet_batch = PacketBatch {
                            packets: vec![WrappedSolanaPacket(packet).into()],
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

/// 블록 엔진의 릴레이어 서비스입니다.
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
    type SubscribeAccountsOfInterestStream =
        Pin<Box<dyn Stream<Item = Result<AccountsOfInterestUpdate, Status>> + Send>>;

    /// 관심 계정 스트림을 구독합니다.
    async fn subscribe_accounts_of_interest(
        &self,
        _req: Request<AccountsOfInterestRequest>,
    ) -> Result<Response<Self::SubscribeAccountsOfInterestStream>, Status> {
        tracing::debug!("SubscribeAccountsOfInterest RPC called.");
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let update = AccountsOfInterestUpdate { accounts: vec![] };
            if tx.send(Ok(update)).await.is_err() {
                return;
            }

            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                let update = AccountsOfInterestUpdate { accounts: vec![] };
                if tx.send(Ok(update)).await.is_err() {
                    break;
                }
            }
        });
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    type SubscribeProgramsOfInterestStream =
        Pin<Box<dyn Stream<Item = Result<ProgramsOfInterestUpdate, Status>> + Send>>;

    /// 관심 프로그램 스트림을 구독합니다.
    async fn subscribe_programs_of_interest(
        &self,
        _req: Request<ProgramsOfInterestRequest>,
    ) -> Result<Response<Self::SubscribeProgramsOfInterestStream>, Status> {
        tracing::debug!("SubscribeProgramsOfInterest RPC called.");
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let programs_of_interest = self.config.programs_of_interest.clone();

        let initial = ProgramsOfInterestUpdate {
            programs: programs_of_interest.clone(),
        };
        tx.send(Ok(initial)).await.ok();

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

    type StartExpiringPacketStreamStream =
        Pin<Box<dyn Stream<Item = Result<StartExpiringPacketStreamResponse, Status>> + Send>>;

    /// 만료되는 패킷 스트림을 시작합니다.
    async fn start_expiring_packet_stream(
        &self,
        mut req: Request<tonic::Streaming<PacketBatchUpdate>>,
    ) -> Result<Response<Self::StartExpiringPacketStreamStream>, Status> {
        tracing::debug!("StartExpiringPacketStream RPC called.");
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
                                let packet_batch_id = uuid::Uuid::new_v4().to_string();
                                let expiry_duration = tokio::time::Duration::from_secs(2);
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
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    let mut expiring_packets = expiring_packets_clone.lock().await;
                    while let Some(expired) =
                        future::poll_fn(|cx| expiring_packets.poll_expired(cx)).await
                    {
                        tracing::info!("Packet batch expired: {}", expired.into_inner());
                    }
                }
            }
        });
        tokio::spawn(async move {
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
}

/// 블록 엔진의 서처 대상 서비스입니다.
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

    /// 번들 결과 스트림을 구독합니다.
    async fn subscribe_bundle_results(
        &self,
        _req: Request<be_proto::searcher::SubscribeBundleResultsRequest>,
    ) -> Result<Response<Self::SubscribeBundleResultsStream>, Status> {
        let stream =
            tokio_stream::wrappers::BroadcastStream::new(self.bundle_result_tx.subscribe())
                .map_err(|e| Status::internal(format!("Bundle result stream error: {}", e)));
        Ok(Response::new(Box::pin(stream)))
    }

    /// 서처로부터 번들을 수신하고, 유효성을 검사한 후 처리를 위해 브로드캐스트합니다.
    async fn send_bundle(
        &self,
        req: Request<be_proto::searcher::SendBundleRequest>,
    ) -> Result<Response<be_proto::searcher::SendBundleResponse>, Status> {
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

        let mut total_tip_lamports = 0;
        let mut total_size = 0;
        for packet in &bundle.packets {
            total_tip_lamports += 5000; // 더미 팁
            total_size += packet.data.len();
        }
        tracing::info!(
            "Calculated total tip: {} lamports, total size: {} bytes",
            total_tip_lamports,
            total_size
        );

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

    /// 다음 스케줄된 리더를 반환합니다.
    async fn get_next_scheduled_leader(
        &self,
        _req: Request<be_proto::searcher::NextScheduledLeaderRequest>,
    ) -> Result<Response<be_proto::searcher::NextScheduledLeaderResponse>, Status> {
        let current_slot = self.rpc_client.get_slot().await.unwrap_or(0);
        let next_leader_slot = current_slot + 2;
        Ok(Response::new(
            be_proto::searcher::NextScheduledLeaderResponse {
                current_slot,
                next_leader_slot,
                next_leader_identity: "validator_A_identity".to_string(),
                next_leader_region: "us-east-1".to_string(),
            },
        ))
    }

    /// 연결된 리더 목록을 반환합니다.
    async fn get_connected_leaders(
        &self,
        _req: Request<be_proto::searcher::ConnectedLeadersRequest>,
    ) -> Result<Response<be_proto::searcher::ConnectedLeadersResponse>, Status> {
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

    /// 지역별로 그룹화된 연결된 리더 목록을 반환합니다.
    async fn get_connected_leaders_regioned(
        &self,
        _req: Request<be_proto::searcher::ConnectedLeadersRegionedRequest>,
    ) -> Result<Response<be_proto::searcher::ConnectedLeadersRegionedResponse>, Status> {
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

    /// 팁 계정 목록을 반환합니다.
    async fn get_tip_accounts(
        &self,
        _req: Request<be_proto::searcher::GetTipAccountsRequest>,
    ) -> Result<Response<be_proto::searcher::GetTipAccountsResponse>, Status> {
        Ok(Response::new(be_proto::searcher::GetTipAccountsResponse {
            accounts: vec![
                "TipAccount11111111111111111111111111111111".to_string(),
                "TipAccount22222222222222222222222222222222".to_string(),
            ],
        }))
    }

    /// 사용 가능한 지역 목록을 반환합니다.
    async fn get_regions(
        &self,
        _req: Request<be_proto::searcher::GetRegionsRequest>,
    ) -> Result<Response<be_proto::searcher::GetRegionsResponse>, Status> {
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

/// 다양한 형식(raw, bs58, base64, hex)의 서명을 디코딩합니다.
fn decode_sig(field: Vec<u8>) -> Result<Vec<u8>, Status> {
    use base64::{engine::general_purpose, Engine as _};
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

/// 주기적으로 블록스토어를 리플레이하여 네트워크와 동기화합니다.
async fn replay_blockstore_loop(
    bank_forks: Arc<std::sync::RwLock<BankForks>>,
    leader_cache: Arc<LeaderScheduleCache>,
    blockstore: Arc<Blockstore>,
) {
    loop {
        if let Err(e) = catch_up(&blockstore) {
            tracing::warn!("rocks catch‑up failed: {e}");
        }

        if let Err(e) = blockstore_processor::process_blockstore_from_root(
            &blockstore,
            &bank_forks,
            &leader_cache,
            &mk_process_opts(),
            None,
            None,
            None,
        ) {
            tracing::error!("process_blockstore failed: {e:?}");
        }

        {
            let r = bank_forks.read().unwrap();
            tracing::info!(
                "progress: root={}, tip={}, banks={}",
                r.root(),
                r.working_bank().slot(),
                r.banks().len()
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// 슬롯 업데이트를 구독하고 `BankForks`의 루트를 진행시킵니다.
async fn slot_update_loop(
    shared_bank: Arc<tokio::sync::RwLock<Arc<Bank>>>,
    bank_forks: Arc<std::sync::RwLock<BankForks>>,
    leader_cache: Arc<LeaderScheduleCache>,
    rpc_http: String,
) {
    let ws_url = rpc_http
        .replace("http://", "ws://")
        .replace("https://", "wss://")
        .replace(":8899", ":8900");

    let client = match solana_client::nonblocking::pubsub_client::PubsubClient::new(&ws_url).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("pubsub connect fail: {e:?}");
            return;
        }
    };
    let (mut updates, _sub) = match client.slot_updates_subscribe().await {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("slot subscribe fail: {e:?}");
            return;
        }
    };

    use futures_util::StreamExt;
    while let Some(update) = updates.next().await {
        if let solana_client::rpc_response::SlotUpdate::Root { slot, .. } = update {
            if bank_forks.read().unwrap().root() == slot {
                tracing::debug!("root {slot} already set, skipping");
                continue;
            }

            if bank_forks.read().unwrap().get(slot).is_none() {
                tracing::info!("root {slot} skipped – Bank not present yet");
                continue;
            }

            let tip_bank = bank_forks.read().unwrap().working_bank();
            {
                let mut w = shared_bank.write().await;
                *w = tip_bank;
            }
            tracing::info!(
                "Shared bank moved to working slot {}",
                shared_bank.read().await.slot()
            );
        }
    }
    tracing::warn!("slot_updates stream closed");
}

/// 지정된 원장 경로에서 제네시스 설정을 읽어옵니다.
fn read_genesis_config(ledger_path: &str) -> anyhow::Result<GenesisConfig> {
    let genesis_path = Path::new(ledger_path).join("genesis.bin");
    let file = std::fs::File::open(genesis_path)?;
    Ok(bincode::deserialize_from(file)?)
}

/// `VersionedTransaction`을 레거시 `Transaction`으로 변환합니다.
fn vtx_to_legacy(v: &VersionedTransaction) -> anyhow::Result<Transaction> {
    let legacy = v
        .clone()
        .into_legacy_transaction()
        .ok_or_else(|| anyhow!("cannot down-cast to legacy tx"))?;
    legacy.sanitize()?;
    Ok(legacy)
}

fn catch_up(blockstore: &Blockstore) -> anyhow::Result<()> {
    blockstore
        .rocks() // &rocksdb::DB 가 반환
        .try_catch_up_with_primary() // RocksDB 메서드
        .map_err(|e| anyhow::anyhow!("catch_up: {e}"))
}

fn mk_process_opts() -> ProcessOptions {
    let mut o = ProcessOptions::default();

    o.run_verification = false;
    o.allow_dead_slots = true; // 있으면 켜두면 안전

    o
}
