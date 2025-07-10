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
};
use be_proto::bundle::BundleUuid;

pub async fn run(addr: std::net::SocketAddr) -> anyhow::Result<()> {
    // 브로드캐스트 채널: stub 데이터
    let (bundle_tx, _) = tokio::sync::broadcast::channel(128);
    let (packet_tx, _) = tokio::sync::broadcast::channel(128);

    let auth = AuthSvc::default();
    let validator = ValidatorSvc::new(bundle_tx.clone(), packet_tx.clone());

    tracing::info!("Serving gRPC on {addr}");
    Server::builder()
        .add_service(AuthServiceServer::new(auth))
        .add_service(BlockEngineValidatorServer::new(validator))
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


