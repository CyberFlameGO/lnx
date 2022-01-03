use poem::Result;
use poem::web::Data;
use poem_openapi::{Object, OpenApi, ApiResponse};
use poem_openapi::param::{Path, Query};
use poem_openapi::payload::Json;

use serde::Deserialize;

use crate::auth::{permissions, TokenData};
use crate::helpers::{LnxRequest, LnxResponse};
use crate::responders::json_response;
use crate::state::State;
use crate::{abort, bad_request, get_or_400, json, unauthorized};
use crate::utils::Detailed;


#[derive(ApiResponse)]
pub enum CreateTokenResponse {
    /// The request was successful
    #[oai(status = 200)]
    Ok(Json<TokenData>),

    /// The server failed to deserialize and validate the payload.
    #[oai(status = 422)]
    DeserializationError(()),

    /// You lack the permissions to perform this operation.
    #[allow(unused)]
    #[oai(status = 401)]
    Unauthorized,

    /// The request is missing a required element. E.g. Payload, parameter, etc...
    #[allow(unused)]
    #[oai(status = 400)]
    BadRequest,
}


#[derive(ApiResponse)]
pub enum RevokeTokenResponse {
    /// The request was successful
    #[oai(status = 200)]
    Ok(Json<Detailed>),

    /// You lack the permissions to perform this operation.
    #[allow(unused)]
    #[oai(status = 401)]
    Unauthorized,

    /// The request is missing a required element. E.g. Payload, parameter, etc...
    #[allow(unused)]
    #[oai(status = 400)]
    BadRequest,
}


pub struct AuthApi;


#[OpenApi]
impl AuthApi {
    /// Create Token
    ///
    /// Creates a new 64 character access token with a given set of metadata.
    #[oai(path = "/auth", method = "post")]
    pub async fn create_token(
        &self,
        payload: Json<CreateTokenPayload>,
        state: Data<&State>,
    ) -> Result<CreateTokenResponse> {
        let data = state.auth.create_token(
            payload.0.permissions,
            payload.0.user,
            payload.0.description,
            payload.0.allowed_indexes,
        );

        let storage = state.storage.clone();
        state.auth.commit(storage).await?;

        Ok(CreateTokenResponse::Ok(todo!()))
    }

    /// Revoke All Tokens
    ///
    /// Revoke all access tokens.
    ///
    /// ### WARNING:
    /// This is absolutely only designed for use in an emergency.
    /// Running this will revoke all tokens including the super user key, run this at your own risk.
    #[oai(path = "/auth", method = "delete")]
    pub async fn revoke_all_tokens(
        &self,
        state: Data<&State>,
    ) -> Result<RevokeTokenResponse> {
        state.auth.revoke_all_tokens();

        let storage = state.storage.clone();
        state.auth.commit(storage).await?;

        Ok(RevokeTokenResponse::Ok(Json(Detailed::from("Successfully revoked all tokens"))))
    }

    /// Revoke Token
    ///
    /// Revokes a given token, any requests after this with the given token will be rejected.
    #[oai(path = "/auth/:token/revoke", method = "post")]
    pub async fn revoke_token(
        &self,
        token: Path<String>,
        state: Data<&State>,
    ) -> Result<RevokeTokenResponse> {
        state.auth.revoke_token(&token.0);

        let storage = state.storage.clone();
        state.auth.commit(storage).await?;

        Ok(RevokeTokenResponse::Ok(Json(Detailed::from("Successfully revoked token"))))
    }
}


/// A set of metadata to associate with a access token.
#[derive(Object)]
struct CreateTokenPayload {
    /// The permissions of the token.
    permissions: usize,

    /// An optional identifier for a user.
    user: Option<String>,

    /// An optional description for the given token.
    description: Option<String>,

    /// An optional set of indexes the user is allowed to access.
    ///
    /// If None the user can access all tokens.
    allowed_indexes: Option<Vec<String>>,
}

/// A middleware that checks the user accessing the endpoint has
/// the required permissions.
///
/// If authorization is disabled then this does no checks.
pub(crate) async fn check_permissions(req: LnxRequest) -> Result<LnxRequest> {
    let state = req.data::<State>().expect("get state");

    if !state.auth.enabled() {
        return Ok(req);
    }

    let auth = req.headers().get("Authorization");
    let token = match auth {
        Some(auth) => auth
            .to_str()
            .map_err(|_| LnxError::BadRequest("invalid token provided"))?,
        None => return unauthorized!("missing authorization header"),
    };

    let data = match state.auth.get_token_data(&token) {
        None => return unauthorized!("invalid token provided"),
        Some(v) => v,
    };

    let required_permissions: usize;
    let path = req.uri().path();
    if path.starts_with("/auth") {
        required_permissions = permissions::MODIFY_AUTH;
    } else if path == "/indexes" {
        required_permissions = permissions::MODIFY_ENGINE;
    } else if path.starts_with("/indexes") {
        if path.ends_with("/search") {
            required_permissions = permissions::SEARCH_INDEX;
        } else if path.ends_with("/stopwords") {
            required_permissions = permissions::MODIFY_STOP_WORDS;
        } else {
            required_permissions = permissions::MODIFY_DOCUMENTS
        }
    } else {
        // A safe default is to return a 404.
        return abort!(404, "unknown route.");
    }

    if !data.has_permissions(required_permissions) {
        return unauthorized!("you lack permissions to perform this request");
    }

    Ok(req)
}

/// Revoke all access tokens.
///
/// # WARNING:
///     This is absolutely only designed for use in an emergency.
///     Running this will revoke all tokens including the super user key,
///     run this at your own risk
pub async fn revoke_all_tokens(req: LnxRequest) -> LnxResponse {
    let state = req.data::<State>().expect("get state");
    state.auth.revoke_all_tokens();

    let storage = state.storage.clone();
    state.auth.commit(storage).await?;

    json_response(200, "token revoked.")
}

pub async fn edit_token(mut req: LnxRequest) -> LnxResponse {
    let body: CreateTokenPayload = json!(req.body_mut());

    let state = req.data::<State>().expect("get state");
    let token = get_or_400!(req.param("token"));

    let data = state.auth.update_token(
        &token,
        body.permissions,
        body.user,
        body.description,
        body.allowed_indexes,
    );

    let data = match data {
        None => return bad_request!("this token does not exist"),
        Some(d) => d,
    };

    let storage = state.storage.clone();
    state.auth.commit(storage).await?;

    json_response(200, data.as_ref())
}
