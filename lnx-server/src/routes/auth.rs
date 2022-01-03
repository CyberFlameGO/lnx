use poem::Result;
use poem::web::Data;
use poem_openapi::{Object, OpenApi, ApiResponse};
use poem_openapi::param::{Path, Query};
use poem_openapi::payload::Json;
use poem_openapi::types::ToJSON;

use serde::Deserialize;

use crate::auth::{permissions, TokenData};
use crate::helpers::{LnxRequest, LnxResponse};
use crate::responders::json_response;
use crate::state::State;
use crate::{abort, bad_request, get_or_400, json, unauthorized};
use crate::utils::Detailed;

#[derive(ApiResponse)]
pub enum TokenResponse<T: ToJSON> {
    /// The request was successful
    #[oai(status = 200)]
    Ok(Json<T>),

    /// You lack the permissions to perform this operation.
    #[allow(unused)]
    #[oai(status = 401)]
    Unauthorized,

    /// The request is missing a required element. E.g. Payload, parameter, etc...
    #[allow(unused)]
    #[oai(status = 400)]
    BadRequest,
}

impl<T: ToJSON> TokenResponse<T> {
    pub fn ok(v: T) -> Self {
        Self::Ok(Json(v))
    }
}


/// A set of metadata to associate with a access token.
#[derive(Object)]
struct TokenPayload {
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


pub struct AuthApi;


#[OpenApi]
impl AuthApi {
    /// Create Token
    ///
    /// Creates a new 64 character access token with a given set of metadata.
    #[oai(path = "/auth", method = "post")]
    pub async fn create_token(
        &self,
        payload: Json<TokenPayload>,
        state: Data<&State>,
    ) -> Result<TokenResponse<TokenData>> {
        let data = state.auth.create_token(
            payload.0.permissions,
            payload.0.user,
            payload.0.description,
            payload.0.allowed_indexes,
        );

        let storage = state.storage.clone();
        state.auth.commit(storage).await?;

        Ok(TokenResponse::ok(data.as_ref().clone()))
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
    ) -> Result<TokenResponse<Detailed>> {
        state.auth.revoke_all_tokens();

        let storage = state.storage.clone();
        state.auth.commit(storage).await?;

        Ok(TokenResponse::ok(Detailed::from("Successfully revoked all tokens")))
    }

    /// Revoke Token
    ///
    /// Revokes a given token, any requests after this with the given token will be rejected.
    #[oai(path = "/auth/:token", method = "delete")]
    pub async fn revoke_token(
        &self,
        token: Path<String>,
        state: Data<&State>,
    ) -> Result<TokenResponse<Detailed>> {
        state.auth.revoke_token(&token.0);

        let storage = state.storage.clone();
        state.auth.commit(storage).await?;

        Ok(TokenResponse::ok(Detailed::from("Successfully revoked token")))
    }

    /// Edit Access Token
    ///
    /// Edits a given token's permissions and metadata.
    /// The payload will replace **ALL** fields which will either set or unset the fields.
    #[oai(path = "/auth/:token", method = "patch")]
    pub async fn edit_token(
        &self,
        token: Path<String>,
        payload: Json<TokenPayload>,
        state: Data<&State>,
    ) -> Result<TokenResponse<TokenData>> {
        let data = state.auth.update_token(
            &token,
            payload.0.permissions,
            payload.0.user,
            payload.0.description,
            payload.0.allowed_indexes,
        );

        let data = match data {
            None => return Ok(TokenResponse::BadRequest),
            Some(d) => d,
        };

        let storage = state.storage.clone();
        state.auth.commit(storage).await?;

        Ok(TokenResponse::ok(data.as_ref().clone()))
    }
}

// /// A middleware that checks the user accessing the endpoint has
// /// the required permissions.
// ///
// /// If authorization is disabled then this does no checks.
// pub async fn check_permissions(req: Re) -> Result<LnxRequest> {
//     let state = req.data::<State>().expect("get state");
//
//     if !state.auth.enabled() {
//         return Ok(req);
//     }
//
//     let auth = req.headers().get("Authorization");
//     let token = match auth {
//         Some(auth) => auth
//             .to_str()
//             .map_err(|_| LnxError::BadRequest("invalid token provided"))?,
//         None => return unauthorized!("missing authorization header"),
//     };
//
//     let data = match state.auth.get_token_data(&token) {
//         None => return unauthorized!("invalid token provided"),
//         Some(v) => v,
//     };
//
//     let required_permissions: usize;
//     let path = req.uri().path();
//     if path.starts_with("/auth") {
//         required_permissions = permissions::MODIFY_AUTH;
//     } else if path == "/indexes" {
//         required_permissions = permissions::MODIFY_ENGINE;
//     } else if path.starts_with("/indexes") {
//         if path.ends_with("/search") {
//             required_permissions = permissions::SEARCH_INDEX;
//         } else if path.ends_with("/stopwords") {
//             required_permissions = permissions::MODIFY_STOP_WORDS;
//         } else {
//             required_permissions = permissions::MODIFY_DOCUMENTS
//         }
//     } else {
//         // A safe default is to return a 404.
//         return abort!(404, "unknown route.");
//     }
//
//     if !data.has_permissions(required_permissions) {
//         return unauthorized!("you lack permissions to perform this request");
//     }
//
//     Ok(req)
// }

