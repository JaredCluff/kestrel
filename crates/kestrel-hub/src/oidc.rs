// crates/kestrel-hub/src/oidc.rs
//
// Phase 11b OIDC integration. Operators configure one or more OIDC
// providers in kestrel-policy.toml; the dashboard exposes /oauth/<provider>/login
// which initiates the auth-code flow and /oauth/<provider>/callback which
// validates the ID token, looks the subject up against the configured
// users, and issues a session cookie if matched.
//
// AUTHOR CAVEAT: written without runtime testing against a real
// provider. The flow follows the openidconnect 3.x API and standard
// PKCE auth-code best practices. Hardcoded provider profiles for
// Google + GitHub make those one-line operator configs; arbitrary
// providers can be configured via raw issuer URLs.

use anyhow::Context;
use openidconnect::core::{CoreClient, CoreProviderMetadata, CoreResponseType};
use openidconnect::{
    AuthenticationFlow, AuthorizationCode, ClientId, ClientSecret, CsrfToken,
    IssuerUrl, Nonce, PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, Scope,
    TokenResponse,
};

#[derive(Debug, Clone, serde::Deserialize)]
pub struct OidcProvider {
    /// Operator-chosen label, used in URL paths and policy rules
    /// (e.g. "google", "github", "okta-corp").
    pub name: String,
    /// Full issuer URL. Hardcoded shortcuts: "google" expands to
    /// https://accounts.google.com, "github" expands to
    /// https://token.actions.githubusercontent.com.
    pub issuer: String,
    pub client_id: String,
    pub client_secret: String,
    /// Where the OIDC provider redirects after login. Must be a
    /// registered redirect URI on the provider side.
    pub redirect_url: String,
    /// Claim to use as the user_id in policy lookups. "sub" is the
    /// safe default; "email" is common but requires the email claim.
    #[serde(default = "default_user_id_claim")]
    pub user_id_claim: String,
}

fn default_user_id_claim() -> String { "sub".into() }

#[derive(Debug, Clone, serde::Serialize)]
pub struct OidcLoginInit {
    pub authorize_url: String,
    pub csrf_state: String,
    pub pkce_verifier: String,
    pub nonce: String,
}

/// Build the provider's auth-code-with-PKCE URL. Caller persists
/// csrf_state + pkce_verifier + nonce in a short-lived cookie/session
/// to validate on the callback.
pub async fn begin_login(provider: &OidcProvider) -> anyhow::Result<OidcLoginInit> {
    let issuer = expand_issuer(&provider.issuer);
    let issuer_url = IssuerUrl::new(issuer.clone())
        .with_context(|| format!("invalid issuer URL: {}", issuer))?;
    let metadata = CoreProviderMetadata::discover_async(issuer_url, async_http_client).await
        .with_context(|| format!("OIDC discovery failed for {}", provider.issuer))?;
    let client = CoreClient::from_provider_metadata(
        metadata,
        ClientId::new(provider.client_id.clone()),
        Some(ClientSecret::new(provider.client_secret.clone())),
    )
    .set_redirect_uri(RedirectUrl::new(provider.redirect_url.clone())?);

    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let (auth_url, csrf, nonce) = client
        .authorize_url(
            AuthenticationFlow::<CoreResponseType>::AuthorizationCode,
            CsrfToken::new_random,
            Nonce::new_random,
        )
        .add_scope(Scope::new("openid".into()))
        .add_scope(Scope::new("email".into()))
        .add_scope(Scope::new("profile".into()))
        .set_pkce_challenge(pkce_challenge)
        .url();

    Ok(OidcLoginInit {
        authorize_url: auth_url.to_string(),
        csrf_state: csrf.secret().to_string(),
        pkce_verifier: pkce_verifier.secret().to_string(),
        nonce: nonce.secret().to_string(),
    })
}

/// Exchange the authorization code for an ID token, validate it
/// against the discovered metadata + the stored nonce, and return
/// the user_id claim. Errors on:
///   - code exchange failure
///   - missing ID token in response
///   - nonce mismatch
///   - signature verification failure
pub async fn complete_login(
    provider: &OidcProvider,
    code: String,
    pkce_verifier: String,
    expected_nonce: String,
) -> anyhow::Result<String> {
    let issuer = expand_issuer(&provider.issuer);
    let issuer_url = IssuerUrl::new(issuer)?;
    let metadata = CoreProviderMetadata::discover_async(issuer_url, async_http_client).await?;
    let client = CoreClient::from_provider_metadata(
        metadata,
        ClientId::new(provider.client_id.clone()),
        Some(ClientSecret::new(provider.client_secret.clone())),
    )
    .set_redirect_uri(RedirectUrl::new(provider.redirect_url.clone())?);

    let token_response = client
        .exchange_code(AuthorizationCode::new(code))
        .set_pkce_verifier(PkceCodeVerifier::new(pkce_verifier))
        .request_async(async_http_client)
        .await?;

    let id_token = token_response
        .id_token()
        .ok_or_else(|| anyhow::anyhow!("OIDC response missing ID token"))?;
    let id_token_verifier = client.id_token_verifier();
    let nonce = Nonce::new(expected_nonce);
    let claims = id_token.claims(&id_token_verifier, &nonce)?;
    let user_id = match provider.user_id_claim.as_str() {
        "sub" => claims.subject().to_string(),
        "email" => claims
            .email()
            .ok_or_else(|| anyhow::anyhow!("email claim missing"))?
            .to_string(),
        other => anyhow::bail!("unsupported user_id_claim: {}", other),
    };
    Ok(user_id)
}

fn expand_issuer(s: &str) -> String {
    match s {
        "google" => "https://accounts.google.com".into(),
        "github" => "https://token.actions.githubusercontent.com".into(),
        other => other.into(),
    }
}

/// Adapter for the openidconnect 3.x HTTP client. Delegates directly
/// to the crate's own reqwest-backed async client; the return type
/// is whatever openidconnect's reqwest::async_http_client returns,
/// which the auth-code helpers accept directly.
use openidconnect::reqwest::async_http_client;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_issuer_shortcuts() {
        assert_eq!(expand_issuer("google"), "https://accounts.google.com");
        assert_eq!(expand_issuer("github"), "https://token.actions.githubusercontent.com");
        assert_eq!(expand_issuer("https://my.idp.example/"), "https://my.idp.example/");
    }
}
