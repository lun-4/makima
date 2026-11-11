use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest, Sha256};

use super::OAuthError;

pub struct PkceChallenge {
    pub verifier: String,
    pub challenge: String,
}

pub fn generate() -> Result<PkceChallenge, OAuthError> {
    let mut buf = [0u8; 32];
    getrandom::fill(&mut buf).map_err(|e| OAuthError::Other(format!("CSPRNG unavailable: {e}")))?;
    let verifier = URL_SAFE_NO_PAD.encode(buf);
    let digest = Sha256::digest(verifier.as_bytes());
    let challenge = URL_SAFE_NO_PAD.encode(digest);
    Ok(PkceChallenge {
        verifier,
        challenge,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc7636_verifier_length_and_uniqueness() {
        let a = generate().unwrap();
        let b = generate().unwrap();
        assert!(a.verifier.len() >= 43 && a.verifier.len() <= 128);
        assert_ne!(a.verifier, b.verifier);
        assert_ne!(a.challenge, b.challenge);
    }
}
