//! HTTP/1.1 WebSocket handshake validation shared by native and event ingress.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use http::HeaderMap;
use sha1::{Digest as _, Sha1};

#[derive(Clone, Debug)]
pub(crate) struct Handshake {
    pub accept: String,
    pub protocols: Vec<String>,
}
fn single<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, ()> {
    let mut values = headers.get_all(name).iter();
    let value = values
        .next()
        .map(|value| value.to_str().map_err(|_| ()))
        .transpose()?;
    if values.next().is_some() {
        return Err(());
    }
    Ok(value)
}
fn token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
}
impl Handshake {
    pub(crate) fn parse(headers: &HeaderMap, allowed_origins: &[String]) -> Result<Self, ()> {
        if !single(headers, "upgrade")?.is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
            || single(headers, "sec-websocket-version")? != Some("13")
        {
            return Err(());
        }
        let connection = headers
            .get_all("connection")
            .iter()
            .map(|value| value.to_str().map_err(|_| ()))
            .collect::<Result<Vec<_>, _>>()?;
        if !connection.iter().any(|value| {
            value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
        }) {
            return Err(());
        }
        if single(headers, "origin")?
            .is_some_and(|origin| !allowed_origins.iter().any(|allowed| allowed == origin))
        {
            return Err(());
        }
        let key = single(headers, "sec-websocket-key")?.ok_or(())?;
        let decoded = STANDARD.decode(key).map_err(|_| ())?;
        if decoded.len() != 16 || STANDARD.encode(decoded) != key {
            return Err(());
        }
        let protocols = single(headers, "sec-websocket-protocol")?
            .map(|value| {
                value
                    .split(',')
                    .map(|part| part.trim().to_owned())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if protocols.len() > 16
            || protocols.iter().any(|value| !token(value))
            || protocols
                .iter()
                .enumerate()
                .any(|(index, value)| protocols[..index].contains(value))
        {
            return Err(());
        }
        let accept = STANDARD.encode(Sha1::digest(
            format!("{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11").as_bytes(),
        ));
        Ok(Self { accept, protocols })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in [
            ("upgrade", "websocket"),
            ("connection", "keep-alive, Upgrade"),
            ("sec-websocket-version", "13"),
            ("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ=="),
        ] {
            headers.insert(
                http::HeaderName::from_static(name),
                http::HeaderValue::from_static(value),
            );
        }
        headers
    }
    #[test]
    fn rfc_handshake_and_explicit_origin_policy() {
        let mut headers = headers();
        assert_eq!(
            Handshake::parse(&headers, &[]).unwrap().accept,
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
        headers.insert("origin", "https://client.invalid".parse().unwrap());
        assert!(Handshake::parse(&headers, &[]).is_err());
        assert!(Handshake::parse(&headers, &["https://client.invalid".into()]).is_ok());
    }
    #[test]
    fn rejects_duplicate_keys_and_ambiguous_protocols() {
        let mut headers = headers();
        headers.append(
            "sec-websocket-key",
            "dGhlIHNhbXBsZSBub25jZQ==".parse().unwrap(),
        );
        assert!(Handshake::parse(&headers, &[]).is_err());
        headers.remove("sec-websocket-key");
        headers.insert(
            "sec-websocket-key",
            "dGhlIHNhbXBsZSBub25jZQ==".parse().unwrap(),
        );
        for value in ["chat,chat", "chat,", "bad protocol"] {
            headers.insert("sec-websocket-protocol", value.parse().unwrap());
            assert!(Handshake::parse(&headers, &[]).is_err());
        }
    }
}
