use http::{
    HeaderMap, HeaderName, Method,
    header::{AUTHORIZATION, COOKIE},
};

use crate::SessionCookieConfig;

#[derive(Clone, Debug)]
pub(super) struct SessionCookiePolicy {
    name: String,
    csrf_cookie_name: String,
    csrf_header_name: HeaderName,
}

impl From<&SessionCookieConfig> for SessionCookiePolicy {
    fn from(config: &SessionCookieConfig) -> Self {
        Self {
            name: config.name().to_owned(),
            csrf_cookie_name: config.csrf_cookie_name().to_owned(),
            csrf_header_name: HeaderName::from_bytes(config.csrf_header_name().as_bytes())
                .expect("validated Web Ingress CSRF header name"),
        }
    }
}

impl SessionCookiePolicy {
    pub(super) fn csrf_header_name(&self) -> &HeaderName {
        &self.csrf_header_name
    }
}

#[derive(Debug)]
pub(super) struct CredentialEvidence {
    pub(super) scheme: String,
    pub(super) value: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CredentialRejection {
    BadRequest,
    CsrfForbidden,
}

pub(super) fn select_credential(
    method: &Method,
    headers: &HeaderMap,
    session_cookie: Option<&SessionCookiePolicy>,
) -> Result<Option<CredentialEvidence>, CredentialRejection> {
    let authorization = authorization_credential(headers)?;
    let Some(policy) = session_cookie else {
        return Ok(authorization);
    };
    let cookies = selected_cookies(headers, policy)?;
    let Some(session) = cookies.session else {
        return Ok(authorization);
    };
    if authorization.is_some() {
        return Err(CredentialRejection::BadRequest);
    }
    if !safe_method(method) {
        let csrf_cookie = cookies.csrf.ok_or(CredentialRejection::CsrfForbidden)?;
        let csrf_header = single_header(headers, &policy.csrf_header_name)?
            .ok_or(CredentialRejection::CsrfForbidden)?;
        if csrf_header.is_empty() || !tokens_match(csrf_cookie.as_bytes(), csrf_header.as_bytes()) {
            return Err(CredentialRejection::CsrfForbidden);
        }
    }
    Ok(Some(CredentialEvidence {
        scheme: "session".to_owned(),
        value: session,
    }))
}

fn authorization_credential(
    headers: &HeaderMap,
) -> Result<Option<CredentialEvidence>, CredentialRejection> {
    let Some(value) = single_header(headers, &AUTHORIZATION)? else {
        return Ok(None);
    };
    let Some((scheme, value)) = value.split_once(' ') else {
        return Err(CredentialRejection::BadRequest);
    };
    if scheme.is_empty() || value.is_empty() {
        return Err(CredentialRejection::BadRequest);
    }
    Ok(Some(CredentialEvidence {
        scheme: scheme.to_ascii_lowercase(),
        value: value.to_owned(),
    }))
}

fn single_header<'a>(
    headers: &'a HeaderMap,
    name: &HeaderName,
) -> Result<Option<&'a str>, CredentialRejection> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(CredentialRejection::BadRequest);
    }
    value
        .to_str()
        .map(Some)
        .map_err(|_| CredentialRejection::BadRequest)
}

#[derive(Debug, Default)]
struct SelectedCookies {
    session: Option<String>,
    csrf: Option<String>,
}

fn selected_cookies(
    headers: &HeaderMap,
    policy: &SessionCookiePolicy,
) -> Result<SelectedCookies, CredentialRejection> {
    let mut selected = SelectedCookies::default();
    for header in headers.get_all(COOKIE) {
        let header = header
            .to_str()
            .map_err(|_| CredentialRejection::BadRequest)?;
        for pair in header
            .split(';')
            .map(str::trim)
            .filter(|pair| !pair.is_empty())
        {
            let Some((name, value)) = pair.split_once('=') else {
                if pair == policy.name || pair == policy.csrf_cookie_name {
                    return Err(CredentialRejection::BadRequest);
                }
                continue;
            };
            let name = name.trim();
            if name == policy.name {
                insert_cookie(&mut selected.session, value)?;
            } else if name == policy.csrf_cookie_name {
                insert_cookie(&mut selected.csrf, value)?;
            }
        }
    }
    Ok(selected)
}

fn insert_cookie(target: &mut Option<String>, value: &str) -> Result<(), CredentialRejection> {
    if target.is_some() {
        return Err(CredentialRejection::BadRequest);
    }
    let value = normalized_cookie_value(value.trim())?;
    if value.is_empty() {
        return Err(CredentialRejection::BadRequest);
    }
    target.replace(value.to_owned());
    Ok(())
}

fn normalized_cookie_value(value: &str) -> Result<&str, CredentialRejection> {
    let value = if value.starts_with('"') || value.ends_with('"') {
        value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .ok_or(CredentialRejection::BadRequest)?
    } else {
        value
    };
    if value.bytes().all(cookie_octet) {
        Ok(value)
    } else {
        Err(CredentialRejection::BadRequest)
    }
}

const fn cookie_octet(byte: u8) -> bool {
    matches!(byte, 0x21 | 0x23..=0x2b | 0x2d..=0x3a | 0x3c..=0x5b | 0x5d..=0x7e)
}

fn safe_method(method: &Method) -> bool {
    method == Method::GET
        || method == Method::HEAD
        || method == Method::OPTIONS
        || method == Method::TRACE
        || method.as_str().eq_ignore_ascii_case("QUERY")
}

fn tokens_match(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}
