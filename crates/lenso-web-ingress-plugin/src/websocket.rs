//! Shared WebSocket session validation for socket and Workers transports.
use lenso_capability_websocket_endpoint::{
    ConnectWebsocketResponse as Frame, ConnectWebsocketResponseKind as Kind,
    EndpointConnectWebsocket,
};
use lenso_kernel::{NativeStream, RuntimeFailure, StreamEvent};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

fn failure() -> RuntimeFailure {
    crate::plugin_failure("invalid WebSocket session")
}
fn no_payload(frame: &Frame) -> bool {
    frame.text.is_none() && frame.body.is_none()
}

/// Validate a complete application message before transport allocation or send.
pub(crate) fn validate_frame(frame: &Frame, limit: usize) -> Result<(), RuntimeFailure> {
    if frame.protocol.is_some() {
        return Err(failure());
    }
    let valid = match frame.kind {
        Kind::Text => {
            frame.text.as_ref().is_some_and(|text| text.len() <= limit)
                && frame.body.is_none()
                && frame.code.is_none()
                && frame.reason.is_none()
        }
        Kind::Binary => {
            frame
                .body
                .as_ref()
                .is_some_and(|body| body.as_ref().len() <= limit)
                && frame.text.is_none()
                && frame.code.is_none()
                && frame.reason.is_none()
        }
        Kind::Close => {
            no_payload(frame)
                && frame
                    .code
                    .is_some_and(|code| matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999))
                && frame
                    .reason
                    .as_ref()
                    .is_none_or(|reason| reason.len() <= 123)
        }
        Kind::Accept => false,
    };
    if valid { Ok(()) } else { Err(failure()) }
}

/// A backend-authorized session. The owning Host retains its App until terminal
/// completion. It must bound queued inbound messages and lifetime separately.
#[derive(Debug)]
pub struct WebSocketSession {
    permit: RefCell<Option<tokio::sync::OwnedSemaphorePermit>>,
    stream: Rc<NativeStream<EndpointConnectWebsocket>>,
    protocol: Option<String>,
    message_limit: usize,
    session_limit: usize,
    transferred: Cell<usize>,
    cancelled: Cell<bool>,
    terminal: Cell<bool>,
    sent_close: Cell<bool>,
    received_close: Cell<bool>,
    reading: Cell<bool>,
    writing: Cell<bool>,
}
struct Active<'a>(&'a Cell<bool>);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}
impl WebSocketSession {
    pub(crate) async fn accept(
        stream: NativeStream<EndpointConnectWebsocket>,
        offered: &[String],
        message_limit: usize,
        session_limit: usize,
    ) -> Result<Self, RuntimeFailure> {
        let Ok(StreamEvent::Message(frame)) = stream.receive().await else {
            stream.cancel();
            return Err(failure());
        };
        if frame.kind != Kind::Accept
            || !no_payload(&frame)
            || frame.code.is_some()
            || frame.reason.is_some()
            || frame
                .protocol
                .as_ref()
                .is_some_and(|protocol| !offered.contains(protocol))
        {
            stream.cancel();
            return Err(failure());
        }
        Ok(Self {
            permit: RefCell::new(None),
            stream: Rc::new(stream),
            protocol: frame.protocol,
            message_limit,
            session_limit,
            transferred: Cell::new(0),
            cancelled: Cell::new(false),
            terminal: Cell::new(false),
            sent_close: Cell::new(false),
            received_close: Cell::new(false),
            reading: Cell::new(false),
            writing: Cell::new(false),
        })
    }
    fn validate_data(&self, frame: &Frame) -> Result<(), RuntimeFailure> {
        validate_frame(frame, self.message_limit)?;
        let bytes = frame.text.as_ref().map_or(0, String::len)
            + frame.body.as_ref().map_or(0, |body| body.as_ref().len());
        if bytes > self.session_limit.saturating_sub(self.transferred.get()) {
            return Err(failure());
        }
        self.transferred.set(self.transferred.get() + bytes);
        Ok(())
    }
    pub fn protocol(&self) -> Option<&str> {
        self.protocol.as_deref()
    }
    pub async fn send(&self, frame: Frame) -> Result<(), RuntimeFailure> {
        if self.cancelled.get()
            || self.terminal.get()
            || self.sent_close.get()
            || self.writing.replace(true)
        {
            return Err(failure());
        }
        let _active = Active(&self.writing);
        if let Err(error) = self.validate_data(&frame) {
            self.cancel();
            return Err(error);
        }
        if frame.kind == Kind::Close {
            self.sent_close.set(true);
        }
        if let Err(error) = self.stream.send(frame).await {
            self.cancel();
            return Err(error);
        }
        Ok(())
    }
    pub async fn receive(&self) -> Result<Option<Frame>, RuntimeFailure> {
        if self.cancelled.get() {
            return Err(failure());
        }
        if self.terminal.get() {
            return Ok(None);
        }
        if self.reading.replace(true) {
            return Err(failure());
        }
        let _active = Active(&self.reading);
        loop {
            match self.stream.receive().await {
                Ok(StreamEvent::Message(frame)) => {
                    if self.received_close.get() {
                        self.cancel();
                        return Err(failure());
                    }
                    if let Err(error) = self.validate_data(&frame) {
                        self.cancel();
                        return Err(error);
                    }
                    if frame.kind == Kind::Close {
                        self.received_close.set(true);
                    }
                    return Ok(Some(frame));
                }
                Ok(StreamEvent::PeerHalfClosed) => {}
                Ok(StreamEvent::Terminal(Ok(()))) => {
                    self.terminal.set(true);
                    self.permit.borrow_mut().take();
                    return Ok(None);
                }
                Ok(StreamEvent::Terminal(Err(_))) => {
                    self.cancel();
                    return Err(failure());
                }
                Err(error) => {
                    self.cancel();
                    return Err(error);
                }
            }
        }
    }
    pub(crate) fn retain_permit(&self, permit: Option<tokio::sync::OwnedSemaphorePermit>) {
        *self.permit.borrow_mut() = permit;
    }
    pub fn cancel(&self) {
        self.permit.borrow_mut().take();
        self.cancelled.set(true);
        self.stream.cancel();
    }
}
impl Drop for WebSocketSession {
    fn drop(&mut self) {
        self.stream.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn frame(kind: Kind) -> Frame {
        Frame {
            kind,
            protocol: None,
            text: None,
            body: None,
            code: None,
            reason: None,
        }
    }
    #[test]
    fn rejects_reserved_close_codes_and_overlong_utf8_reasons() {
        let mut close = frame(Kind::Close);
        for code in [1004, 1005, 1006, 1015, 2000] {
            close.code = Some(code);
            assert!(validate_frame(&close, 1024).is_err());
        }
        close.code = Some(1000);
        close.reason = Some("界".repeat(42));
        assert!(validate_frame(&close, 1024).is_err());
        close.reason = Some("界".repeat(41));
        assert!(validate_frame(&close, 1024).is_ok());
    }
    #[test]
    fn preserves_empty_payloads_but_rejects_mixed_frames_and_oversized_messages() {
        let mut text = frame(Kind::Text);
        text.text = Some(String::new());
        assert!(validate_frame(&text, 4).is_ok());
        text.text = Some("hello".into());
        assert!(validate_frame(&text, 4).is_err());
        text.text = Some("ok".into());
        text.code = Some(1000);
        assert!(validate_frame(&text, 4).is_err());
        let mut binary = frame(Kind::Binary);
        binary.body = Some(Vec::new().into());
        assert!(validate_frame(&binary, 4).is_ok());
    }
}

#[derive(Debug)]
pub(crate) struct WebSocketUpgrade {
    pub session: WebSocketSession,
    pub accept_key: String,
}
