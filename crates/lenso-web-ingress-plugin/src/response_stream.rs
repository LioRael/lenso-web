//! Shared response frame validation for socket and event transports.
use bytes::Bytes;
use lenso_capability_http_stream_endpoint as endpoint;
use lenso_kernel::{NativeStream, RuntimeFailure, StreamEvent};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

/// A response stream whose chunks are pulled under transport backpressure.
/// Dropping or cancelling it closes the bound Capability session. A caller must
/// keep its App generation alive until this stream and App shutdown complete.
#[derive(Debug)]
pub struct WebIngressResponseStream {
    permit: RefCell<Option<tokio::sync::OwnedSemaphorePermit>>,
    stream: Rc<NativeStream<endpoint::StreamEndpointHandle>>,
    done: Cell<bool>,
    cancelled: Cell<bool>,
    receiving: Cell<bool>,
}

struct Reading<'a>(&'a Cell<bool>);
impl Drop for Reading<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

impl WebIngressResponseStream {
    pub(crate) fn new(stream: NativeStream<endpoint::StreamEndpointHandle>) -> Self {
        Self {
            permit: RefCell::new(None),
            stream: Rc::new(stream),
            done: Cell::new(false),
            cancelled: Cell::new(false),
            receiving: Cell::new(false),
        }
    }

    /// Receives one chunk. Only a successful terminal frame means clean EOF;
    /// malformed frames, domain rejection, and runtime failure remain errors.
    pub async fn receive(&self) -> Result<Option<Bytes>, RuntimeFailure> {
        if self.cancelled.get() {
            return Err(crate::plugin_failure("HTTP response stream cancelled"));
        }
        if self.done.get() {
            return Ok(None);
        }
        if self.receiving.replace(true) {
            return Err(crate::plugin_failure(
                "concurrent HTTP response stream read",
            ));
        }
        let _reading = Reading(&self.receiving);
        loop {
            match self.stream.receive().await {
                Ok(StreamEvent::Message(frame))
                    if frame.kind == endpoint::HandleResponseKind::Chunk
                        && frame.status.is_none()
                        && frame.headers.is_none() =>
                {
                    return Ok(Some(frame.body.unwrap_or_default().into_shared()));
                }
                Ok(StreamEvent::PeerHalfClosed) => {}
                Ok(StreamEvent::Terminal(Ok(()))) => {
                    self.done.set(true);
                    self.permit.borrow_mut().take();
                    return Ok(None);
                }
                Ok(StreamEvent::Terminal(Err(_)) | StreamEvent::Message(_)) => {
                    self.cancel();
                    return Err(crate::plugin_failure(
                        "invalid HTTP response stream termination",
                    ));
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
        self.done.set(true);
        self.stream.cancel();
    }
    pub fn is_closed(&self) -> bool {
        self.done.get()
    }
}

impl Drop for WebIngressResponseStream {
    fn drop(&mut self) {
        self.stream.cancel();
    }
}
