//! Native socket pump; route selection and frame semantics are shared with events.
use crate::WebSocketSession;
use futures::{SinkExt as _, StreamExt as _};
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use lenso_capability_websocket_endpoint::{
    ConnectWebsocketResponse as Frame, ConnectWebsocketResponseKind as Kind,
};
use lenso_kernel::CancellationToken;
use std::{cell::Cell, time::Duration};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        protocol::frame::coding::CloseCode,
        protocol::{CloseFrame, Role, WebSocketConfig},
    },
};

fn closed(error: &tokio_tungstenite::tungstenite::Error) -> bool {
    matches!(
        error,
        tokio_tungstenite::tungstenite::Error::ConnectionClosed
            | tokio_tungstenite::tungstenite::Error::AlreadyClosed
    )
}
fn frame(kind: Kind) -> Frame {
    Frame {
        kind,
        body: None,
        text: None,
        code: None,
        reason: None,
        protocol: None,
    }
}
#[allow(
    clippy::too_many_lines,
    reason = "Both duplex pumps and bounded close share the same socket owner"
)]
pub(crate) async fn run(
    upgrade: OnUpgrade,
    session: WebSocketSession,
    cancellation: CancellationToken,
    message_limit: usize,
    lifetime: Duration,
    grace: Duration,
) {
    let upgraded = tokio::select! {
        result=tokio::time::timeout(grace,upgrade)=>if let Ok(Ok(io)) = result {io} else {session.cancel();return;},
        ()=cancellation.cancelled()=>{session.cancel();return;},
    };
    let config = WebSocketConfig::default()
        .max_message_size(Some(message_limit))
        .max_frame_size(Some(message_limit))
        .write_buffer_size(0)
        .max_write_buffer_size(message_limit.saturating_add(1024));
    let socket =
        WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Server, Some(config)).await;
    let (mut sender, mut receiver) = socket.split();
    let peer_closed = Cell::new(false);
    let outcome = {
        let incoming = async {
            while let Some(message) = receiver.next().await {
                let value = match message.map_err(|_| ())? {
                    Message::Text(text) => {
                        let mut f = frame(Kind::Text);
                        f.text = Some(text.to_string());
                        f
                    }
                    Message::Binary(bytes) => {
                        let mut f = frame(Kind::Binary);
                        f.body = Some(bytes.to_vec().into());
                        f
                    }
                    Message::Close(close) => {
                        peer_closed.set(true);
                        let mut f = frame(Kind::Close);
                        if let Some(close) = close {
                            f.code = Some(u16::from(close.code).into());
                            f.reason = Some(close.reason.to_string());
                        } else {
                            f.code = Some(1000);
                        }
                        f
                    }
                    Message::Ping(_) | Message::Pong(_) => continue,
                    Message::Frame(_) => return Err(()),
                };
                let closing = value.kind == Kind::Close;
                session.send(value).await.map_err(|_| ())?;
                if closing {
                    return Ok(());
                }
            }
            Err(())
        };
        let outgoing = async {
            while let Some(value) = session.receive().await.map_err(|_| ())? {
                let message = match value.kind {
                    Kind::Text => Message::Text(value.text.ok_or(())?.into()),
                    Kind::Binary => Message::Binary(value.body.ok_or(())?.as_ref().to_vec().into()),
                    Kind::Close => {
                        if peer_closed.get() {
                            if let Err(error) = sender.flush().await
                                && !closed(&error)
                            {
                                return Err(());
                            }
                            continue;
                        }
                        Message::Close(Some(CloseFrame {
                            code: CloseCode::from(
                                u16::try_from(value.code.ok_or(())?).map_err(|_| ())?,
                            ),
                            reason: value.reason.unwrap_or_default().into(),
                        }))
                    }
                    Kind::Accept => return Err(()),
                };
                if let Err(error) = sender.send(message).await
                    && !closed(&error)
                {
                    return Err(());
                }
            }
            if let Err(error) = sender.close().await
                && !closed(&error)
            {
                return Err(());
            }
            Ok(())
        };
        let exchange = async { tokio::try_join!(incoming, outgoing).map(|_| ()) };
        tokio::select! {
            result=tokio::time::timeout(lifetime,exchange)=>result.unwrap_or(Err(())),
            ()=cancellation.cancelled()=>Err(()),
        }
    };
    if outcome.is_err() {
        let _ = tokio::time::timeout(
            grace,
            sender.send(Message::Close(Some(CloseFrame {
                code: CloseCode::Error,
                reason: "session unavailable".into(),
            }))),
        )
        .await;
    }
    session.cancel();
}
