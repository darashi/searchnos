use futures::{sink::SinkExt, stream::SplitSink};
use nostr_sdk::{prelude::RelayMessage, Event, JsonUtil, SubscriptionId};
use std::sync::Arc;
use tokio::sync::Mutex;
use yawc::{frame::Frame, HttpWebSocket as YawcWebSocket};

pub type WsSink = SplitSink<YawcWebSocket, Frame>;

#[derive(Clone)]
pub struct RelaySender {
    inner: Arc<Mutex<WsSink>>,
}

impl RelaySender {
    pub fn new(inner: WsSink) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn text(&self, text: impl Into<String>) -> anyhow::Result<()> {
        self.inner
            .lock()
            .await
            .send(Frame::text(text.into()))
            .await?;
        Ok(())
    }

    pub async fn frame(&self, frame: Frame) -> anyhow::Result<()> {
        self.inner.lock().await.send(frame).await?;
        Ok(())
    }

    pub async fn relay_message(&self, msg: RelayMessage<'_>) -> anyhow::Result<()> {
        self.text(msg.as_json()).await
    }

    pub async fn notice(&self, msg: &str) -> anyhow::Result<()> {
        self.relay_message(RelayMessage::notice(msg)).await
    }

    pub async fn closed(&self, subscription_id: SubscriptionId, msg: &str) -> anyhow::Result<()> {
        self.relay_message(RelayMessage::closed(subscription_id, msg))
            .await
    }

    pub async fn eose(&self, subscription_id: SubscriptionId) -> anyhow::Result<()> {
        self.relay_message(RelayMessage::eose(subscription_id))
            .await
    }

    pub async fn ok(&self, event: &Event, status: bool, message: &str) -> anyhow::Result<()> {
        self.relay_message(RelayMessage::ok(event.id, status, message))
            .await
    }
}
