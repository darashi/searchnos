use std::{
    collections::HashMap,
    error::Error,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::{DateTime, Utc};
use elasticsearch::Elasticsearch;
use log::{error, info};
use nostr_sdk::prelude::Event;
use tokio::sync::broadcast;

use crate::{condition::Condition, search::do_search};

#[derive(Debug)]
pub struct Engine {
    es_client: Elasticsearch,
    index_name: String,

    subscriptions: Arc<Mutex<HashMap<SocketAddr, HashMap<String, Condition>>>>, // addr -> subscription_id -> condition
    senders: Arc<Mutex<HashMap<Condition, broadcast::Sender<Arc<Vec<Event>>>>>>, // condition -> sender
    cursors: Arc<Mutex<HashMap<Condition, Option<DateTime<Utc>>>>>, // condition -> cursor
}

impl Engine {
    pub fn new(es_client: Elasticsearch, index_name: String) -> Self {
        Self {
            es_client,
            index_name,
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            senders: Arc::new(Mutex::new(HashMap::new())),
            cursors: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn subscribe(
        &self,
        addr: SocketAddr,
        subscription_id: String,
        condition: Condition,
    ) -> broadcast::Receiver<Arc<Vec<Event>>> {
        info!(
            "Subscribe: {} with id={} {:?}",
            addr, subscription_id, &condition
        );
        self.subscriptions
            .lock()
            .unwrap()
            .entry(addr)
            .or_default()
            .insert(subscription_id, condition.clone());

        let mut senders = self.senders.lock().unwrap();
        if let Some(sender) = senders.get(&condition) {
            return sender.subscribe();
        }

        let (sender, receiver) = broadcast::channel(100);
        senders.insert(condition, sender);
        receiver
    }

    pub fn leave(&self, addr: SocketAddr) {
        info!("Leave: {}", addr);
        self.subscriptions.lock().unwrap().remove(&addr);
    }

    pub fn unsubscribe(&self, addr: SocketAddr, subscription_id: String) {
        info!("Unsubscribe: {} with id={}", addr, subscription_id);
        self.subscriptions
            .lock()
            .unwrap()
            .entry(addr)
            .or_default()
            .remove(&subscription_id);
    }

    pub async fn search_once(
        &self,
        condition: &Condition,
        limit: &Option<usize>,
    ) -> Result<(Vec<Event>, Option<DateTime<Utc>>), Box<dyn Error + Send + Sync>> {
        do_search(&self.es_client, &self.index_name, &condition, &None, limit).await
    }

    pub async fn search(&self) {
        // remove stale senders
        {
            let mut senders = self.senders.lock().unwrap();
            senders.retain(|_, sender| sender.receiver_count() > 0);

            self.cursors
                .lock()
                .unwrap()
                .retain(|condition, _| senders.contains_key(condition));
        }

        let mut kv = vec![];
        for (condition, sender) in self.senders.lock().unwrap().iter() {
            kv.push((condition.clone(), sender.clone()));
        }

        for (condition, sender) in kv {
            info!(
                "QUERY {:?} subscriber={}",
                condition,
                sender.receiver_count()
            );
            let cursor = self
                .cursors
                .lock()
                .unwrap()
                .get(&condition)
                .cloned()
                .unwrap_or(None);
            let notes = do_search(
                &self.es_client,
                &self.index_name,
                &condition,
                &cursor,
                &None,
            )
            .await;
            if notes.is_err() {
                error!("QUERY {:?} failed", condition);
                continue;
            }

            let (notes, latest) = notes.unwrap();
            if let Some(latest) = latest {
                self.cursors
                    .lock()
                    .unwrap()
                    .insert(condition.clone(), Some(latest));
            }
            let _ = sender.send(Arc::new(notes));
        }
    }

    pub async fn searcher(&self) {
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            self.search().await;
        }
    }
}
