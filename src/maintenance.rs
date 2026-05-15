use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Instant, SystemTime};

use searchnos_db::SearchnosDB;
use tracing::{info, warn};

use crate::negentropy_sync;

#[cfg(unix)]
pub fn spawn_negentropy_signal_listener(db: Arc<SearchnosDB>, relays: Vec<String>, days: u64) {
    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};

        let mut signal = match signal(SignalKind::user_defined2()) {
            Ok(signal) => signal,
            Err(err) => {
                warn!(%err, "failed to listen for SIGUSR2");
                return;
            }
        };
        let running = Arc::new(AtomicBool::new(false));

        while signal.recv().await.is_some() {
            if relays.is_empty() {
                warn!("received SIGUSR2 but no negentropy relays are configured");
                continue;
            }
            if running
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                info!("negentropy reconcile already running");
                continue;
            }

            let db = db.clone();
            let relays = relays.clone();
            let running = running.clone();
            thread::Builder::new()
                .name("searchnos-negentropy".to_owned())
                .spawn(move || {
                    let started_at = Instant::now();
                    let unix_days = recent_unix_days(days);
                    info!(days, "received SIGUSR2, starting negentropy reconcile");
                    if let Err(err) = negentropy_sync::reconcile_unix_days(db, &relays, &unix_days)
                    {
                        warn!(%err, "negentropy reconcile failed");
                    }
                    info!(
                        relays = relays.len(),
                        days = unix_days.len(),
                        elapsed_ms = started_at.elapsed().as_millis(),
                        "finished negentropy reconcile"
                    );
                    running.store(false, Ordering::Release);
                })
                .expect("spawn negentropy reconcile thread");
        }
    });
}

#[cfg(not(unix))]
pub fn spawn_negentropy_signal_listener(_db: Arc<SearchnosDB>, _relays: Vec<String>, _days: u64) {}

pub fn negentropy_relays(relays: Vec<String>) -> Vec<String> {
    relays
        .into_iter()
        .map(|relay| relay.trim().to_owned())
        .filter(|relay| !relay.is_empty())
        .collect()
}

fn recent_unix_days(days: u64) -> Vec<u64> {
    let today = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() / 86_400)
        .unwrap_or(0);
    (0..days)
        .filter_map(|offset| today.checked_sub(offset))
        .collect()
}
