use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{anyhow, Context};
use nostr_sdk::Event;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct WritePolicyPlugin {
    command: String,
    script: Option<PathBuf>,
    state: Arc<Mutex<PluginState>>,
}

impl WritePolicyPlugin {
    pub fn new(command: String) -> anyhow::Result<Self> {
        let trimmed = command.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("plugin command must not be empty"));
        }
        let script = if trimmed.split_whitespace().count() == 1 {
            Some(PathBuf::from(trimmed))
        } else {
            None
        };
        Ok(Self {
            command: trimmed.to_string(),
            script,
            state: Arc::new(Mutex::new(PluginState::default())),
        })
    }

    pub async fn evaluate_event(
        &self,
        event: &Event,
        received_at: i64,
        source_type: PluginSourceType,
        source_info: &str,
        authenticated_pubkeys: &[String],
    ) -> anyhow::Result<PluginDecision> {
        let request = PluginRequest {
            request_type: "new",
            event,
            received_at,
            source_type: source_type.as_str(),
            source_info,
            authenticated_pubkeys,
        };
        let payload = serde_json::to_string(&request)?;
        let mut state = self.state.lock().await;
        self.ensure_process(&mut state).await?;
        let process = state
            .process
            .as_mut()
            .ok_or_else(|| anyhow!("plugin process not available"))?;
        let line = match process.request(&payload).await {
            Ok(line) => line,
            Err(err) => {
                state.process.take();
                return Err(err);
            }
        };
        let response: PluginResponse =
            serde_json::from_str(&line).context("plugin returned invalid JSON")?;

        let event_id = event.id.to_hex();
        if response.id != event_id {
            return Err(anyhow!(
                "plugin response id mismatch (expected {}, got {})",
                event_id,
                response.id
            ));
        }

        Ok(match response.action {
            PluginAction::Accept => PluginDecision::Accept,
            PluginAction::Reject => PluginDecision::Reject { msg: response.msg },
            PluginAction::ShadowReject => PluginDecision::ShadowReject,
            PluginAction::RejectAndChallenge => {
                PluginDecision::RejectAndChallenge { msg: response.msg }
            }
        })
    }

    async fn ensure_process(&self, state: &mut PluginState) -> anyhow::Result<()> {
        if let Some(script) = &self.script {
            let metadata = std::fs::metadata(script).with_context(|| {
                format!(
                    "failed to read metadata for plugin script '{}'",
                    script.display()
                )
            })?;
            if let Ok(modified) = metadata.modified() {
                match state.last_mtime {
                    Some(prev) if prev != modified => {
                        tracing::info!(
                            script = %script.display(),
                            "plugin script modified; reloading"
                        );
                        state.last_mtime = Some(modified);
                        state.process.take();
                    }
                    None => state.last_mtime = Some(modified),
                    _ => {}
                }
            }
        }

        if state.process.is_none() {
            let process = PluginProcess::spawn(&self.command, self.script.is_some()).await?;
            state.process.replace(process);
        }
        Ok(())
    }
}

#[derive(Default)]
struct PluginState {
    last_mtime: Option<SystemTime>,
    process: Option<PluginProcess>,
}

struct PluginProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
}

impl PluginProcess {
    async fn spawn(command: &str, direct_exec: bool) -> anyhow::Result<Self> {
        let mut cmd = if direct_exec {
            Command::new(command)
        } else {
            let mut c = Command::new("sh");
            c.arg("-c").arg(command);
            c
        };
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        let mut child = cmd
            .spawn()
            .with_context(|| format!("failed to launch plugin '{}'", command))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("failed to capture plugin stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("failed to capture plugin stdout"))?;
        let stdout = BufReader::new(stdout).lines();
        Ok(Self {
            child,
            stdin,
            stdout,
        })
    }

    async fn request(&mut self, payload: &str) -> anyhow::Result<String> {
        self.stdin.write_all(payload.as_bytes()).await?;
        self.stdin.write_all(b"\n").await?;
        self.stdin.flush().await?;
        match self.stdout.next_line().await {
            Ok(Some(line)) => Ok(line),
            Ok(None) => Err(anyhow!("plugin exited without response")),
            Err(err) => Err(err.into()),
        }
    }
}

impl Drop for PluginProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

#[derive(Debug, Clone, Copy)]
pub enum PluginSourceType {
    Ip4,
    Ip6,
    Import,
    Stream,
    Sync,
    Stored,
}

impl PluginSourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            PluginSourceType::Ip4 => "IP4",
            PluginSourceType::Ip6 => "IP6",
            PluginSourceType::Import => "Import",
            PluginSourceType::Stream => "Stream",
            PluginSourceType::Sync => "Sync",
            PluginSourceType::Stored => "Stored",
        }
    }
}

#[derive(Debug)]
pub enum PluginDecision {
    Accept,
    Reject { msg: Option<String> },
    ShadowReject,
    RejectAndChallenge { msg: Option<String> },
}

#[derive(Serialize)]
struct PluginRequest<'a> {
    #[serde(rename = "type")]
    request_type: &'static str,
    event: &'a Event,
    #[serde(rename = "receivedAt")]
    received_at: i64,
    #[serde(rename = "sourceType")]
    source_type: &'a str,
    #[serde(rename = "sourceInfo")]
    source_info: &'a str,
    #[serde(rename = "authenticatedPubkeys")]
    authenticated_pubkeys: &'a [String],
}

#[derive(Deserialize)]
struct PluginResponse {
    id: String,
    action: PluginAction,
    msg: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum PluginAction {
    Accept,
    Reject,
    ShadowReject,
    RejectAndChallenge,
}
