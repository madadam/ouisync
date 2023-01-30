use crate::{
    interface::{ClientState, Notification},
    session::{ServerState, SubscriptionHandle},
};
use ouisync_lib::{Error, MonitorId, Result, StateMonitor};
use std::time::Duration;
use tokio::time;

/// Retrieve a state monitor corresponding to the `path`.
pub(crate) fn get(state: &ServerState, path: String) -> Result<StateMonitor> {
    let path = parse_path(&path)?;
    state.root_monitor.locate(path).ok_or(Error::EntryNotFound)
}

/// Subscribe to "on change" events happening inside a monitor corresponding to the `path`.
pub(crate) fn subscribe(
    server_state: &ServerState,
    client_state: &ClientState,
    path: String,
) -> Result<SubscriptionHandle> {
    let path = parse_path(&path)?;
    let monitor = server_state
        .root_monitor
        .locate(path)
        .ok_or(Error::EntryNotFound)?;
    let mut rx = monitor.subscribe();

    let notification_tx = client_state.notification_tx.clone();

    let entry = server_state.tasks.vacant_entry();
    let subscription_id = entry.handle().id();

    let handle = scoped_task::spawn(async move {
        loop {
            match rx.changed().await {
                Ok(()) => {
                    notification_tx
                        .send((subscription_id, Notification::StateMonitor))
                        .await
                        .ok();
                }
                Err(_) => return,
            }

            // Prevent flooding the app with too many "on change" notifications.
            time::sleep(Duration::from_millis(200)).await;
        }
    });

    Ok(entry.insert(handle))
}

fn parse_path(path: &str) -> Result<Vec<MonitorId>> {
    match path.split('/').map(|s| s.parse()).collect() {
        Ok(path) => Ok(path),
        Err(error) => {
            tracing::error!("Failed to parse state monitor path: {:?}", error);
            Err(Error::MalformedData)
        }
    }
}
