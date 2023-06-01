use crate::{
    repository::{RepositoryHolder, RepositoryName, OPEN_ON_START},
    state::State,
};
use async_trait::async_trait;
use ouisync_bridge::{
    error::{Error, Result},
    protocol::remote::{Request, Response},
    transport::NotificationSender,
};
use ouisync_lib::{AccessMode, RepositoryId, ShareToken};
use std::{
    iter,
    sync::{Arc, Weak},
};

#[derive(Clone)]
pub(crate) struct RemoteHandler {
    state: Weak<State>,
}

impl RemoteHandler {
    pub fn new(state: Arc<State>) -> Self {
        Self {
            state: Arc::downgrade(&state),
        }
    }
}

#[async_trait]
impl ouisync_bridge::transport::Handler for RemoteHandler {
    type Request = Request;
    type Response = Response;

    async fn handle(
        &self,
        request: Self::Request,
        _notification_tx: &NotificationSender,
    ) -> Result<Self::Response> {
        tracing::debug!(?request);

        let Some(state) = self.state.upgrade() else {
            tracing::error!("can't handle request - shutting down");
            // TODO: return more appropriate error (ShuttingDown or similar)
            return Err(Error::ForbiddenRequest);
        };

        match request {
            Request::Mirror { share_token } => {
                // Mirroring is supported for blind replicas only.
                let share_token: ShareToken = share_token
                    .into_secrets()
                    .with_mode(AccessMode::Blind)
                    .into();

                let name = make_name(share_token.id());

                // Mirror is idempotent
                if state.repositories.contains(&name) {
                    return Ok(().into());
                }

                let store_path = state.store_path(name.as_ref());

                let repository = ouisync_bridge::repository::create(
                    store_path.try_into().map_err(|_| Error::InvalidArgument)?,
                    None,
                    None,
                    Some(share_token),
                    &state.config,
                    &state.repositories_monitor,
                )
                .await?;

                tracing::info!(%name, "repository created");

                let holder = RepositoryHolder::new(repository, name, &state.network).await;
                let holder = Arc::new(holder);

                // Mirror is idempotent
                if !state.repositories.try_insert(holder.clone()) {
                    return Ok(().into());
                }

                holder
                    .repository
                    .metadata()
                    .set(OPEN_ON_START, true)
                    .await
                    .ok();
                holder.registration.set_dht_enabled(true).await;
                holder.registration.set_pex_enabled(true).await;

                Ok(().into())
            }
        }
    }
}

// Derive name from the hash of repository id
fn make_name(id: &RepositoryId) -> RepositoryName {
    RepositoryName::try_from(insert_separators(
        &id.salted_hash(b"ouisync server repository name")
            .to_string(),
    ))
    .unwrap()
}

fn insert_separators(input: &str) -> String {
    let chunk_count = 4;
    let chunk_len = 2;
    let sep = '/';

    let (head, tail) = input.split_at(chunk_count * chunk_len);

    head.chars()
        .enumerate()
        .flat_map(|(i, c)| {
            (i > 0 && i < chunk_count * chunk_len && i % chunk_len == 0)
                .then_some(sep)
                .into_iter()
                .chain(iter::once(c))
        })
        .chain(iter::once(sep))
        .chain(tail.chars())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_separators_test() {
        let input = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

        let expected_output = format!(
            "{}/{}/{}/{}/{}",
            &input[0..2],
            &input[2..4],
            &input[4..6],
            &input[6..8],
            &input[8..],
        );
        let actual_output = insert_separators(input);

        assert_eq!(actual_output, expected_output);
    }
}