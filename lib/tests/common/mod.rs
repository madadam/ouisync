use ouisync::{
    AccessSecrets, ConfigStore, DbStore, MasterSecret, Network, NetworkOptions, Repository,
};
use rand::{rngs::StdRng, Rng};
use std::{
    future::Future,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
};

// Create two `Network` instances connected together.
pub(crate) async fn create_connected_peers() -> (Network, Network) {
    let a = Network::new(&test_network_options(), ConfigStore::null())
        .await
        .unwrap();

    let b = create_peer_connected_to(*a.listener_local_addr_v4().unwrap()).await;

    (a, b)
}

// Create a `Network` instance connected only to the given address.
pub(crate) async fn create_peer_connected_to(addr: SocketAddr) -> Network {
    Network::new(
        &NetworkOptions {
            peers: vec![addr],
            ..test_network_options()
        },
        ConfigStore::null(),
    )
    .await
    .unwrap()
}

pub(crate) async fn create_repo(rng: &mut StdRng) -> Repository {
    let secrets = AccessSecrets::generate_write(rng);
    create_repo_with_secrets(rng, secrets).await
}

pub(crate) async fn create_repo_with_secrets(
    rng: &mut StdRng,
    secrets: AccessSecrets,
) -> Repository {
    Repository::create(
        &DbStore::Temporary,
        rng.gen(),
        MasterSecret::generate(rng),
        secrets,
        false,
    )
    .await
    .unwrap()
}

pub(crate) async fn create_linked_repos(rng: &mut StdRng) -> (Repository, Repository) {
    let repo_a = create_repo(rng).await;
    let repo_b = create_repo_with_secrets(rng, repo_a.secrets().clone()).await;

    (repo_a, repo_b)
}

pub(crate) fn test_network_options() -> NetworkOptions {
    NetworkOptions {
        bind_v4: Ipv4Addr::LOCALHOST.into(),
        bind_v6: Ipv6Addr::LOCALHOST.into(),
        disable_local_discovery: true,
        disable_upnp: true,
        disable_dht: true,
        ..Default::default()
    }
}

// Keep calling `f` until it returns `true`. Wait for repo notification between calls.
pub(crate) async fn eventually<F, Fut>(repo: &Repository, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let mut rx = repo.subscribe();

    loop {
        if f().await {
            break;
        }

        rx.recv().await.unwrap();
    }
}
