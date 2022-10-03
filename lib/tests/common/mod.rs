use ouisync::{
    network::Network, AccessSecrets, BranchChangedReceiver, ConfigStore, MasterSecret, PeerAddr,
    Repository, StateMonitor,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::{
    future::Future,
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    thread,
    time::Duration,
};
use tempfile::TempDir;
use tokio::sync::broadcast::error::RecvError;
use tracing::Instrument;

pub(crate) const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

// Test environment
pub(crate) struct Env {
    pub rng: StdRng,
    base_dir: TempDir,
    next_repo_num: u64,
    _span: tracing::span::EnteredSpan,
}

impl Env {
    pub fn with_seed(seed: u64) -> Self {
        Self::with_rng(StdRng::seed_from_u64(seed))
    }

    pub fn with_rng(rng: StdRng) -> Self {
        init_log();

        let span = tracing::info_span!("test", name = thread::current().name()).entered();
        let base_dir = TempDir::new().unwrap();

        Self {
            rng,
            base_dir,
            next_repo_num: 0,
            _span: span,
        }
    }

    pub fn next_store(&mut self) -> PathBuf {
        let num = self.next_repo_num;
        self.next_repo_num += 1;

        self.base_dir.path().join(format!("repo-{}.db", num))
    }

    pub async fn create_repo(&mut self) -> Repository {
        let secrets = AccessSecrets::generate_write(&mut self.rng);
        self.create_repo_with_secrets(secrets).await
    }

    pub async fn create_repo_with_secrets(&mut self, secrets: AccessSecrets) -> Repository {
        Repository::create(
            &self.next_store(),
            self.rng.gen(),
            MasterSecret::generate(&mut self.rng),
            secrets,
            true,
            &StateMonitor::make_root(),
        )
        .await
        .unwrap()
    }

    #[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
    pub async fn create_linked_repos(&mut self) -> (Repository, Repository) {
        let repo_a = self.create_repo().await;
        let repo_b = self
            .create_repo_with_secrets(repo_a.secrets().clone())
            .await;

        (repo_a, repo_b)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Proto {
    #[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
    Tcp,
    #[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
    Quic,
}

impl Proto {
    pub fn wrap(&self, addr: impl Into<SocketAddr>) -> PeerAddr {
        match self {
            Self::Tcp => PeerAddr::Tcp(addr.into()),
            Self::Quic => PeerAddr::Quic(addr.into()),
        }
    }

    #[track_caller]
    pub fn listener_local_addr_v4(&self, network: &Network) -> PeerAddr {
        match self {
            Self::Tcp => PeerAddr::Tcp(network.tcp_listener_local_addr_v4().unwrap()),
            Self::Quic => PeerAddr::Quic(network.quic_listener_local_addr_v4().unwrap()),
        }
    }
}

// Create a single `Network` instance initially bound to the specified address.
pub(crate) async fn create_peer(bind: PeerAddr) -> Network {
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_PEER_ID: AtomicU64 = AtomicU64::new(0);
    let peer_id = NEXT_PEER_ID.fetch_add(1, Ordering::Relaxed);
    let span = tracing::info_span!("peer", id = peer_id);

    let network = {
        let _enter = span.enter();
        Network::new(&[bind], ConfigStore::null(), StateMonitor::make_root())
    };

    network.handle().enable().instrument(span).await;
    network
}

// Create two `Network` instances connected together.
#[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
pub(crate) async fn create_connected_peers(proto: Proto) -> (Network, Network) {
    let a = create_peer(proto.wrap((Ipv4Addr::LOCALHOST, 0))).await;
    let b = create_peer(proto.wrap((Ipv4Addr::LOCALHOST, 0))).await;

    b.add_user_provided_peer(&proto.listener_local_addr_v4(&a));

    (a, b)
}

// Keep calling `f` until it returns `true`. Wait for repo notification between calls.
#[allow(unused)] // https://github.com/rust-lang/rust/issues/46379
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

        wait(&mut rx).await;
    }
}

pub(crate) async fn wait(rx: &mut BranchChangedReceiver) {
    match rx.recv().await {
        Ok(_) | Err(RecvError::Lagged(_)) => (),
        Err(RecvError::Closed) => panic!("notification channel unexpectedly closed"),
    }
}

fn init_log() {
    use tracing::metadata::LevelFilter;

    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                // Only show the logs if explicitly enabled with the `RUST_LOG` env variable.
                .with_default_directive(LevelFilter::OFF.into())
                .from_env_lossy(),
        )
        .try_init()
        // error here most likely means the logger is already initialized. We can ignore that.
        .ok();
}
