use crate::{
    db,
    error::{Error, Result},
    file::File,
    format,
};
use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};

const PREFIX: &str = r"\\.\pipe\";

/// Windows named pipe which can be used to read a file from a repository as if it were a regular
/// file on the filesystem.
pub struct NamedPipe {
    name: String,
    server: NamedPipeServer,
}

impl NamedPipe {
    pub fn new() -> Result<Self> {
        let name: [u8; 16] = rand::random();
        let name = format!("{}{:x}", PREFIX, format::Hex(&name));

        let server = ServerOptions::new()
            .first_pipe_instance(true)
            .access_inbound(false)
            .create(&name)
            .map_err(Error::Writer)?;

        Ok(Self { name, server })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Write the whole content of `file` into this named pipe.
    pub async fn write(&mut self, conn: &mut db::Connection, src: &mut File) -> Result<()> {
        self.server.connect().await.map_err(Error::Writer)?;
        src.copy_to_writer(conn, &mut self.server).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        access_control::{AccessSecrets, MasterSecret},
        repository::Repository,
    };
    use futures_util::future;
    use rand::{distributions::Standard, Rng};
    use std::io::SeekFrom;
    use tokio::{fs, io::AsyncReadExt, task};

    #[tokio::test(flavor = "multi_thread")]
    async fn read_file_through_pipe() {
        let device_id = rand::random();
        let repo = Repository::create(
            &db::Store::Temporary,
            device_id,
            MasterSecret::random(),
            AccessSecrets::random_write(),
            false,
        )
        .await
        .unwrap();

        let size = 1024;
        let write_content: Vec<_> = rand::thread_rng()
            .sample_iter(Standard)
            .take(size)
            .collect();

        let mut file = repo.create_file("test.dat").await.unwrap();
        let mut conn = repo.db().acquire().await.unwrap();
        file.write(&mut conn, &write_content).await.unwrap();
        file.flush(&mut conn).await.unwrap();
        file.seek(&mut conn, SeekFrom::Start(0)).await.unwrap();

        let mut pipe_writer = NamedPipe::new().unwrap();
        let name = pipe_writer.name().to_owned();

        let write = task::spawn({
            let db = repo.db().clone();
            async move {
                let mut conn = db.acquire().await.unwrap();
                pipe_writer.write(&mut conn, &mut file).await.unwrap();
                pipe_writer
            }
        });

        let read = task::spawn({
            async move {
                let mut read_content = Vec::new();
                let mut pipe_reader = fs::File::open(name).await.unwrap();
                pipe_reader.read_to_end(&mut read_content).await.unwrap();
                read_content
            }
        });

        let _pipe_writer = write.await.unwrap();
        let read_content = read.await.unwrap();

        assert_eq!(read_content, write_content);
    }
}
