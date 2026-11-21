//! `freshet-state-server` — hosted state for Freshet, on a directory you choose.
//!
//! ```sh
//! FRESHET_STATE_SERVER_TOKEN=… freshet-state-server --dir /var/lib/freshet-state --host 0.0.0.0
//! FRESHET_STATE_TOKEN=… freshet plan --project ./models --state https://state.example.com/v1/analytics --commit
//! freshet-state-server --dir state --console apps/freshet-console   # and the console at /console/
//! ```

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

use anyhow::{bail, Context};
use clap::Parser;
use freshet_state_server::console::ConsoleFiles;
use freshet_state_server::http::{self, Tokens, READ_TOKEN_ENV, TOKEN_ENV};
use freshet_state_server::store::Store;

#[derive(Debug, Parser)]
#[command(
    name = "freshet-state-server",
    version,
    about = "Hosted state for Freshet: versioned snapshots, compare-and-swap commits and a commit history, kept in one directory."
)]
struct Cli {
    /// Where states are kept, one subdirectory each. Created if missing; one server
    /// per directory.
    #[arg(long, value_name = "DIR")]
    dir: PathBuf,

    /// The commits each state keeps, newest first. Older ones, and their snapshots,
    /// are deleted as new ones land.
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u64).range(1..))]
    keep: u64,

    #[arg(long, default_value_t = 8790)]
    port: u16,

    /// The address to bind. Defaults to loopback; anywhere else, set the tokens.
    #[arg(long, default_value = "127.0.0.1")]
    host: IpAddr,

    /// Serve the console from this directory (apps/freshet-console) at /console/.
    /// Unset, $FRESHET_STATE_SERVER_CONSOLE; unset too, there is no console.
    #[arg(long, value_name = "DIR")]
    console: Option<PathBuf>,
}

/// Where the image puts the console, so `docker run` serves it with no flag.
const CONSOLE_ENV: &str = "FRESHET_STATE_SERVER_CONSOLE";

fn token(var: &str) -> anyhow::Result<Option<String>> {
    match std::env::var(var) {
        Ok(t) if t.trim().is_empty() => bail!("{var} is set but empty"),
        // Trimmed, as the CLI trims `FRESHET_STATE_TOKEN`: a token read from a file
        // with its newline would otherwise never match what a client sends.
        Ok(t) => Ok(Some(t.trim().to_string())),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(e) => bail!("{var}: {e}"),
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let tokens = Tokens {
        commit: token(TOKEN_ENV)?,
        read: token(READ_TOKEN_ENV)?,
    };
    if tokens.commit.is_none() && tokens.read.is_some() {
        bail!("{READ_TOKEN_ENV} is set without {TOKEN_ENV}: then nothing could ever commit");
    }
    if tokens.commit.is_some() && tokens.commit == tokens.read {
        bail!("{TOKEN_ENV} and {READ_TOKEN_ENV} are the same token, so the read-only one could commit");
    }
    let console_dir = cli.console.clone().or_else(|| {
        std::env::var_os(CONSOLE_ENV)
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
    });
    // A console that is asked for and cannot be served stops the server here, rather
    // than leaving a /console/ that 404s for no visible reason.
    let console = console_dir
        .map(|dir| ConsoleFiles::new(&dir).with_context(|| format!("--console (or {CONSOLE_ENV})")))
        .transpose()?;
    let store = Store::open(&cli.dir, cli.keep as usize)?;
    let addr = SocketAddr::new(cli.host, cli.port);
    http::warn_if_exposed(addr, &tokens);
    let console_root = console
        .as_ref()
        .map(|files| files.root().display().to_string());
    let app = http::app(store, tokens, console);
    let dir = cli.dir.display().to_string();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async move {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            // The address actually bound, so `--port 0` says which port it got.
            let bound = listener.local_addr()?;
            eprintln!("serving states from {dir} on http://{bound}/v1/<name>");
            if let Some(root) = &console_root {
                eprintln!("console at http://{bound}/console/ (from {root})");
            }
            http::serve_app_on(app, listener).await
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_token_is_trimmed_as_the_client_trims_it() {
        let var = "FRESHET_STATE_SERVER_TEST_TOKEN_TRIM";
        std::env::set_var(var, "s3cret\n");
        assert_eq!(super::token(var).unwrap().as_deref(), Some("s3cret"));
        std::env::set_var(var, " \n");
        assert!(super::token(var).is_err(), "blank is still refused");
        std::env::remove_var(var);
        assert_eq!(super::token(var).unwrap(), None);
    }
}
