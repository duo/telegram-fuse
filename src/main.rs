use anyhow::{Context as _, Result};
use clap::Parser;
use fuser::MountOption;
use grammers_client::{Client, Config, SignInError};
use grammers_session::Session;
use std::io::{self, BufRead as _, Write as _};
use std::path::PathBuf;
use tokio::task;

mod fuse_fs;
mod vfs;

const SESSION_FILE: &str = "tg.session";

#[tokio::main]
async fn main() -> Result<()> {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_hook(info);
        // Immediately exit the whole program when any (async) thread panicked.
        std::process::exit(101);
    }));

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();

    async_main(args).await
}

async fn async_main(args: Args) -> Result<()> {
    let app_id = args.app_id;
    let app_hash = args.app_hash;

    log::info!("Connecting to Telegram...");
    let client = Client::connect(Config {
        session: Session::load_file_or_create(SESSION_FILE)?,
        api_id: app_id,
        api_hash: app_hash.clone(),
        params: Default::default(),
    })
    .await?;
    log::info!("Connected!");

    if !client.is_authorized().await? {
        log::info!("Signing in...");
        let phone = prompt("Enter your phone number (international format): ")?;
        let token = client.request_login_code(&phone, app_id, &app_hash).await?;
        let code = prompt("Enter the code you received: ")?;
        let signed_in = client.sign_in(&token, &code).await;
        match signed_in {
            Err(SignInError::PasswordRequired(password_token)) => {
                let hint = password_token.hint().unwrap();
                let prompt_message = format!("Enter the password (hint {}): ", &hint);
                let password = prompt(prompt_message.as_str())?;

                client
                    .check_password(password_token, password.trim())
                    .await?;
            }
            Ok(_) => (),
            Err(e) => panic!("{}", e),
        };
        log::info!("Signed in!");
        match client.session().save_to_file(SESSION_FILE) {
            Ok(_) => {}
            Err(e) => {
                log::info!(
                    "NOTE: failed to save the session, will sign out when done: {}",
                    e
                );
            }
        }
    }

    let client_handle = client.clone();
    task::spawn(async move { client.run_until_disconnected().await });

    let async_flush = match args.async_flush {
        Some(arg) => arg,
        None => false,
    };
    let vfs = vfs::Vfs::new(client_handle, async_flush)
        .await
        .context("Failed to initialize vfs")?;

    log::info!("Mounting...");
    let fs = fuse_fs::Filesystem::new(vfs);
    let fuse_options = [
        MountOption::FSName("telegram".into()),
        MountOption::DefaultPermissions,
        MountOption::NoDev,
        MountOption::NoSuid,
        MountOption::NoAtime,
        MountOption::RW,
    ];

    tokio::task::spawn_blocking(move || fuser::mount2(fs, &args.mount_point, &fuse_options))
        .await??;

    Ok(())
}

fn prompt(message: &str) -> Result<String> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    stdout.write_all(message.as_bytes())?;
    stdout.flush()?;

    let stdin = io::stdin();
    let mut stdin = stdin.lock();

    let mut line = String::new();
    stdin.read_line(&mut line)?;
    Ok(line)
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    app_id: i32,

    #[arg(long)]
    app_hash: String,

    #[arg(long)]
    async_flush: Option<bool>,

    mount_point: PathBuf,
}
