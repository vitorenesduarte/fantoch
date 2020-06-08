use color_eyre::Report;
use eyre::WrapErr;
use std::future::Future;
use std::pin::Pin;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tsunami::Session;

/// This script should be called like: $ bash script branch
/// - branch: which `fantoch` branch to build
const SETUP_SCRIPT: &str = "./../exp/files/build.sh";

#[macro_export]
macro_rules! args {
    ($($element:expr),*) => {{
        #[allow(unused_mut)]
        let mut vs = Vec::new();
        $(vs.push($element.to_string());)*
        vs
    }};
    ($($element:expr,)*) => {{
        $crate::args![$($element),*]
    }};
}

pub async fn script_exec(
    path: &str,
    args: Vec<String>,
    ssh: &Session,
) -> Result<String, Report> {
    let args = args.join(" ");
    let command = format!("chmod u+x {}; ./{} {} 2>&1", path, path, args);
    let output = ssh
        .shell(command)
        .output()
        .await
        .wrap_err("chmod; ./script")?;
    let stdout = String::from_utf8(output.stdout)?;
    Ok(stdout)
}

pub async fn copy_to(
    local_path: &str,
    (remote_path, ssh): (&str, &Session),
) -> Result<(), Report> {
    // get file contents
    let mut contents = String::new();
    File::open(local_path)
        .await?
        .read_to_string(&mut contents)
        .await?;
    // write them in remote machine
    let mut remote_file = ssh.sftp().write_to(remote_path).await?;
    remote_file.write_all(contents.as_bytes()).await?;
    remote_file.close().await?;
    Ok(())
}

pub async fn copy_from(
    (remote_path, ssh): (&str, &Session),
    local_path: &str,
) -> Result<(), Report> {
    // get file contents from remote machine
    let mut contents = String::new();
    let mut remote_file = ssh.sftp().read_from(remote_path).await?;
    remote_file.read_to_string(&mut contents).await?;
    remote_file.close().await?;
    // write them in file
    File::create(local_path)
        .await?
        .write_all(contents.as_bytes())
        .await?;
    Ok(())
}

pub fn fantoch_setup(
    branch: &'static str,
) -> Box<
    dyn for<'r> Fn(
            &'r mut tsunami::Session,
        ) -> Pin<
            Box<dyn Future<Output = Result<(), Report>> + Send + 'r>,
        > + Send
        + Sync
        + 'static,
> {
    Box::new(move |ssh| {
        Box::pin(async move {
            // files
            let script_file = "setup.sh";

            // first copy file to the machine
            copy_to(SETUP_SCRIPT, (script_file, ssh))
                .await
                .wrap_err("copy_to setup script")?;

            // execute script remotely: "$ bash setup.sh branch"
            let args = args![branch];
            let stdout = script_exec(script_file, args, ssh).await?;
            tracing::debug!("script ended {}", stdout);
            Ok(())
        })
    })
}
