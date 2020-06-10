use color_eyre::Report;
use eyre::WrapErr;
use std::future::Future;
use std::pin::Pin;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

pub async fn exec(
    vm: &tsunami::Machine<'_>,
    command: String,
) -> Result<String, Report> {
    let out = prepare_command(vm, command)
        .output()
        .await
        .wrap_err("ssh command")?;
    let out = String::from_utf8(out.stdout)
        .wrap_err("output conversion to utf8")?
        .trim()
        .to_string();
    Ok(out)
}

pub async fn script_exec(
    path: &str,
    args: Vec<String>,
    vm: &tsunami::Machine<'_>,
) -> Result<String, Report> {
    let args = args.join(" ");
    let command = format!("chmod u+x {} && ./{} {}", path, path, args);
    exec(vm, command).await.wrap_err("chmod && ./script")
}

pub fn prepare_command(
    vm: &tsunami::Machine<'_>,
    command: String,
) -> tokio::process::Command {
    let private_key =
        vm.private_key.clone().expect("private key should be set");
    let ssh_command = format!(
        "ssh {}@{} -i {} {}",
        vm.username,
        vm.public_ip,
        private_key.as_path().display(),
        escape(command)
    );
    tracing::debug!("prepared: {}", ssh_command);
    let mut command = tokio::process::Command::new("sh");
    command.arg("-c");
    command.arg(ssh_command);
    command
}

pub async fn copy_to(
    local_path: &str,
    (remote_path, ssh): (&str, &tsunami::Session),
) -> Result<(), Report> {
    // get file contents
    let mut contents = String::new();
    tokio::fs::File::open(local_path)
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
    (remote_path, ssh): (&str, &tsunami::Session),
    local_path: &str,
) -> Result<(), Report> {
    // get file contents from remote machine
    let mut contents = String::new();
    let mut remote_file = ssh.sftp().read_from(remote_path).await?;
    remote_file.read_to_string(&mut contents).await?;
    remote_file.close().await?;
    // write them in file
    tokio::fs::File::create(local_path)
        .await?
        .write_all(contents.as_bytes())
        .await?;
    Ok(())
}

pub fn fantoch_setup(
    branch: String,
) -> Box<
    dyn for<'r> Fn(
            &'r mut tsunami::Machine<'_>,
        ) -> Pin<
            Box<dyn Future<Output = Result<(), Report>> + Send + 'r>,
        > + Send
        + Sync
        + 'static,
> {
    Box::new(move |vm| {
        let branch = branch.clone();
        Box::pin(async move {
            // files
            let script_file = "setup.sh";

            // first copy file to the machine
            copy_to(SETUP_SCRIPT, (script_file, &vm.ssh))
                .await
                .wrap_err("copy_to setup script")?;

            // execute script remotely: "$ setup.sh branch"
            let mut done = false;
            while !done {
                let stdout =
                    script_exec(script_file, args![branch], &vm).await?;
                // we're done if no warning about these packages was issued
                done = vec!["build-essential", "pkg-config"].into_iter().all(
                    |package| {
                        let msg =
                            format!("Package {} is not available", package);
                        !stdout.contains(&msg)
                    },
                );
                if !done {
                    tracing::warn!(
                        "trying again since at least one package was not available"
                    );
                    tracing::debug!("full output:\n{}", stdout);
                }
            }
            Ok(())
        })
    })
}

fn escape(command: String) -> String {
    format!("\"{}\"", command)
}
