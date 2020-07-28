use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use std::path::Path;

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

pub async fn vm_exec(
    vm: &tsunami::Machine<'_>,
    command: impl ToString,
) -> Result<String, Report> {
    exec(
        &vm.username,
        &vm.public_ip,
        vm.private_key.as_ref().expect("private key should be set"),
        command,
    )
    .await
}

pub async fn vm_script_exec(
    path: &str,
    args: Vec<String>,
    vm: &tsunami::Machine<'_>,
) -> Result<String, Report> {
    let args = args.join(" ");
    let command = format!("chmod u+x {} && ./{} {}", path, path, args);
    vm_exec(vm, command).await.wrap_err("chmod && ./script")
}

pub fn vm_prepare_command(
    vm: &tsunami::Machine<'_>,
    command: String,
) -> tokio::process::Command {
    prepare_ssh(
        &vm.username,
        &vm.public_ip,
        vm.private_key.as_ref().expect("private key should be set"),
        command,
    )
}

pub async fn exec(
    username: &str,
    public_ip: &str,
    private_key: &std::path::PathBuf,
    command: impl ToString,
) -> Result<String, Report> {
    let out = prepare_ssh(username, public_ip, private_key, command)
        .output()
        .await
        .wrap_err("ssh command")?;
    let out = String::from_utf8(out.stdout)
        .wrap_err("output conversion to utf8")?
        .trim()
        .to_string();
    Ok(out)
}

pub fn prepare_ssh(
    username: &str,
    public_ip: &str,
    private_key: &std::path::PathBuf,
    command: impl ToString,
) -> tokio::process::Command {
    let ssh_command = format!(
        "ssh -o StrictHostKeyChecking=no -i {} {}@{} {}",
        private_key.as_path().display(),
        username,
        public_ip,
        escape(command)
    );
    create_command(ssh_command)
}

pub async fn copy_to(
    local_path: impl AsRef<Path>,
    (remote_path, vm): (impl AsRef<Path>, &tsunami::Machine<'_>),
) -> Result<(), Report> {
    let from = local_path.as_ref().display();
    let to = format!(
        "{}@{}:{}",
        vm.username,
        vm.public_ip,
        remote_path.as_ref().display()
    );
    let scp_command = format!(
        "scp -o StrictHostKeyChecking=no -i {} {} {}",
        vm.private_key
            .as_ref()
            .expect("private key should be set")
            .as_path()
            .display(),
        from,
        to,
    );
    create_command(scp_command).status().await?;
    Ok(())
}

pub async fn copy_from(
    (remote_path, vm): (impl AsRef<Path>, &tsunami::Machine<'_>),
    local_path: impl AsRef<Path>,
) -> Result<(), Report> {
    let from = format!(
        "{}@{}:{}",
        vm.username,
        vm.public_ip,
        remote_path.as_ref().display()
    );
    let to = local_path.as_ref().display();
    let scp_command = format!(
        "scp -o StrictHostKeyChecking=no -i {} {} {}",
        vm.private_key
            .as_ref()
            .expect("private key should be set")
            .as_path()
            .display(),
        from,
        to,
    );
    create_command(scp_command).status().await?;
    Ok(())
}

pub fn create_command(command_arg: String) -> tokio::process::Command {
    tracing::debug!("{}", command_arg);
    let mut command = tokio::process::Command::new("sh");
    command.arg("-c");
    command.arg(command_arg);
    command
}

fn escape(command: impl ToString) -> String {
    format!("\"{}\"", command.to_string())
}
