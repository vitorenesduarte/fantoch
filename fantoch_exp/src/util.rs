use color_eyre::Report;
use eyre::WrapErr;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
    prepare_command(
        &vm.username,
        &vm.public_ip,
        vm.private_key.as_ref().expect("private key should be set"),
        command,
    )
}

pub async fn exec(
    username: &String,
    public_ip: &String,
    private_key: &std::path::PathBuf,
    command: impl ToString,
) -> Result<String, Report> {
    let out = prepare_command(username, public_ip, private_key, command)
        .output()
        .await
        .wrap_err("ssh command")?;
    let out = String::from_utf8(out.stdout)
        .wrap_err("output conversion to utf8")?
        .trim()
        .to_string();
    Ok(out)
}

pub fn prepare_command(
    username: &String,
    public_ip: &String,
    private_key: &std::path::PathBuf,
    command: impl ToString,
) -> tokio::process::Command {
    let ssh_command = format!(
        "ssh -o StrictHostKeyChecking=no {}@{} -i {} {}",
        username,
        public_ip,
        private_key.as_path().display(),
        escape(command)
    );
    tracing::debug!("{}", ssh_command);
    let mut command = tokio::process::Command::new("sh");
    command.arg("-c");
    command.arg(ssh_command);
    command
}

pub async fn copy_to(
    local_path: impl AsRef<Path>,
    (remote_path, vm): (impl AsRef<Path>, &tsunami::Machine<'_>),
) -> Result<(), Report> {
    // get file contents
    let mut contents = String::new();
    tokio::fs::File::open(local_path)
        .await?
        .read_to_string(&mut contents)
        .await?;
    // write them in remote machine
    let mut remote_file = vm.ssh.sftp().write_to(remote_path).await?;
    remote_file.write_all(contents.as_bytes()).await?;
    remote_file.close().await?;
    Ok(())
}

pub async fn copy_from(
    (remote_path, vm): (impl AsRef<Path>, &tsunami::Machine<'_>),
    local_path: impl AsRef<Path>,
) -> Result<(), Report> {
    // get file contents from remote machine
    let mut contents = String::new();
    let mut remote_file = vm.ssh.sftp().read_from(remote_path).await?;
    remote_file.read_to_string(&mut contents).await?;
    remote_file.close().await?;
    // write them in file
    tokio::fs::File::create(local_path)
        .await?
        .write_all(contents.as_bytes())
        .await?;
    Ok(())
}

fn escape(command: impl ToString) -> String {
    format!("\"{}\"", command.to_string())
}

// TODO make this async
pub fn serialize<T>(data: T, file: impl AsRef<Path>) -> Result<(), Report>
where
    T: serde::Serialize,
{
    // if the file does not exist it will be created, otherwise truncated
    let file = std::fs::File::create(file).wrap_err("serialize create file")?;
    // create a buf writer
    let buf = std::io::BufWriter::new(file);
    // and try to serialize
    bincode::serialize_into(buf, &data).wrap_err("serialize")?;
    Ok(())
}

// TODO make this async
pub fn deserialize<T>(file: impl AsRef<Path>) -> Result<T, Report>
where
    T: serde::de::DeserializeOwned,
{
    // open the file in read-only
    let file = std::fs::File::open(file).wrap_err("deserialize open file")?;
    // create a buf reader
    let buf = std::io::BufReader::new(file);
    // and try to deserialize
    let data = bincode::deserialize_from(buf).wrap_err("deserialize")?;
    Ok(data)
}
