use crate::args;
use crate::util;
use color_eyre::Report;
use eyre::WrapErr;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

/// This script should be called like: $ bash script branch
/// - branch: which `fantoch` branch to build
const SETUP_SCRIPT: &str = "./../exp/files/build.sh";

pub struct Machines<'a> {
    pub regions: Vec<String>,
    pub servers: HashMap<String, tsunami::Machine<'a>>,
    pub clients: HashMap<String, tsunami::Machine<'a>>,
}

#[derive(PartialEq, Clone, Copy)]
pub enum RunMode {
    Release,
    Flamegraph,
}

impl RunMode {
    pub fn binary(&self, binary: &str) -> String {
        let binary = format!("./fantoch/target/release/{}", binary);
        match self {
            RunMode::Release => binary,
            RunMode::Flamegraph => {
                // `source` is needed in order for `flamegraph` to be found
                format!("source ~/.cargo/env && flamegraph {}", binary)
            }
        }
    }

    pub fn is_flamegraph(&self) -> bool {
        self == &RunMode::Flamegraph
    }
}

pub fn fantoch_setup(
    branch: String,
    run_mode: RunMode,
) -> Box<
    dyn for<'r> Fn(
            &'r tsunami::Machine<'_>,
        ) -> Pin<
            Box<dyn Future<Output = Result<(), Report>> + Send + 'r>,
        > + Send
        + Sync
        + 'static,
> {
    Box::new(move |vm| {
        let branch = branch.clone();
        let flamegraph = run_mode.is_flamegraph();
        let aws = "true";
        Box::pin(async move {
            // files
            let script_file = "setup.sh";

            // first copy file to the machine
            util::copy_to(SETUP_SCRIPT, (script_file, &vm))
                .await
                .wrap_err("copy_to setup script")?;

            // execute script remotely: "$ setup.sh branch"
            let mut done = false;
            while !done {
                let stdout = util::script_exec(
                    script_file,
                    args![branch, flamegraph, aws],
                    &vm,
                )
                .await?;
                tracing::debug!("full output:\n{}", stdout);
                // we're done if there was no warning about the packages we need
                done = vec![
                    "build-essential",
                    "pkg-config",
                    "libssl-dev",
                    "chrony",
                    "perf-tools-unstable",
                    "linux-tools-common",
                    "linux-tools-generic",
                    "htop",
                    "dstat",
                    "lsof",
                ]
                .into_iter()
                .all(|package| {
                    let msg = format!("Package {} is not available", package);
                    !stdout.contains(&msg)
                });
                if !done {
                    tracing::warn!(
                        "trying again since at least one package was not available"
                    );
                }
            }
            Ok(())
        })
    })
}

pub fn fantoch_bin_script(
    binary: &str,
    args: Vec<String>,
    run_mode: RunMode,
    output_file: String,
) -> String {
    let binary = run_mode.binary(binary);
    let args = args.join(" ");
    format!("{} {} > {} 2>&1", binary, args, output_file)
}
