#[derive(Clone)]
pub struct TracingProgressBar {
    progress: indicatif::ProgressBar,
}

impl TracingProgressBar {
    pub fn init(len: u64) -> Self {
        // create progress bar style
        let style = indicatif::ProgressStyle::default_bar().template(
            "[{elapsed_precise}] {wide_bar:.green} {pos:>2}/{len:2} (ETA {eta})",
        );
        // create progress bar and set its style
        let progress = indicatif::ProgressBar::new(len);
        progress.set_style(style);
        let progress = Self { progress };

        // init tracing subscriber
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            // redirect all tracing logs to self; this makes sure that there's a
            // single progress bar that, and not one scattered, in between
            // tracing logs
            .with_writer(progress.clone())
            .init();

        progress
    }

    pub fn inc(&mut self) {
        // NOTE: this function doesn't have to be &mut self
        self.progress.inc(1);
    }
}

impl std::io::Write for TracingProgressBar {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.progress
            .println(format!("{}", std::str::from_utf8(buf).unwrap()));
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer>
    for TracingProgressBar
{
    type Writer = Self;

    fn make_writer(&self) -> Self::Writer {
        self.clone()
    }
}
