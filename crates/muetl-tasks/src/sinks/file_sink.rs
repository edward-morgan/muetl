//! FileSink - writes events to files.

use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
};

use muetl::{
    impl_config_template, impl_sink_handler,
    task_defs::{MuetlSinkContext, SinkInput, TaskConfig, TaskDef},
};

/// FileSink writes string events to a file.
///
/// Configuration:
/// - `path` (required): Path to the output file
/// - `append` (default: true): Append to existing file or overwrite
/// - `flush_every` (default: 1): Flush after this many events (0 = never auto-flush)
///
/// Accepts String data. Each string is written as a line.
pub struct FileSink {
    #[allow(dead_code)]
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    flush_every: i64,
    event_count: i64,
}

impl FileSink {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn muetl::task_defs::sink::Sink>, String> {
        let path = PathBuf::from(config.require_str("path"));
        let append = config.get_bool("append").unwrap_or(true);
        let flush_every = config.get_i64("flush_every").unwrap_or(1);

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(append)
            .truncate(!append)
            .open(&path)
            .map_err(|e| format!("failed to open file {:?}: {}", path, e))?;

        Ok(Box::new(FileSink {
            path,
            writer: Some(BufWriter::new(file)),
            flush_every,
            event_count: 0,
        }))
    }
}

impl TaskDef for FileSink {
    fn deinit(&mut self) -> Result<(), String> {
        if let Some(ref mut writer) = self.writer {
            writer
                .flush()
                .map_err(|e| format!("failed to flush file: {}", e))?;
        }
        Ok(())
    }
}

impl SinkInput<String> for FileSink {
    const conn_name: &'static str = "input";
    fn handle<'a>(
        &'a mut self,
        _ctx: &'a MuetlSinkContext,
        data: &'a String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let Some(ref mut writer) = self.writer else {
                return;
            };

            let _ = writeln!(writer, "{}", data);
            self.event_count += 1;

            if self.flush_every > 0 && self.event_count % self.flush_every == 0 {
                let _ = writer.flush();
            }
        })
    }
}

impl_sink_handler!(FileSink, task_id = "urn:muetl:sink:file_sink", "input" => String);
impl_config_template!(
    FileSink,
    path: Str!,
    append: Bool = true,
    flush_every: Num = 1,
);
