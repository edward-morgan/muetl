//! Constructor logs must be attributed and persisted before startup reports failure.
use std::{collections::HashMap, sync::Arc};

use kameo::actor::Spawn;
use muetl::{
    flow::{Flow, RawFlow},
    logging,
    registry::{Registry, TaskDefInfo, TaskInfo},
    runtime::{monitor_actor::Monitor, root::Root},
};

async fn fail<T>() -> Result<T, String> {
    tracing::info!("constructor before await");
    tokio::task::yield_now().await;
    tracing::warn!("constructor after await");
    Err("deliberate constructor failure".into())
}

#[tokio::test]
async fn failed_constructors_persist_task_logs_before_startup_returns() {
    logging::init_task_logging_only();
    let directory = std::env::temp_dir().join(format!("muetl-creation-{}", uuid::Uuid::new_v4()));
    for (kind, info) in [
        (
            "source",
            TaskDefInfo::SourceDef {
                outputs: HashMap::new(),
                build_source: |_| Box::pin(fail()),
            },
        ),
        (
            "sink",
            TaskDefInfo::SinkDef {
                inputs: HashMap::new(),
                build_sink: |_| Box::pin(fail()),
            },
        ),
        (
            "operator",
            TaskDefInfo::OperatorDef {
                inputs: HashMap::new(),
                outputs: HashMap::new(),
                build_operator: |_| Box::pin(fail()),
            },
        ),
    ] {
        let mut registry = Registry::new();
        registry.add_def(TaskInfo {
            task_id: kind.into(),
            config_tpl: None,
            info,
        });
        let raw: RawFlow = serde_json::from_value(serde_json::json!({
            "id": kind,
            "nodes": [{"node_id": kind, "task_id": kind, "configuration": {}}],
            "edges": [],
        }))
        .unwrap();
        let flow = Flow::parse_from(raw, Arc::new(registry)).unwrap();
        let monitor = Monitor::spawn(Monitor::new());
        let root = Root::spawn(
            Root::new(flow, monitor.clone())
                .with_file_logging(&directory)
                .unwrap(),
        );
        assert!(root
            .wait_for_startup_with_result(|result| result.map_err(|error| error.to_string()))
            .await
            .is_err());
        let files: Vec<_> = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with(&format!("{kind}_"))
            })
            .collect();
        assert_eq!(files.len(), 1);
        // No sleep or polling: startup must not return before the writer drains.
        let logs = std::fs::read_to_string(&files[0]).unwrap();
        assert!(
            logs.contains(&format!("INFO [{kind}] constructor before await")),
            "{logs}"
        );
        assert!(
            logs.contains(&format!("WARN [{kind}] constructor after await")),
            "{logs}"
        );
        assert!(
            logs.contains(&format!("ERROR [{kind}] Failed to create task")),
            "{logs}"
        );
        assert!(logs.contains("deliberate constructor failure"), "{logs}");
        monitor.stop_gracefully().await.unwrap();
    }
    std::fs::remove_dir_all(directory).unwrap();
}
