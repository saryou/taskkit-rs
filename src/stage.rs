use crate::task::{TaskId, TaskInfo};
use crate::util::Timestamp;
use crate::worker::WorkerId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageInfo {
    pub worker_id: WorkerId,
    pub task_id: TaskId,
    pub assigned_at: Timestamp,
}

impl StageInfo {
    pub fn init(worker_id: impl Into<WorkerId>, task_info: &TaskInfo) -> Self {
        Self {
            worker_id: worker_id.into(),
            task_id: task_info.id.clone(),
            assigned_at: Timestamp::now(),
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("never fail")
    }

    pub fn from_json(json: &str) -> Option<Self> {
        serde_json::from_str(json).ok()
    }
}
