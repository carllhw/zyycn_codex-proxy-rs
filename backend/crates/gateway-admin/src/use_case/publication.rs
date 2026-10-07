//! 管理事务提交后的运行快照发布入口

use crate::model::AdminError;
use gateway_core::{routing::ConfigRevision, runtime::SnapshotControl};

pub(super) async fn publish_committed(
    snapshot: &dyn SnapshotControl,
    revision: crate::model::Revision,
) -> Result<(), AdminError> {
    let revision = ConfigRevision::new(revision.get())
        .map_err(|_| AdminError::internal("已提交的配置版本不合法"))?;
    snapshot.publish_committed(revision).await;
    Ok(())
}
