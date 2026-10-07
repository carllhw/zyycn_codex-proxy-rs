//! 系统更新的部署配置、编译元数据与本地路径解析

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use super::{OperationError, conflict, internal};
use crate::config::ConfigError;

const APP_BINARY_NAME: &str = "codex-proxy-rs";
const DEFAULT_GITHUB_API_BASE: &str = "https://api.github.com/repos";
const DEFAULT_UPDATE_REPOSITORY: &str = "zyycn/codex-proxy-rs";

/// 系统更新与重启配置；所有字段只由 Host 解释
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct SystemUpdateConfig {
    pub version: String,
    pub git_sha: String,
    pub build_time: String,
    pub deployment_mode: String,
    pub build_type: String,
    pub update_repository: Option<String>,
    pub github_api_base: String,
    pub executable_path: Option<PathBuf>,
    /// 未显式指定时，由组合根传入 API 实际使用的静态资源目录
    pub web_dist_dir: Option<PathBuf>,
    pub update_state_file: PathBuf,
    pub update_lock_file: PathBuf,
    pub update_temp_dir: PathBuf,
    pub self_restart_enabled: bool,
}

impl Default for SystemUpdateConfig {
    fn default() -> Self {
        let deployment_mode =
            environment_value("CPR_DEPLOYMENT_MODE").unwrap_or_else(|| "source".to_owned());
        let executable_path = environment_value("CPR_UPDATE_EXE_PATH")
            .map(PathBuf::from)
            .or_else(|| {
                (deployment_mode == "docker")
                    .then(|| PathBuf::from("/app/bin").join(APP_BINARY_NAME))
            });
        let update_state_file = environment_value("CPR_UPDATE_STATE_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("update-state.json"));
        let update_lock_file = environment_value("CPR_UPDATE_LOCK_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|| update_state_file.with_file_name("update.lock"));
        let update_temp_dir = environment_value("CPR_UPDATE_TEMP_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| default_temp_dir(&update_state_file));
        Self {
            version: option_env!("CPR_VERSION")
                .unwrap_or(env!("CARGO_PKG_VERSION"))
                .to_owned(),
            git_sha: option_env!("CPR_GIT_SHA").unwrap_or("unknown").to_owned(),
            build_time: option_env!("CPR_BUILD_TIME")
                .unwrap_or("unknown")
                .to_owned(),
            deployment_mode,
            build_type: option_env!("CPR_BUILD_TYPE").unwrap_or("source").to_owned(),
            update_repository: Some(
                environment_value("CPR_UPDATE_REPOSITORY")
                    .unwrap_or_else(|| DEFAULT_UPDATE_REPOSITORY.to_owned()),
            ),
            github_api_base: environment_value("CPR_GITHUB_API_BASE")
                .unwrap_or_else(|| DEFAULT_GITHUB_API_BASE.to_owned()),
            executable_path,
            web_dist_dir: None,
            update_state_file,
            update_lock_file,
            update_temp_dir,
            self_restart_enabled: environment_value("CPR_ENABLE_SELF_RESTART").as_deref()
                == Some("true"),
        }
    }
}

impl SystemUpdateConfig {
    pub(crate) fn resolve_and_validate(
        &mut self,
        source_dir: &Path,
        runtime_data_dir: &Path,
        asset_directory: &Path,
    ) -> Result<(), ConfigError> {
        let web_dist_dir = self
            .web_dist_dir
            .get_or_insert_with(|| asset_directory.to_path_buf());
        if web_dist_dir.as_os_str().is_empty() {
            return Err(ConfigError::InvalidField("host.system_update.web_dist_dir"));
        }
        for path in [self.executable_path.as_mut(), self.web_dist_dir.as_mut()]
            .into_iter()
            .flatten()
        {
            if path.is_relative() {
                *path = source_dir.join(&*path);
            }
        }
        for path in [
            &mut self.update_state_file,
            &mut self.update_lock_file,
            &mut self.update_temp_dir,
        ] {
            if path.is_relative() {
                *path = runtime_data_dir.join(&*path);
            }
        }
        if self.version.trim().is_empty() {
            return Err(ConfigError::InvalidField("host.system_update.version"));
        }
        if !matches!(
            self.deployment_mode.as_str(),
            "source" | "binary" | "docker"
        ) {
            return Err(ConfigError::InvalidField(
                "host.system_update.deployment_mode",
            ));
        }
        Ok(())
    }

    pub(crate) fn executable_path(&self) -> Result<PathBuf, OperationError> {
        if let Some(path) = &self.executable_path {
            return Ok(path.clone());
        }
        env::current_exe()
            .and_then(fs::canonicalize)
            .map_err(|error| internal(format!("failed to resolve executable: {error}")))
    }

    pub(crate) fn official_plugins_dir(&self) -> Result<PathBuf, OperationError> {
        let executable = self.executable_path()?;
        let parent = executable
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .ok_or_else(|| internal("failed to resolve official plugin release directory"))?;
        Ok(parent.join("plugins").join("official"))
    }

    pub(crate) fn web_dist_dir(&self) -> Result<&Path, OperationError> {
        self.web_dist_dir
            .as_deref()
            .filter(|path| !path.as_os_str().is_empty())
            .ok_or_else(|| conflict("web assets directory is not configured"))
    }
}

pub(super) fn environment_value(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn default_temp_dir(state_file: &Path) -> PathBuf {
    state_file
        .parent()
        .map(|parent| parent.join("update-tmp"))
        .unwrap_or_else(|| std::env::temp_dir().join("codex-proxy-rs-update"))
}
