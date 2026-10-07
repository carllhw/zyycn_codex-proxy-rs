//! 更新下载来源信任边界测试

use super::*;

#[test]
fn update_should_trust_github_release_asset_redirect_host() {
    assert!(
        validate_download_url(
            "https://release-assets.githubusercontent.com/github-production-release-asset/archive",
            "https://api.github.com/repos",
        )
        .is_ok()
    );
}
