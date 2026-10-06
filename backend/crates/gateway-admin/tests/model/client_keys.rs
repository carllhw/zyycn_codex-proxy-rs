//! 验证 Client Key 分组集合的共同上限与唯一性规则

use gateway_admin::model::client_keys::validate_group_ids;
use gateway_core::routing::AccountGroupId;

#[test]
fn client_key_groups_reject_duplicates_and_more_than_one_thousand_ids() {
    assert!(validate_group_ids(&[]).is_ok());
    let mut groups: Vec<_> = (0..1000)
        .map(|index| AccountGroupId::new(format!("grp_{index:032x}")).unwrap())
        .collect();
    assert!(validate_group_ids(&groups).is_ok());
    groups.push(AccountGroupId::new("grp_ffffffffffffffffffffffffffffffff").unwrap());
    assert!(validate_group_ids(&groups).is_err());
    assert!(validate_group_ids(&[groups[0].clone(), groups[0].clone()]).is_err());
}
