//! 验证分组文本共用规则中的字符数、字节数和控制字符边界

use gateway_admin::model::account_groups::validate_group_fields;

#[test]
fn group_fields_distinguish_name_characters_from_description_bytes() {
    let name = "组".repeat(100);
    let description = "é".repeat(2048);
    assert!(validate_group_fields(&name, Some(&description)).is_ok());
    assert!(validate_group_fields(&"组".repeat(101), None).is_err());
    assert!(validate_group_fields("组", Some(&"é".repeat(2049))).is_err());
    for name in ["", " 组", "组 ", "组\n名"] {
        assert!(validate_group_fields(name, None).is_err());
    }
    assert!(validate_group_fields("组", Some("含\t制表符")).is_err());
    assert!(validate_group_fields("组", Some("")).is_ok());
}
