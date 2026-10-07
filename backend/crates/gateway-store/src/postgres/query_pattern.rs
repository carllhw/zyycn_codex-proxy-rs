//! 管理实体搜索共用的 SQL 字面前缀转义

pub(super) fn literal_prefix_pattern(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len().saturating_add(1));
    for character in value.to_lowercase().chars() {
        if matches!(character, '\\' | '%' | '_') {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped.push('%');
    escaped
}
