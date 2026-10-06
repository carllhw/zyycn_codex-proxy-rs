//! 验证观测查询和数值类型在 HTTP、管理及持久化边界共用的约束

use gateway_admin::model::{
    PageSize,
    observability::{DecimalAmount, ObservabilityPageSize, PercentileMilliseconds},
};

#[test]
fn observability_page_size_has_its_own_limit() {
    for value in [1, 100] {
        assert_eq!(ObservabilityPageSize::new(value).unwrap().get(), value);
    }
    for value in [0, 101, 150, 200, u16::MAX] {
        assert!(ObservabilityPageSize::new(value).is_err(), "{value}");
    }
    assert!(PageSize::new(200).is_ok());
}

#[test]
fn decimal_amount_preserves_numeric_precision_and_normalization() {
    for (input, expected) in [
        ("0", "0"),
        (" 00012.34000 ", "12.34"),
        ("00000.00000", "0"),
        ("9999999999.9999999999", "9999999999.9999999999"),
        ("0.0000000001", "0.0000000001"),
    ] {
        assert_eq!(input.parse::<DecimalAmount>().unwrap().as_str(), expected);
    }
    for input in [
        "",
        "-1",
        "+1",
        "NaN",
        "inf",
        "1e3",
        ".1",
        "1.",
        "1.2.3",
        "10000000000",
        "0.00000000001",
        "１",
    ] {
        assert!(input.parse::<DecimalAmount>().is_err(), "{input:?}");
    }
}

#[test]
fn decimal_arithmetic_keeps_the_shared_storage_precision() {
    let max: DecimalAmount = "9999999999.9999999999".parse().unwrap();
    let unit: DecimalAmount = "0.0000000001".parse().unwrap();
    assert!(max.checked_add(&unit).is_none());
    let one: DecimalAmount = "1".parse().unwrap();
    assert_eq!(one.checked_div_u64(3).unwrap().as_str(), "0.3333333333");
    assert!(one.checked_div_u64(0).is_none());
    assert_eq!(one.checked_add(&unit).unwrap().as_str(), "1.0000000001");
}

#[test]
fn percentile_preserves_interpolation_and_rejects_invalid_values() {
    for value in [0.0, 0.125, 1.5, 95.75] {
        assert_eq!(PercentileMilliseconds::new(value).unwrap().as_f64(), value);
    }
    for value in [-1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(PercentileMilliseconds::new(value).is_err());
    }
}
