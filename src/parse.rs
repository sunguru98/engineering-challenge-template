use anyhow::Context;

pub fn convert_to_schema_int(input: &str) -> anyhow::Result<i64> {
    let mut fractional = 0;
    let numerical;

    if input.contains("-") {
        anyhow::bail!("Invalid input: negative data");
    }

    if input.contains(".") {
        let (number, decimal) = input.split_once(".").unwrap();
        if decimal.len() > 6 || decimal.is_empty() {
            anyhow::bail!("Invalid input: invalid decimal")
        }

        if number.is_empty() {
            anyhow::bail!("Invalid input: invalid number");
        }

        let mut decimal_padded = decimal.to_owned();
        decimal_padded.push_str(&"0".repeat(6 - decimal.len()));
        fractional = decimal_padded.parse::<i64>()?;
        numerical = number.parse::<i64>()?;
    } else {
        numerical = input.parse::<i64>()?;
    }

    if numerical < 0 {
        anyhow::bail!("Invalid input: negative numbers");
    }

    numerical
        .checked_mul(1_000_000)
        .and_then(|n| n.checked_add(fractional))
        .context("Invalid input: numeric overflow")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_valid_numbers() {
        assert_eq!(convert_to_schema_int("0").unwrap(), 0);
        assert_eq!(convert_to_schema_int("1").unwrap(), 1_000_000);
        assert_eq!(convert_to_schema_int("0.5").unwrap(), 500_000);
        assert_eq!(convert_to_schema_int("12.345678").unwrap(), 12_345_678);
    }

    #[test]
    fn rejects_bad_numbers() {
        assert!(convert_to_schema_int("-1").is_err());
        assert!(convert_to_schema_int("0.1234567").is_err()); // 7 decimals
        assert!(convert_to_schema_int(".5").is_err()); // no number before the dot
        assert!(convert_to_schema_int("5.").is_err()); // no digits after the dot
        assert!(convert_to_schema_int("abc").is_err());
        assert!(convert_to_schema_int("9223372036855").is_err()); // overflows after x 1_000_000
    }
}
