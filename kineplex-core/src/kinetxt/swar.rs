/// Converte 8 dígitos ASCII em Big-Endian num u64 para inteiro u64 via SWAR
/// em 4 ciclos de clock (Zero Loops, Zero Branches).
#[inline(always)]
pub fn parse_8_digits_swar(ascii_be: u64) -> u64 {
    let mut val = ascii_be.wrapping_sub(0x3030_3030_3030_3030);
    val = ((val >> 8).wrapping_mul(10) + (val & 0x00FF_00FF_00FF_00FF)) & 0x00FF_00FF_00FF_00FF;
    val = ((val >> 16).wrapping_mul(100) + (val & 0x0000_FFFF_0000_FFFF)) & 0x0000_FFFF_0000_FFFF;
    ((val >> 32).wrapping_mul(10000) + (val & 0x0000_0000_FFFF_FFFF)) & 0x0000_0000_FFFF_FFFF
}

/// Parser Numérico Branchless ultra-rápido para fatias ASCII arbitrárias (Inteiros e Decimais com '.')
#[inline(always)]
pub fn fast_parse_f64_ascii(bytes: &[u8], default_scale: f64) -> f64 {
    if bytes.is_empty() {
        return 0.0;
    }
    let mut idx = 0;
    let len = bytes.len();

    // Ignora espaços em branco iniciais
    while idx < len && bytes[idx] == b' ' {
        idx += 1;
    }
    if idx == len {
        return 0.0;
    }

    // Detecta sinal (+/-) sem branch usando aritmética condicional
    let mut sign = 1.0_f64;
    if bytes[idx] == b'-' {
        sign = -1.0;
        idx += 1;
    } else if bytes[idx] == b'+' {
        idx += 1;
    }

    let mut mantissa: u64 = 0;
    let mut frac_digits: i32 = 0;
    let mut in_fraction = false;

    // Caminho acelerado SWAR se houver exatamente 8 dígitos contíguos sem ponto
    if len - idx == 8 {
        if let Ok(chunk) = bytes[idx..idx + 8].try_into() {
            let raw_u64 = u64::from_be_bytes(chunk);
            // Verifica se todos os 8 bytes estão entre '0' (0x30) e '9' (0x39)
            let is_all_digits = ((raw_u64.wrapping_sub(0x3030_3030_3030_3030))
                | (0x3939_3939_3939_3939u64.wrapping_sub(raw_u64)))
                & 0x8080_8080_8080_8080
                == 0;
            if is_all_digits {
                return sign * (parse_8_digits_swar(raw_u64) as f64) * default_scale;
            }
        }
    }

    while idx < len {
        let b = bytes[idx];
        if b.is_ascii_digit() {
            mantissa = mantissa.wrapping_mul(10).wrapping_add((b - b'0') as u64);
            if in_fraction {
                frac_digits += 1;
            }
        } else if b == b'.' || b == b',' {
            in_fraction = true;
        } else if b == b' ' || b == b'\r' {
            break;
        }
        idx += 1;
    }

    let final_val = if in_fraction && frac_digits > 0 {
        let scale = 10f64.powi(frac_digits);
        (mantissa as f64) / scale
    } else {
        (mantissa as f64) * default_scale
    };
    sign * final_val
}
