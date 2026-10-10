//! Read-only mod filters are separate from performance-adapter admission.
//! Stock uses NM as a placeholder alongside virtual Classic in normal mode.
use super::*;

pub(crate) struct ModFilter {
    pub(crate) legacy_bits: i64,
    pub(crate) acronyms: Vec<String>,
    pub(crate) variant: ScoreVariant,
}

pub(crate) fn mod_filter(ruleset: u8, input: &[ApiMod]) -> Result<ModFilter, ApiError> {
    let invalid = || ApiError(StatusCode::BAD_REQUEST, "invalid_score_mods");
    if ruleset > 3 || input.is_empty() || input.len() > 32 {
        return Err(invalid());
    }
    let mut seen = HashSet::new();
    let mut acronyms = Vec::new();
    let mut bits = 0u32;
    let mut representable = true;
    for modifier in input {
        if !modifier.settings.is_empty() || !seen.insert(modifier.acronym.as_str()) {
            return Err(invalid());
        }
        let acronym = modifier.acronym.as_str();
        if !(2..=3).contains(&acronym.len())
            || !acronym
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        {
            return Err(invalid());
        }
        // NoMod does not describe an actual score mod. In particular NM+CL
        // means the unmodified Classic board, not a performance input NM.
        if acronym == "NM" {
            continue;
        }
        let parsed = acronym
            .parse::<rosu_mods::Acronym>()
            .map_err(|_| invalid())?;
        let intermode = rosu_mods::GameModIntermode::from_acronym(parsed);
        match intermode.bits() {
            Some(value) => bits |= value,
            None if acronym == "CL" => {}
            // Native-only/new native acronyms match structured scores only.
            // They must not accidentally match a legacy zero-bit score.
            None => representable = false,
        }
        acronyms.push(acronym.to_owned());
    }
    let relax = acronyms.iter().any(|acronym| acronym == "RX");
    let variant = ScoreVariant::from_legacy(ruleset, if relax { 128 } else { 0 })
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_score_variant"))?;
    Ok(ModFilter {
        legacy_bits: if representable { i64::from(bits) } else { -1 },
        acronyms,
        variant,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn mods(names: &[&str]) -> Vec<ApiMod> {
        names
            .iter()
            .map(|name| ApiMod {
                acronym: (*name).into(),
                settings: Default::default(),
            })
            .collect()
    }

    #[test]
    fn normal_classic_no_mod_filters_are_canonical_without_performance_calculation() {
        let filter = mod_filter(0, &mods(&["NM", "CL"])).unwrap();
        assert_eq!(filter.legacy_bits, 0);
        assert_eq!(filter.acronyms, vec!["CL"]);
        assert_eq!(filter.variant, ScoreVariant::Vanilla);
        let filter = mod_filter(0, &mods(&["NM"])).unwrap();
        assert_eq!(filter.legacy_bits, 0);
        assert!(filter.acronyms.is_empty());
        let filter = mod_filter(0, &mods(&["CL", "RX"])).unwrap();
        assert_eq!(filter.legacy_bits, 128);
        assert_eq!(filter.variant, ScoreVariant::Relax);
        assert!(mod_filter(3, &mods(&["RX"])).is_err());
    }

    #[test]
    fn native_only_filters_do_not_match_legacy_no_mod_or_require_pp_support() {
        let filter = mod_filter(0, &mods(&["WU"])).unwrap();
        assert_eq!(filter.legacy_bits, -1);
        assert_eq!(filter.acronyms, vec!["WU"]);
        assert_eq!(filter.variant, ScoreVariant::Vanilla);
        let filter = mod_filter(0, &mods(&["WU", "RX"])).unwrap();
        assert_eq!(filter.legacy_bits, -1);
        assert_eq!(filter.variant, ScoreVariant::Relax);
        let filter = mod_filter(0, &mods(&["AT"])).unwrap();
        assert_eq!(filter.legacy_bits, 2048);
        assert_eq!(filter.variant, ScoreVariant::Vanilla);
    }

    #[test]
    fn malformed_duplicate_and_oversized_filters_are_bounded() {
        for names in [
            &["CL", "CL"][..],
            &["nm"][..],
            &["<script>"][..],
            &["A"][..],
            &["N M"][..],
        ] {
            assert!(mod_filter(0, &mods(names)).is_err());
        }
        assert!(mod_filter(0, &[]).is_err());
        assert!(mod_filter(0, &mods(&["HD"; 33])).is_err());
        assert!(mod_filter(4, &mods(&["NM"])).is_err());
    }
}
