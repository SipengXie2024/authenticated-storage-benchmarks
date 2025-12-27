//! 测试样本数据
//!
//! 对应 C++ SampleTriples.hpp

/// 生成长字符串测试数据（URL 格式）
///
/// 对应 C++ testWithLongStringsAndNodeSplit 中的数据
pub fn get_long_strings() -> Vec<String> {
    vec![
        "http://www.2001aSpaceOdyssey.com/HAL.html".to_string(),
        "http://www.2001aspaceodyssey.com/hal.html".to_string(),
        "http://www.20000LeaguesUnderTheSea.com/Nautilus.html".to_string(),
        "http://www.ABriefHistoryOfTime.com/Hawking.html".to_string(),
        "http://www.AliceInWonderland.com/WhiteRabbit.html".to_string(),
        "http://www.Alien.com/Xenomorph.html".to_string(),
        "http://www.AnimalFarm.com/Napoleon.html".to_string(),
        "http://www.Apocalypse.com/Kurtz.html".to_string(),
        "http://www.Atlas.com/JohnGalt.html".to_string(),
        "http://www.BladeRunner.com/Replicant.html".to_string(),
        "http://www.BraveNewWorld.com/Soma.html".to_string(),
        "http://www.BreakfastOfChampions.com/Vonnegut.html".to_string(),
        "http://www.Catch22.com/Yossarian.html".to_string(),
        "http://www.Childhood.com/Clarke.html".to_string(),
        "http://www.Clockwork.com/Alex.html".to_string(),
        "http://www.CloseEncounters.com/DevilsTower.html".to_string(),
        "http://www.Contact.com/Ellie.html".to_string(),
        "http://www.CrimeAndPunishment.com/Raskolnikov.html".to_string(),
        "http://www.Dune.com/Arrakis.html".to_string(),
        "http://www.EndersGame.com/BattleRoom.html".to_string(),
        "http://www.ET.com/PhoneHome.html".to_string(),
        "http://www.Fahrenheit451.com/Montag.html".to_string(),
        "http://www.FlowersForAlgernon.com/Charlie.html".to_string(),
        "http://www.Foundation.com/Seldon.html".to_string(),
        "http://www.Frankenstein.com/Monster.html".to_string(),
        "http://www.Gatsby.com/GreenLight.html".to_string(),
        "http://www.Gattaca.com/Vincent.html".to_string(),
        "http://www.Gravity.com/Ryan.html".to_string(),
        "http://www.Gravity.com/ryan.html".to_string(),
        "http://www.Hamlet.com/ToBe.html".to_string(),
        "http://www.Hitchhikers.com/42.html".to_string(),
        "http://www.HungerGames.com/Katniss.html".to_string(),
        "http://www.IAmLegend.com/Neville.html".to_string(),
        "http://www.Inception.com/Totem.html".to_string(),
        "http://www.Interstellar.com/Tesseract.html".to_string(),
        "http://www.Invisible.com/Griffin.html".to_string(),
        "http://www.JaneEyre.com/Rochester.html".to_string(),
        "http://www.JurassicPark.com/TRex.html".to_string(),
        "http://www.LordOfTheFlies.com/Conch.html".to_string(),
        "http://www.LordOfTheRings.com/OneRing.html".to_string(),
    ]
}

/// 生成顺序整数键（用于 sequential insert 测试）
pub fn get_sequential_keys(count: usize) -> Vec<[u8; 32]> {
    (0..count)
        .map(|i| {
            let mut key = [0u8; 32];
            let bytes = (i as u64).to_be_bytes();
            key[24..32].copy_from_slice(&bytes);
            key
        })
        .collect()
}

/// 生成随机键（确定性）
pub fn get_random_keys(count: usize, seed: u64) -> Vec<[u8; 32]> {
    use super::DeterministicRng;

    let mut rng = DeterministicRng::new(seed);
    (0..count)
        .map(|_| {
            let mut key = [0u8; 32];
            for chunk in key.chunks_mut(8) {
                let val = rng.next_u64();
                chunk.copy_from_slice(&val.to_be_bytes());
            }
            key
        })
        .collect()
}

/// 字符串转为固定长度 key
pub fn string_to_key(s: &str) -> [u8; 256] {
    let mut key = [0u8; 256];
    let bytes = s.as_bytes();
    let len = bytes.len().min(255);
    key[..len].copy_from_slice(&bytes[..len]);
    key
}

/// 生成 triple 格式的测试数据
///
/// 对应 C++ SampleTriples - 简化版本
pub fn get_sample_triples() -> Vec<String> {
    let mut triples = Vec::new();

    // 生成一些典型的 triple 格式数据
    let subjects = ["Alice", "Bob", "Charlie", "David", "Eve"];
    let predicates = ["knows", "likes", "follows", "blocks", "mentions"];
    let objects = ["Fred", "Grace", "Henry", "Ivy", "Jack"];

    for s in &subjects {
        for p in &predicates {
            for o in &objects {
                triples.push(format!("<{}> <{}> <{}>", s, p, o));
            }
        }
    }

    triples
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_long_strings() {
        let strings = get_long_strings();
        assert!(!strings.is_empty());
        assert!(strings.len() >= 40);

        // 验证所有字符串都以 http:// 开头
        for s in &strings {
            assert!(s.starts_with("http://"));
        }
    }

    #[test]
    fn test_sequential_keys() {
        let keys = get_sequential_keys(100);
        assert_eq!(keys.len(), 100);

        // 验证顺序
        for i in 0..99 {
            assert!(keys[i] < keys[i + 1]);
        }
    }

    #[test]
    fn test_random_keys_deterministic() {
        let keys1 = get_random_keys(100, 12345);
        let keys2 = get_random_keys(100, 12345);

        assert_eq!(keys1, keys2);
    }

    #[test]
    fn test_sample_triples() {
        let triples = get_sample_triples();
        assert_eq!(triples.len(), 5 * 5 * 5); // 125 个
    }
}
