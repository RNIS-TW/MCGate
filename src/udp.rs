//! Everything UDP: standalone static forwards (`proxy`), the voicechat relay + its
//! IP-to-backend routing table, and the shared anti-abuse session/rate limits (`throttle`).

pub mod proxy;
pub mod throttle;
pub mod voice_relay;
pub mod voice_routing;
