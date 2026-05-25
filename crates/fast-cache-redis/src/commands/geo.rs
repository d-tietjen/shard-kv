use crate::commands::redis::{
    array_bulk, bulk, eq_ignore_ascii_case, error, int, parse_f64, parse_usize, wrong_arity,
    wrongtype,
};
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, RedisObjectError, RedisObjectResult, RedisZSetStore};

const GEO_LAT_MIN: f64 = -85.051_128_78;
const GEO_LAT_MAX: f64 = 85.051_128_78;
const GEO_SCALE: f64 = ((1_u64 << 26) - 1) as f64;
const EARTH_RADIUS_M: f64 = 6_372_797.560_856;
const GEOHASH_ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";

macro_rules! define_geo_command {
    ($type:ident, $static_name:ident, $name:literal, $mutates:expr) => {
        #[derive(Debug, Clone, Copy)]
        pub(crate) struct $type;

        pub(crate) static $static_name: $type = $type;

        impl crate::commands::CommandSpec for $type {
            const NAME: &'static str = $name;
            const MUTATES_VALUE: bool = $mutates;
        }
    };
}

define_geo_command!(GeoAdd, GEOADD_COMMAND, "GEOADD", true);
define_geo_command!(GeoDist, GEODIST_COMMAND, "GEODIST", false);
define_geo_command!(GeoHash, GEOHASH_COMMAND, "GEOHASH", false);
define_geo_command!(GeoPos, GEOPOS_COMMAND, "GEOPOS", false);
define_geo_command!(GeoRadius, GEORADIUS_COMMAND, "GEORADIUS", true);
define_geo_command!(GeoRadiusRo, GEORADIUS_RO_COMMAND, "GEORADIUS_RO", false);
define_geo_command!(
    GeoRadiusByMember,
    GEORADIUSBYMEMBER_COMMAND,
    "GEORADIUSBYMEMBER",
    true
);
define_geo_command!(
    GeoRadiusByMemberRo,
    GEORADIUSBYMEMBER_RO_COMMAND,
    "GEORADIUSBYMEMBER_RO",
    false
);

impl crate::commands::redis::RedisCommand for GeoAdd {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() < 4 || !(args.len() - 1).is_multiple_of(3) {
            return wrong_arity("GEOADD");
        }
        let key = args[0];
        let mut entries = Vec::with_capacity((args.len() - 1) / 3);
        for chunk in args[1..].chunks_exact(3) {
            let (Ok(lon), Ok(lat)) = (parse_f64(chunk[0]), parse_f64(chunk[1])) else {
                return error("ERR invalid longitude,latitude pair");
            };
            let Some(score) = encode_geo_score(lon, lat) else {
                return error("ERR invalid longitude,latitude pair");
            };
            entries.push((score, chunk[2]));
        }
        let mut inserted = 0;
        for (score, member) in entries {
            match store.zadd(key, score, member) {
                RedisObjectResult::Integer(value) => inserted += value,
                RedisObjectResult::WrongType => return wrongtype(),
                _ => {}
            }
        }
        int(inserted)
    }
}

impl crate::commands::redis::RedisCommand for GeoDist {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let (key, left, right, unit) = match args {
            [key, left, right] => (*key, *left, *right, b"m".as_slice()),
            [key, left, right, unit] => (*key, *left, *right, *unit),
            _ => return wrong_arity("GEODIST"),
        };
        let Some(unit) = GeoUnit::parse(unit) else {
            return error("ERR unsupported unit provided. please use M, KM, FT, MI");
        };
        let left = match member_position(store, key, left) {
            Ok(Some(position)) => position,
            Ok(None) => return Frame::Null,
            Err(frame) => return frame,
        };
        let right = match member_position(store, key, right) {
            Ok(Some(position)) => position,
            Ok(None) => return Frame::Null,
            Err(frame) => return frame,
        };
        bulk(format_float(unit.from_meters(distance_m(left, right))).into_bytes())
    }
}

impl crate::commands::redis::RedisCommand for GeoHash {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, members @ ..] = args else {
            return wrong_arity("GEOHASH");
        };
        if members.is_empty() {
            return wrong_arity("GEOHASH");
        }
        let mut out = Vec::with_capacity(members.len());
        for member in members {
            match member_score(store, key, member) {
                Ok(Some(score)) => out.push(bulk(geohash_string(score))),
                Ok(None) => out.push(Frame::Null),
                Err(frame) => return frame,
            }
        }
        Frame::Array(out)
    }
}

impl crate::commands::redis::RedisCommand for GeoPos {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, members @ ..] = args else {
            return wrong_arity("GEOPOS");
        };
        if members.is_empty() {
            return wrong_arity("GEOPOS");
        }
        let mut out = Vec::with_capacity(members.len());
        for member in members {
            match member_position(store, key, member) {
                Ok(Some(position)) => out.push(coord_frame(position)),
                Ok(None) => out.push(Frame::Null),
                Err(frame) => return frame,
            }
        }
        Frame::Array(out)
    }
}

impl crate::commands::redis::RedisCommand for GeoRadius {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        georadius(store, args, false, false)
    }
}

impl crate::commands::redis::RedisCommand for GeoRadiusRo {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        georadius(store, args, false, true)
    }
}

impl crate::commands::redis::RedisCommand for GeoRadiusByMember {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        georadius(store, args, true, false)
    }
}

impl crate::commands::redis::RedisCommand for GeoRadiusByMemberRo {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        georadius(store, args, true, true)
    }
}

#[derive(Debug, Clone, Copy)]
struct Position {
    lon: f64,
    lat: f64,
}

#[derive(Debug, Clone, Copy)]
struct GeoUnit {
    meters: f64,
}

impl GeoUnit {
    fn parse(raw: &[u8]) -> Option<Self> {
        if eq_ignore_ascii_case(raw, b"m") {
            Some(Self { meters: 1.0 })
        } else if eq_ignore_ascii_case(raw, b"km") {
            Some(Self { meters: 1_000.0 })
        } else if eq_ignore_ascii_case(raw, b"mi") {
            Some(Self { meters: 1_609.344 })
        } else if eq_ignore_ascii_case(raw, b"ft") {
            Some(Self { meters: 0.3048 })
        } else {
            None
        }
    }

    fn to_meters(self, value: f64) -> f64 {
        value * self.meters
    }

    fn from_meters(self, value: f64) -> f64 {
        value / self.meters
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortOrder {
    None,
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
struct GeoRadiusOptions<'a> {
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
    count: Option<usize>,
    sort: SortOrder,
    store: Option<(&'a [u8], bool)>,
}

#[derive(Debug)]
struct GeoHit {
    member: Vec<u8>,
    score: f64,
    position: Position,
    distance_m: f64,
}

fn georadius(store: &EmbeddedStore, args: &[&[u8]], by_member: bool, read_only: bool) -> Frame {
    let min_len = if by_member { 4 } else { 5 };
    if args.len() < min_len {
        return wrong_arity(if by_member {
            "GEORADIUSBYMEMBER"
        } else {
            "GEORADIUS"
        });
    }
    let key = args[0];
    let (center, radius, unit, option_start) = if by_member {
        let center = match member_position(store, key, args[1]) {
            Ok(Some(position)) => position,
            Ok(None) => return Frame::Array(Vec::new()),
            Err(frame) => return frame,
        };
        let (Ok(radius), Some(unit)) = (parse_f64(args[2]), GeoUnit::parse(args[3])) else {
            return error("ERR unsupported unit provided. please use M, KM, FT, MI");
        };
        (center, radius, unit, 4)
    } else {
        let (Ok(lon), Ok(lat), Ok(radius), Some(unit)) = (
            parse_f64(args[1]),
            parse_f64(args[2]),
            parse_f64(args[3]),
            GeoUnit::parse(args[4]),
        ) else {
            return error("ERR invalid longitude,latitude pair or radius");
        };
        let Some(score) = encode_geo_score(lon, lat) else {
            return error("ERR invalid longitude,latitude pair");
        };
        (decode_geo_score(score), radius, unit, 5)
    };
    let options = match parse_radius_options(&args[option_start..], read_only) {
        Ok(options) => options,
        Err(frame) => return frame,
    };
    let mut hits = match geo_hits(store, key, center, unit.to_meters(radius)) {
        Ok(hits) => hits,
        Err(frame) => return frame,
    };
    match options.sort {
        SortOrder::Asc => hits.sort_by(|left, right| left.distance_m.total_cmp(&right.distance_m)),
        SortOrder::Desc => {
            hits.sort_by(|left, right| right.distance_m.total_cmp(&left.distance_m));
        }
        SortOrder::None => {}
    }
    if let Some(count) = options.count {
        hits.truncate(count);
    }
    if let Some((dest, store_dist)) = options.store {
        store.delete(dest);
        for hit in &hits {
            let score = if store_dist {
                unit.from_meters(hit.distance_m)
            } else {
                hit.score
            };
            let result = store.zadd(dest, score, &hit.member);
            if matches!(result, RedisObjectResult::WrongType) {
                return wrongtype();
            }
        }
        return int(hits.len() as i64);
    }
    radius_response(hits, unit, &options)
}

fn parse_radius_options<'a>(
    args: &'a [&'a [u8]],
    read_only: bool,
) -> Result<GeoRadiusOptions<'a>, Frame> {
    let mut options = GeoRadiusOptions {
        with_coord: false,
        with_dist: false,
        with_hash: false,
        count: None,
        sort: SortOrder::None,
        store: None,
    };
    let mut index = 0;
    while index < args.len() {
        let option = args[index];
        if eq_ignore_ascii_case(option, b"WITHCOORD") {
            options.with_coord = true;
            index += 1;
        } else if eq_ignore_ascii_case(option, b"WITHDIST") {
            options.with_dist = true;
            index += 1;
        } else if eq_ignore_ascii_case(option, b"WITHHASH") {
            options.with_hash = true;
            index += 1;
        } else if eq_ignore_ascii_case(option, b"ASC") {
            options.sort = SortOrder::Asc;
            index += 1;
        } else if eq_ignore_ascii_case(option, b"DESC") {
            options.sort = SortOrder::Desc;
            index += 1;
        } else if eq_ignore_ascii_case(option, b"COUNT") {
            let Some(count) = args.get(index + 1) else {
                return Err(error("ERR syntax error"));
            };
            let Ok(count) = parse_usize(count) else {
                return Err(error("ERR value is not an integer or out of range"));
            };
            options.count = Some(count);
            index += if args
                .get(index + 2)
                .is_some_and(|arg| eq_ignore_ascii_case(arg, b"ANY"))
            {
                3
            } else {
                2
            };
        } else if eq_ignore_ascii_case(option, b"STORE")
            || eq_ignore_ascii_case(option, b"STOREDIST")
        {
            if read_only {
                return Err(error(
                    "ERR STORE option is not allowed for read-only GEO command",
                ));
            }
            let Some(dest) = args.get(index + 1) else {
                return Err(error("ERR syntax error"));
            };
            options.store = Some((*dest, eq_ignore_ascii_case(option, b"STOREDIST")));
            index += 2;
        } else {
            return Err(error("ERR syntax error"));
        }
    }
    Ok(options)
}

fn geo_hits(
    store: &EmbeddedStore,
    key: &[u8],
    center: Position,
    radius_m: f64,
) -> Result<Vec<GeoHit>, Frame> {
    let entries = match store.zentries(key) {
        Ok(entries) => entries,
        Err(RedisObjectError::MissingKey) => Vec::new(),
        Err(RedisObjectError::WrongType) => return Err(wrongtype()),
    };
    let mut hits = Vec::new();
    for (member, score) in entries {
        let position = decode_geo_score(score);
        let distance_m = distance_m(center, position);
        if distance_m <= radius_m {
            hits.push(GeoHit {
                member,
                score,
                position,
                distance_m,
            });
        }
    }
    Ok(hits)
}

fn radius_response(hits: Vec<GeoHit>, unit: GeoUnit, options: &GeoRadiusOptions<'_>) -> Frame {
    let decorated = options.with_coord || options.with_dist || options.with_hash;
    if !decorated {
        return array_bulk(hits.into_iter().map(|hit| hit.member).collect());
    }
    Frame::Array(
        hits.into_iter()
            .map(|hit| {
                let mut item = Vec::new();
                item.push(bulk(hit.member));
                if options.with_dist {
                    item.push(bulk(
                        format_float(unit.from_meters(hit.distance_m)).into_bytes(),
                    ));
                }
                if options.with_hash {
                    item.push(int(hit.score as i64));
                }
                if options.with_coord {
                    item.push(coord_frame(hit.position));
                }
                Frame::Array(item)
            })
            .collect(),
    )
}

fn member_score(store: &EmbeddedStore, key: &[u8], member: &[u8]) -> Result<Option<f64>, Frame> {
    store
        .zscore_value(key, member)
        .map_err(|error| match error {
            RedisObjectError::WrongType => wrongtype(),
            RedisObjectError::MissingKey => Frame::Null,
        })
}

fn member_position(
    store: &EmbeddedStore,
    key: &[u8],
    member: &[u8],
) -> Result<Option<Position>, Frame> {
    member_score(store, key, member).map(|score| score.map(decode_geo_score))
}

fn encode_geo_score(lon: f64, lat: f64) -> Option<f64> {
    if !lon.is_finite()
        || !lat.is_finite()
        || !(-180.0..=180.0).contains(&lon)
        || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat)
    {
        return None;
    }
    let lon_bits = (((lon + 180.0) / 360.0) * GEO_SCALE).round() as u64;
    let lat_bits = (((lat - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN)) * GEO_SCALE).round() as u64;
    Some(((lon_bits << 26) | lat_bits) as f64)
}

fn decode_geo_score(score: f64) -> Position {
    let bits = score.max(0.0) as u64;
    let lon_bits = (bits >> 26) & ((1_u64 << 26) - 1);
    let lat_bits = bits & ((1_u64 << 26) - 1);
    Position {
        lon: (lon_bits as f64 / GEO_SCALE) * 360.0 - 180.0,
        lat: (lat_bits as f64 / GEO_SCALE) * (GEO_LAT_MAX - GEO_LAT_MIN) + GEO_LAT_MIN,
    }
}

fn distance_m(left: Position, right: Position) -> f64 {
    let lat1 = left.lat.to_radians();
    let lat2 = right.lat.to_radians();
    let dlat = (right.lat - left.lat).to_radians();
    let dlon = (right.lon - left.lon).to_radians();
    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * a.sqrt().asin()
}

fn coord_frame(position: Position) -> Frame {
    Frame::Array(vec![
        bulk(format_float(position.lon).into_bytes()),
        bulk(format_float(position.lat).into_bytes()),
    ])
}

fn geohash_string(score: f64) -> Vec<u8> {
    let bits = score.max(0.0) as u64;
    let mut out = Vec::with_capacity(11);
    for shift in (0..55).rev().step_by(5) {
        let index = ((bits >> shift.min(51)) & 31) as usize;
        out.push(GEOHASH_ALPHABET[index]);
    }
    out.truncate(11);
    out
}

fn format_float(value: f64) -> String {
    let mut out = format!("{value:.6}");
    while out.contains('.') && out.ends_with('0') {
        out.pop();
    }
    if out.ends_with('.') {
        out.push('0');
    }
    out
}
