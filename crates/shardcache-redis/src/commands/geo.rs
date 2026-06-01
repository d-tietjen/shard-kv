#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    array_bulk, bulk, eq_ignore_ascii_case, error, int, parse_f64, parse_usize, wrong_arity,
    wrongtype,
};
#[cfg(feature = "server")]
use crate::commands::redis::{write_frame, write_resp_array_header, write_resp_null};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{
    EmbeddedStore, RedisObjectError, RedisObjectReadOutcome, RedisObjectResult,
    RedisObjectZSetRangeItem, RedisZSetStore,
};

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
define_geo_command!(GeoSearch, GEOSEARCH_COMMAND, "GEOSEARCH", false);
define_geo_command!(
    GeoSearchStore,
    GEOSEARCHSTORE_COMMAND,
    "GEOSEARCHSTORE",
    true
);

impl crate::commands::redis::RedisCommand for GeoAdd {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        geoadd_update(store, args)
            .map(int)
            .unwrap_or_else(|frame| frame)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_geo_result(out, geoadd_update(store, args), |out, inserted| {
            ServerWire::write_resp_integer(out, inserted);
        });
    }
}

impl crate::commands::redis::RedisCommand for GeoDist {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match geodist_value(store, args) {
            Ok(Some(value)) => bulk(value.into_bytes()),
            Ok(None) => Frame::Null,
            Err(frame) => frame,
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match geodist_value(store, args) {
            Ok(Some(value)) => ServerWire::write_resp_blob_string(out, value.as_bytes()),
            Ok(None) => write_resp_null(out),
            Err(frame) => write_frame(out, &frame),
        }
    }
}

impl crate::commands::redis::RedisCommand for GeoHash {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match geohash_scores(store, args) {
            Ok(scores) => Frame::Array(
                scores
                    .into_iter()
                    .map(|score| score.map_or(Frame::Null, |score| bulk(geohash_string(score))))
                    .collect(),
            ),
            Err(frame) => frame,
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match geohash_scores(store, args) {
            Ok(scores) => write_geohash_scores_resp(out, &scores),
            Err(frame) => write_frame(out, &frame),
        }
    }
}

impl crate::commands::redis::RedisCommand for GeoPos {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match geopos_positions(store, args) {
            Ok(positions) => Frame::Array(
                positions
                    .into_iter()
                    .map(|position| position.map_or(Frame::Null, coord_frame))
                    .collect(),
            ),
            Err(frame) => frame,
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match geopos_positions(store, args) {
            Ok(positions) => write_geopos_positions_resp(out, &positions),
            Err(frame) => write_frame(out, &frame),
        }
    }
}

impl crate::commands::redis::RedisCommand for GeoRadius {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        georadius(store, args, false, false)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_georadius_resp(store, args, false, false, out);
    }
}

impl crate::commands::redis::RedisCommand for GeoRadiusRo {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        georadius(store, args, false, true)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_georadius_resp(store, args, false, true, out);
    }
}

impl crate::commands::redis::RedisCommand for GeoRadiusByMember {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        georadius(store, args, true, false)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_georadius_resp(store, args, true, false, out);
    }
}

impl crate::commands::redis::RedisCommand for GeoRadiusByMemberRo {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        georadius(store, args, true, true)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_georadius_resp(store, args, true, true, out);
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
        match raw {
            raw if eq_ignore_ascii_case(raw, b"m") => Some(Self { meters: 1.0 }),
            raw if eq_ignore_ascii_case(raw, b"km") => Some(Self { meters: 1_000.0 }),
            raw if eq_ignore_ascii_case(raw, b"mi") => Some(Self { meters: 1_609.344 }),
            raw if eq_ignore_ascii_case(raw, b"ft") => Some(Self { meters: 0.3048 }),
            _ => None,
        }
    }

    fn to_meters(self, value: f64) -> f64 {
        value * self.meters
    }

    fn meters_to_unit(self, value: f64) -> f64 {
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

fn geoadd_update(store: &EmbeddedStore, args: &[&[u8]]) -> Result<i64, Frame> {
    if args.len() < 4 || !(args.len() - 1).is_multiple_of(3) {
        return Err(wrong_arity("GEOADD"));
    }
    let key = args[0];
    let mut entries = Vec::with_capacity((args.len() - 1) / 3);
    for chunk in args[1..].chunks_exact(3) {
        let (Ok(lon), Ok(lat)) = (parse_f64(chunk[0]), parse_f64(chunk[1])) else {
            return Err(error("ERR invalid longitude,latitude pair"));
        };
        let Some(score) = encode_geo_score(lon, lat) else {
            return Err(error("ERR invalid longitude,latitude pair"));
        };
        entries.push((score, chunk[2]));
    }
    let mut inserted = 0;
    for (score, member) in entries {
        match store.zadd(key, score, member) {
            RedisObjectResult::Integer(value) => inserted += value,
            RedisObjectResult::WrongType => return Err(wrongtype()),
            _ => {}
        }
    }
    Ok(inserted)
}

fn geodist_value(store: &EmbeddedStore, args: &[&[u8]]) -> Result<Option<String>, Frame> {
    let (key, left, right, unit) = match args {
        [key, left, right] => (*key, *left, *right, b"m".as_slice()),
        [key, left, right, unit] => (*key, *left, *right, *unit),
        _ => return Err(wrong_arity("GEODIST")),
    };
    let Some(unit) = GeoUnit::parse(unit) else {
        return Err(error(
            "ERR unsupported unit provided. please use M, KM, FT, MI",
        ));
    };
    let left = match member_position(store, key, left) {
        Ok(Some(position)) => position,
        Ok(None) => return Ok(None),
        Err(frame) => return Err(frame),
    };
    let right = match member_position(store, key, right) {
        Ok(Some(position)) => position,
        Ok(None) => return Ok(None),
        Err(frame) => return Err(frame),
    };
    Ok(Some(format_float(
        unit.meters_to_unit(distance_m(left, right)),
    )))
}

fn geohash_scores(store: &EmbeddedStore, args: &[&[u8]]) -> Result<Vec<Option<f64>>, Frame> {
    let [key, members @ ..] = args else {
        return Err(wrong_arity("GEOHASH"));
    };
    if members.is_empty() {
        return Err(wrong_arity("GEOHASH"));
    }
    let mut scores = Vec::with_capacity(members.len());
    for member in members {
        scores.push(member_score(store, key, member)?);
    }
    Ok(scores)
}

fn geopos_positions(store: &EmbeddedStore, args: &[&[u8]]) -> Result<Vec<Option<Position>>, Frame> {
    let [key, members @ ..] = args else {
        return Err(wrong_arity("GEOPOS"));
    };
    if members.is_empty() {
        return Err(wrong_arity("GEOPOS"));
    }
    let mut positions = Vec::with_capacity(members.len());
    for member in members {
        positions.push(member_position(store, key, member)?);
    }
    Ok(positions)
}

fn georadius(store: &EmbeddedStore, args: &[&[u8]], by_member: bool, read_only: bool) -> Frame {
    match georadius_result(store, args, by_member, read_only) {
        Ok(GeoRadiusResult::Empty) => Frame::Array(Vec::new()),
        Ok(GeoRadiusResult::Stored(count)) => int(count as i64),
        Ok(GeoRadiusResult::Hits {
            hits,
            unit,
            options,
        }) => radius_response(hits, unit, &options),
        Err(frame) => frame,
    }
}

enum GeoRadiusResult<'a> {
    Empty,
    Stored(usize),
    Hits {
        hits: Vec<GeoHit>,
        unit: GeoUnit,
        options: GeoRadiusOptions<'a>,
    },
}

struct GeoRadiusQuery<'a> {
    key: &'a [u8],
    center: Position,
    radius_m: f64,
    unit: GeoUnit,
    options: GeoRadiusOptions<'a>,
}

enum GeoRadiusQueryResult<'a> {
    Empty,
    Query(GeoRadiusQuery<'a>),
}

fn georadius_result<'a>(
    store: &EmbeddedStore,
    args: &'a [&'a [u8]],
    by_member: bool,
    read_only: bool,
) -> Result<GeoRadiusResult<'a>, Frame> {
    let query = match georadius_query(store, args, by_member, read_only)? {
        GeoRadiusQueryResult::Empty => return Ok(GeoRadiusResult::Empty),
        GeoRadiusQueryResult::Query(query) => query,
    };
    let mut hits = geo_hits(store, query.key, query.center, query.radius_m)?;
    sort_and_limit_hits(&mut hits, query.options.sort, query.options.count);
    if let Some((dest, store_dist)) = query.options.store {
        store.delete(dest);
        for hit in &hits {
            let score = if store_dist {
                query.unit.meters_to_unit(hit.distance_m)
            } else {
                hit.score
            };
            let result = store.zadd(dest, score, &hit.member);
            if matches!(result, RedisObjectResult::WrongType) {
                return Err(wrongtype());
            }
        }
        return Ok(GeoRadiusResult::Stored(hits.len()));
    }
    Ok(GeoRadiusResult::Hits {
        hits,
        unit: query.unit,
        options: query.options,
    })
}

fn georadius_query<'a>(
    store: &EmbeddedStore,
    args: &'a [&'a [u8]],
    by_member: bool,
    read_only: bool,
) -> Result<GeoRadiusQueryResult<'a>, Frame> {
    let min_len = if by_member { 4 } else { 5 };
    if args.len() < min_len {
        return Err(wrong_arity(if by_member {
            "GEORADIUSBYMEMBER"
        } else {
            "GEORADIUS"
        }));
    }
    let key = args[0];
    let (center, radius, unit, option_start) = if by_member {
        let center = match member_position(store, key, args[1]) {
            Ok(Some(position)) => position,
            Ok(None) => return Ok(GeoRadiusQueryResult::Empty),
            Err(frame) => return Err(frame),
        };
        let (Ok(radius), Some(unit)) = (parse_f64(args[2]), GeoUnit::parse(args[3])) else {
            return Err(error(
                "ERR unsupported unit provided. please use M, KM, FT, MI",
            ));
        };
        (center, radius, unit, 4)
    } else {
        let (Ok(lon), Ok(lat), Ok(radius), Some(unit)) = (
            parse_f64(args[1]),
            parse_f64(args[2]),
            parse_f64(args[3]),
            GeoUnit::parse(args[4]),
        ) else {
            return Err(error("ERR invalid longitude,latitude pair or radius"));
        };
        let Some(score) = encode_geo_score(lon, lat) else {
            return Err(error("ERR invalid longitude,latitude pair"));
        };
        (decode_geo_score(score), radius, unit, 5)
    };
    let options = parse_radius_options(&args[option_start..], read_only)?;
    Ok(GeoRadiusQueryResult::Query(GeoRadiusQuery {
        key,
        center,
        radius_m: unit.to_meters(radius),
        unit,
        options,
    }))
}

#[cfg(feature = "server")]
fn write_geo_result<T>(
    out: &mut BytesMut,
    result: Result<T, Frame>,
    write_ok: impl FnOnce(&mut BytesMut, T),
) {
    match result {
        Ok(value) => write_ok(out, value),
        Err(frame) => write_frame(out, &frame),
    }
}

#[cfg(feature = "server")]
fn write_geohash_scores_resp(out: &mut BytesMut, scores: &[Option<f64>]) {
    write_resp_array_header(out, scores.len());
    for score in scores {
        match score {
            Some(score) => {
                let hash = geohash_string(*score);
                ServerWire::write_resp_blob_string(out, &hash);
            }
            None => write_resp_null(out),
        }
    }
}

#[cfg(feature = "server")]
fn write_geopos_positions_resp(out: &mut BytesMut, positions: &[Option<Position>]) {
    write_resp_array_header(out, positions.len());
    for position in positions {
        match position {
            Some(position) => write_coord_resp(out, *position),
            None => write_resp_null(out),
        }
    }
}

#[cfg(feature = "server")]
fn write_georadius_resp(
    store: &EmbeddedStore,
    args: &[&[u8]],
    by_member: bool,
    read_only: bool,
    out: &mut BytesMut,
) {
    if try_write_georadius_streaming_resp(store, args, by_member, read_only, out) {
        return;
    }
    match georadius_result(store, args, by_member, read_only) {
        Ok(GeoRadiusResult::Empty) => write_resp_array_header(out, 0),
        Ok(GeoRadiusResult::Stored(count)) => ServerWire::write_resp_integer(out, count as i64),
        Ok(GeoRadiusResult::Hits {
            hits,
            unit,
            options,
        }) => write_radius_response_resp(out, &hits, unit, &options),
        Err(frame) => write_frame(out, &frame),
    }
}

#[cfg(feature = "server")]
fn try_write_georadius_streaming_resp(
    store: &EmbeddedStore,
    args: &[&[u8]],
    by_member: bool,
    read_only: bool,
    out: &mut BytesMut,
) -> bool {
    let query = match georadius_query(store, args, by_member, read_only) {
        Ok(GeoRadiusQueryResult::Empty) => {
            write_resp_array_header(out, 0);
            return true;
        }
        Ok(GeoRadiusQueryResult::Query(query)) => query,
        Err(frame) => {
            write_frame(out, &frame);
            return true;
        }
    };
    if query.options.sort != SortOrder::None
        || query.options.count.is_some()
        || query.options.store.is_some()
    {
        return false;
    }
    write_radius_scan_resp(
        store,
        query.key,
        query.center,
        query.radius_m,
        query.unit,
        &query.options,
        out,
    );
    true
}

#[cfg(feature = "server")]
fn write_radius_scan_resp(
    store: &EmbeddedStore,
    key: &[u8],
    center: Position,
    radius_m: f64,
    unit: GeoUnit,
    options: &GeoRadiusOptions<'_>,
    out: &mut BytesMut,
) {
    let mut items = BytesMut::new();
    let mut count = 0usize;
    match store.zrange_entries_visit(key, 0, -1, false, |item| match item {
        RedisObjectZSetRangeItem::Begin(total) => items.reserve(total.min(64).saturating_mul(16)),
        RedisObjectZSetRangeItem::Entry { member, score } => {
            let position = decode_geo_score(score);
            let distance_m = distance_m(center, position);
            if distance_m <= radius_m {
                count += 1;
                write_radius_hit_resp(
                    &mut items, member, score, position, distance_m, unit, options,
                );
            }
        }
    }) {
        RedisObjectReadOutcome::Written => {
            write_resp_array_header(out, count);
            out.extend_from_slice(&items);
        }
        RedisObjectReadOutcome::Missing => write_resp_array_header(out, 0),
        RedisObjectReadOutcome::WrongType => write_frame(out, &wrongtype()),
    }
}

#[cfg(feature = "server")]
fn write_radius_response_resp(
    out: &mut BytesMut,
    hits: &[GeoHit],
    unit: GeoUnit,
    options: &GeoRadiusOptions<'_>,
) {
    write_resp_array_header(out, hits.len());
    for hit in hits {
        write_radius_hit_resp(
            out,
            &hit.member,
            hit.score,
            hit.position,
            hit.distance_m,
            unit,
            options,
        );
    }
}

#[cfg(feature = "server")]
fn write_radius_hit_resp(
    out: &mut BytesMut,
    member: &[u8],
    score: f64,
    position: Position,
    distance_m: f64,
    unit: GeoUnit,
    options: &GeoRadiusOptions<'_>,
) {
    if !(options.with_coord || options.with_dist || options.with_hash) {
        ServerWire::write_resp_blob_string(out, member);
        return;
    }
    let len = 1
        + usize::from(options.with_dist)
        + usize::from(options.with_hash)
        + usize::from(options.with_coord);
    write_resp_array_header(out, len);
    ServerWire::write_resp_blob_string(out, member);
    if options.with_dist {
        let distance = format_float(unit.meters_to_unit(distance_m));
        ServerWire::write_resp_blob_string(out, distance.as_bytes());
    }
    if options.with_hash {
        ServerWire::write_resp_integer(out, score as i64);
    }
    if options.with_coord {
        write_coord_resp(out, position);
    }
}

#[cfg(feature = "server")]
fn write_coord_resp(out: &mut BytesMut, position: Position) {
    write_resp_array_header(out, 2);
    let lon = format_float(position.lon);
    ServerWire::write_resp_blob_string(out, lon.as_bytes());
    let lat = format_float(position.lat);
    ServerWire::write_resp_blob_string(out, lat.as_bytes());
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
        match option {
            option if eq_ignore_ascii_case(option, b"WITHCOORD") => {
                options.with_coord = true;
                index += 1;
            }
            option if eq_ignore_ascii_case(option, b"WITHDIST") => {
                options.with_dist = true;
                index += 1;
            }
            option if eq_ignore_ascii_case(option, b"WITHHASH") => {
                options.with_hash = true;
                index += 1;
            }
            option if eq_ignore_ascii_case(option, b"ASC") => {
                options.sort = SortOrder::Asc;
                index += 1;
            }
            option if eq_ignore_ascii_case(option, b"DESC") => {
                options.sort = SortOrder::Desc;
                index += 1;
            }
            option if eq_ignore_ascii_case(option, b"COUNT") => {
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
            }
            option
                if eq_ignore_ascii_case(option, b"STORE")
                    || eq_ignore_ascii_case(option, b"STOREDIST") =>
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
            }
            _ => return Err(error("ERR syntax error")),
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
    let mut hits = Vec::new();
    match store.zrange_entries_visit(key, 0, -1, false, |item| match item {
        RedisObjectZSetRangeItem::Begin(count) => hits.reserve(count.min(64)),
        RedisObjectZSetRangeItem::Entry { member, score } => {
            let position = decode_geo_score(score);
            let distance_m = distance_m(center, position);
            if distance_m <= radius_m {
                hits.push(GeoHit {
                    member: member.to_vec(),
                    score,
                    position,
                    distance_m,
                });
            }
        }
    }) {
        RedisObjectReadOutcome::Written => Ok(hits),
        RedisObjectReadOutcome::Missing => Ok(Vec::new()),
        RedisObjectReadOutcome::WrongType => Err(wrongtype()),
    }
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
                        format_float(unit.meters_to_unit(hit.distance_m)).into_bytes(),
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

impl crate::commands::redis::RedisCommand for GeoSearch {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        geo_search(store, args, false)
    }
}

impl crate::commands::redis::RedisCommand for GeoSearchStore {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        geo_search(store, args, true)
    }
}

/// One of the two GEOSEARCH search shapes.
enum SearchShape {
    Radius(f64),
    Box { width_m: f64, height_m: f64 },
}

/// Parsed GEOSEARCH / GEOSEARCHSTORE request.
struct SearchRequest<'a> {
    key: &'a [u8],
    center: Position,
    shape: SearchShape,
    unit: GeoUnit,
    options: GeoRadiusOptions<'a>,
    store_dest: Option<&'a [u8]>,
    store_dist: bool,
}

/// GEOSEARCH (read) / GEOSEARCHSTORE (write) — Redis 6.2 unified geo queries.
///
/// GEOSEARCH key <FROMMEMBER m | FROMLONLAT lon lat>
///   <BYRADIUS r unit | BYBOX w h unit> [ASC|DESC] [COUNT n [ANY]]
///   [WITHCOORD] [WITHDIST] [WITHHASH]
///
/// GEOSEARCHSTORE dest src <same FROM/BY ...> [ASC|DESC] [COUNT n [ANY]]
///   [STOREDIST]
fn geo_search(store: &EmbeddedStore, args: &[&[u8]], store_variant: bool) -> Frame {
    let request = match parse_search_request(store, args, store_variant) {
        Ok(Some(request)) => request,
        Ok(None) => {
            // Missing source key: empty result (or empty store).
            return if store_variant {
                int(0)
            } else {
                Frame::Array(Vec::new())
            };
        }
        Err(frame) => return frame,
    };

    let mut hits = match &request.shape {
        SearchShape::Radius(radius_m) => {
            match geo_hits(store, request.key, request.center, *radius_m) {
                Ok(hits) => hits,
                Err(frame) => return frame,
            }
        }
        SearchShape::Box { width_m, height_m } => {
            match geo_box_hits(store, request.key, request.center, *width_m, *height_m) {
                Ok(hits) => hits,
                Err(frame) => return frame,
            }
        }
    };

    sort_and_limit_hits(&mut hits, request.options.sort, request.options.count);

    if let Some(dest) = request.store_dest {
        store_search_hits(store, dest, hits, &request)
    } else {
        radius_response(hits, request.unit, &request.options)
    }
}

fn sort_and_limit_hits(hits: &mut Vec<GeoHit>, sort: SortOrder, count: Option<usize>) {
    match sort {
        SortOrder::Asc => hits.sort_by(|a, b| a.distance_m.total_cmp(&b.distance_m)),
        SortOrder::Desc => hits.sort_by(|a, b| b.distance_m.total_cmp(&a.distance_m)),
        SortOrder::None => {}
    }
    if let Some(count) = count {
        hits.truncate(count);
    }
}

/// Sort + truncate then write the hits into `dest` as a sorted set, mirroring
/// the GEORADIUS STORE / STOREDIST behavior.
fn store_search_hits(
    store: &EmbeddedStore,
    dest: &[u8],
    hits: Vec<GeoHit>,
    request: &SearchRequest<'_>,
) -> Frame {
    store.delete(dest);
    for hit in &hits {
        let score = if request.store_dist {
            request.unit.meters_to_unit(hit.distance_m)
        } else {
            hit.score
        };
        if matches!(
            store.zadd(dest, score, &hit.member),
            RedisObjectResult::WrongType
        ) {
            return wrongtype();
        }
    }
    int(hits.len() as i64)
}

/// Collect members whose position falls inside the axis-aligned box centered on
/// `center` with the given width (longitude span) and height (latitude span).
fn geo_box_hits(
    store: &EmbeddedStore,
    key: &[u8],
    center: Position,
    width_m: f64,
    height_m: f64,
) -> Result<Vec<GeoHit>, Frame> {
    let half_width = width_m / 2.0;
    let half_height = height_m / 2.0;
    let mut hits = Vec::new();
    match store.zrange_entries_visit(key, 0, -1, false, |item| match item {
        RedisObjectZSetRangeItem::Begin(count) => hits.reserve(count.min(64)),
        RedisObjectZSetRangeItem::Entry { member, score } => {
            let position = decode_geo_score(score);
            // Distance along the longitude axis (same latitude as the center) and
            // along the latitude axis (same longitude as the center).
            let lon_distance = distance_m(
                center,
                Position {
                    lon: position.lon,
                    lat: center.lat,
                },
            );
            let lat_distance = distance_m(
                center,
                Position {
                    lon: center.lon,
                    lat: position.lat,
                },
            );
            if lon_distance <= half_width && lat_distance <= half_height {
                hits.push(GeoHit {
                    member: member.to_vec(),
                    score,
                    position,
                    distance_m: distance_m(center, position),
                });
            }
        }
    }) {
        RedisObjectReadOutcome::Written => Ok(hits),
        RedisObjectReadOutcome::Missing => Ok(Vec::new()),
        RedisObjectReadOutcome::WrongType => Err(wrongtype()),
    }
}

fn parse_search_request<'a>(
    store: &EmbeddedStore,
    args: &'a [&'a [u8]],
    store_variant: bool,
) -> Result<Option<SearchRequest<'a>>, Frame> {
    let name = if store_variant {
        "GEOSEARCHSTORE"
    } else {
        "GEOSEARCH"
    };
    let mut cursor = 0;

    let store_dest = if store_variant {
        let dest = *args.get(cursor).ok_or_else(|| wrong_arity(name))?;
        cursor += 1;
        Some(dest)
    } else {
        None
    };
    let key = *args.get(cursor).ok_or_else(|| wrong_arity(name))?;
    cursor += 1;

    let mut center: Option<Position> = None;
    let mut shape: Option<SearchShape> = None;
    let mut unit = GeoUnit { meters: 1.0 };
    let mut options = GeoRadiusOptions {
        with_coord: false,
        with_dist: false,
        with_hash: false,
        count: None,
        sort: SortOrder::None,
        store: None,
    };
    let mut store_dist = false;
    let mut member_missing = false;

    while cursor < args.len() {
        let option = args[cursor];
        cursor += 1;
        match option {
            option if eq_ignore_ascii_case(option, b"FROMMEMBER") => {
                if center.is_some() || member_missing {
                    return Err(error(
                        "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH",
                    ));
                }
                let member = *args.get(cursor).ok_or_else(|| error("ERR syntax error"))?;
                cursor += 1;
                match member_position(store, key, member)? {
                    Some(position) => center = Some(position),
                    None => member_missing = true,
                }
            }
            option if eq_ignore_ascii_case(option, b"FROMLONLAT") => {
                if center.is_some() || member_missing {
                    return Err(error(
                        "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH",
                    ));
                }
                let lon = parse_required_f64(args.get(cursor).copied())?;
                let lat = parse_required_f64(args.get(cursor + 1).copied())?;
                cursor += 2;
                let Some(score) = encode_geo_score(lon, lat) else {
                    return Err(error("ERR invalid longitude,latitude pair"));
                };
                center = Some(decode_geo_score(score));
            }
            option if eq_ignore_ascii_case(option, b"BYRADIUS") => {
                if shape.is_some() {
                    return Err(error(
                        "ERR exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH",
                    ));
                }
                let radius = parse_required_f64(args.get(cursor).copied())?;
                let parsed_unit = parse_required_unit(args.get(cursor + 1).copied())?;
                cursor += 2;
                unit = parsed_unit;
                shape = Some(SearchShape::Radius(parsed_unit.to_meters(radius)));
            }
            option if eq_ignore_ascii_case(option, b"BYBOX") => {
                if shape.is_some() {
                    return Err(error(
                        "ERR exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH",
                    ));
                }
                let width = parse_required_f64(args.get(cursor).copied())?;
                let height = parse_required_f64(args.get(cursor + 1).copied())?;
                let parsed_unit = parse_required_unit(args.get(cursor + 2).copied())?;
                cursor += 3;
                unit = parsed_unit;
                shape = Some(SearchShape::Box {
                    width_m: parsed_unit.to_meters(width),
                    height_m: parsed_unit.to_meters(height),
                });
            }
            option if eq_ignore_ascii_case(option, b"ASC") => options.sort = SortOrder::Asc,
            option if eq_ignore_ascii_case(option, b"DESC") => options.sort = SortOrder::Desc,
            option if eq_ignore_ascii_case(option, b"COUNT") => {
                let raw = *args.get(cursor).ok_or_else(|| error("ERR syntax error"))?;
                cursor += 1;
                let parsed = std::str::from_utf8(raw)
                    .ok()
                    .and_then(|text| text.parse::<usize>().ok())
                    .filter(|value| *value > 0)
                    .ok_or_else(|| error("ERR COUNT must be > 0"))?;
                options.count = Some(parsed);
                if args
                    .get(cursor)
                    .is_some_and(|next| eq_ignore_ascii_case(next, b"ANY"))
                {
                    cursor += 1;
                }
            }
            option if !store_variant && eq_ignore_ascii_case(option, b"WITHCOORD") => {
                options.with_coord = true;
            }
            option if !store_variant && eq_ignore_ascii_case(option, b"WITHDIST") => {
                options.with_dist = true;
            }
            option if !store_variant && eq_ignore_ascii_case(option, b"WITHHASH") => {
                options.with_hash = true;
            }
            option if store_variant && eq_ignore_ascii_case(option, b"STOREDIST") => {
                store_dist = true;
            }
            _ => return Err(error("ERR syntax error")),
        }
    }

    let Some(shape) = shape else {
        return Err(error(
            "ERR exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH",
        ));
    };
    if center.is_none() && !member_missing {
        return Err(error(
            "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH",
        ));
    }
    if member_missing {
        // FROMMEMBER referenced a member that does not exist: empty result.
        return Ok(None);
    }
    let center = center.expect("center resolved");

    Ok(Some(SearchRequest {
        key,
        center,
        shape,
        unit,
        options,
        store_dest,
        store_dist,
    }))
}

fn parse_required_f64(raw: Option<&[u8]>) -> Result<f64, Frame> {
    raw.and_then(|bytes| parse_f64(bytes).ok())
        .ok_or_else(|| error("ERR syntax error"))
}

fn parse_required_unit(raw: Option<&[u8]>) -> Result<GeoUnit, Frame> {
    raw.and_then(GeoUnit::parse)
        .ok_or_else(|| error("ERR unsupported unit provided. please use M, KM, FT, MI"))
}
