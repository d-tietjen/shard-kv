//! Native semantic-cache server commands.

#[cfg(feature = "server")]
use crate::commands::CommandSpec;
#[cfg(feature = "server")]
use crate::server::commands::{RawCommandContext, RawDirectCommand};
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;

#[cfg(feature = "server")]
pub(crate) struct SemanticSet;
#[cfg(feature = "server")]
pub(crate) struct SemanticSearch;

#[cfg(feature = "server")]
pub(crate) static SEMANTIC_SET_COMMAND: SemanticSet = SemanticSet;
#[cfg(feature = "server")]
pub(crate) static SEMANTIC_SEARCH_COMMAND: SemanticSearch = SemanticSearch;

#[cfg(feature = "server")]
impl CommandSpec for SemanticSet {
    const NAME: &'static str = "SEMANTIC.SET";
    const MUTATES_VALUE: bool = true;
}

#[cfg(feature = "server")]
impl CommandSpec for SemanticSearch {
    const NAME: &'static str = "SEMANTIC.SEARCH";
    const MUTATES_VALUE: bool = false;
}

#[cfg(feature = "server")]
impl RawDirectCommand for SemanticSet {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_, '_>) {
        let args = ctx.args.as_slice();
        if !matches!(args.len(), 3 | 4) {
            ServerWire::write_resp_error(
                ctx.out,
                "ERR wrong number of arguments for 'SEMANTIC.SET' command",
            );
            return;
        }

        let embedding = match parse_embedding_arg(args[2]) {
            Ok(embedding) => embedding,
            Err(message) => {
                ServerWire::write_resp_error(ctx.out, &message);
                return;
            }
        };

        let result = match args.get(3).copied() {
            Some(governance) => ctx
                .store
                .set_semantic_slice_with_governance(args[0], args[1], &embedding, None, governance),
            None => ctx
                .store
                .set_semantic_slice(args[0], args[1], &embedding, None),
        };
        match result {
            Ok(()) => ctx.out.extend_from_slice(b"+OK\r\n"),
            Err(error) => ServerWire::write_resp_error(ctx.out, &format!("ERR {error}")),
        }
    }
}

#[cfg(feature = "server")]
impl RawDirectCommand for SemanticSearch {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_, '_>) {
        let args = ctx.args.as_slice();
        if args.len() != 2 {
            ServerWire::write_resp_error(
                ctx.out,
                "ERR wrong number of arguments for 'SEMANTIC.SEARCH' command",
            );
            return;
        }

        let embedding = match parse_embedding_arg(args[0]) {
            Ok(embedding) => embedding,
            Err(message) => {
                ServerWire::write_resp_error(ctx.out, &message);
                return;
            }
        };
        let min_score = match parse_min_score(args[1]) {
            Some(score) => score,
            None => {
                ServerWire::write_resp_error(
                    ctx.out,
                    "ERR SEMANTIC.SEARCH min_score must be a finite f32",
                );
                return;
            }
        };

        match ctx.store.semantic_search(&embedding, min_score) {
            Ok(Some(hit)) => write_semantic_match(ctx.out, &hit),
            Ok(None) => ServerWire::write_resp_null(ctx.out, ctx.resp_protocol),
            Err(error) => ServerWire::write_resp_error(ctx.out, &format!("ERR {error}")),
        }
    }
}

#[cfg(feature = "server")]
fn parse_embedding_arg(raw: &[u8]) -> Result<Vec<f32>, String> {
    if raw.is_empty() || !raw.len().is_multiple_of(std::mem::size_of::<f32>()) {
        return Err("ERR semantic embedding must be non-empty little-endian f32 bytes".into());
    }
    Ok(raw
        .chunks_exact(std::mem::size_of::<f32>())
        .map(|chunk| f32::from_le_bytes(chunk.try_into().expect("chunk has four bytes")))
        .collect())
}

#[cfg(feature = "server")]
fn parse_min_score(raw: &[u8]) -> Option<f32> {
    let text = std::str::from_utf8(raw).ok()?;
    let value = text.parse::<f32>().ok()?;
    value.is_finite().then_some(value)
}

#[cfg(feature = "server")]
fn write_semantic_match(out: &mut bytes::BytesMut, hit: &crate::storage::SemanticMatch) {
    ServerWire::write_resp_array_header(out, 4);
    ServerWire::write_resp_blob_string(out, &hit.key);
    ServerWire::write_resp_blob_string(out, &hit.value);
    ServerWire::write_resp_blob_string(out, hit.score.to_string().as_bytes());
    match hit.governance.as_deref() {
        Some(governance) => ServerWire::write_resp_blob_string(out, governance),
        None => ServerWire::write_resp_null(out, crate::server::wire::RespProtocolVersion::Resp2),
    }
}
