use super::*;

pub(super) struct HandoffConfig;

impl HandoffConfig {
    pub(super) fn buffer() -> HandoffBufferConfig {
        HandoffBufferConfig::new(HANDOFF_BUFFER_MAX).with_read_reserve(READ_CHUNK_SIZE)
    }

    pub(super) fn write() -> WriteHandoffConfig {
        WriteHandoffConfig::new(WRITE_HANDOFF_MAX_ITEMS, WRITE_HANDOFF_MAX_PENDING_BYTES)
    }
}

pub(super) struct ConnectionRejector;

impl ConnectionRejector {
    pub(super) async fn reject<S>(mut stream: S) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let mut bytes = Vec::new();
        RespCodec::encode(
            &Frame::Error("ERR max connections reached".into()),
            &mut bytes,
        );
        stream.write_all(&bytes).await?;
        let _ = stream.shutdown().await;
        Ok(())
    }
}

pub(super) struct EngineConnection;

impl EngineConnection {
    pub(super) async fn handle<R>(
        mut read_half: R,
        write_handoff: WriteHandoff,
        engine: EngineHandle,
        _permit: OwnedSemaphorePermit,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin,
    {
        let mut frame_buffer = HandoffBuffer::with_config(HandoffConfig::buffer());

        loop {
            let read = frame_buffer
                .read_available(&mut read_half)
                .await
                .map_err(|error| {
                    crate::FastCacheError::Protocol(format!("handoff read error: {error}"))
                })?;
            if read == 0 {
                return Ok(());
            }

            let mut write_buffer = Vec::with_capacity(CONNECTION_BUFFER_CAPACITY);
            let mut consumed_total = 0usize;

            while let Some(first_byte) = frame_buffer.peek().get(consumed_total).copied() {
                if FastCodec::is_fast_request_prefix(first_byte) {
                    let slice = &frame_buffer.peek()[consumed_total..];
                    let decoded = FastCodec::decode_request(slice)?;
                    let Some((request, consumed)) = decoded else {
                        break;
                    };
                    consumed_total += consumed;
                    let response = match engine.execute_fast(request).await {
                        Ok(response) => response,
                        Err(error) => FastResponse::Error(format!("ERR {error}").into_bytes()),
                    };
                    FastCodec::encode_response(&response, &mut write_buffer);
                } else {
                    let spanned_consumed = {
                        let slice = &frame_buffer.peek()[consumed_total..];
                        let decoded = RespCodec::decode_command(slice)?;
                        let Some((frame, consumed)) = decoded else {
                            break;
                        };

                        match BorrowedCommand::from_frame(frame) {
                            Ok(command) => {
                                if EngineHandle::should_use_spanned_resp(&command) {
                                    Some(consumed)
                                } else {
                                    consumed_total += consumed;
                                    if let Err(error) = engine
                                        .execute_resp_borrowed_into(command, &mut write_buffer)
                                        .await
                                    {
                                        RespCodec::encode(
                                            &Frame::Error(format!("ERR {error}")),
                                            &mut write_buffer,
                                        );
                                    }
                                    None
                                }
                            }
                            Err(error) => {
                                consumed_total += consumed;
                                RespCodec::encode(
                                    &Frame::Error(format!("ERR {error}")),
                                    &mut write_buffer,
                                );
                                None
                            }
                        }
                    };

                    if let Some(consumed) = spanned_consumed {
                        if consumed_total > 0 {
                            frame_buffer.advance(consumed_total).map_err(|error| {
                                crate::FastCacheError::Protocol(format!(
                                    "handoff advance error: {error}"
                                ))
                            })?;
                            consumed_total = 0;
                            continue;
                        }
                        let Some((span_frame, span_consumed)) =
                            RespCodec::decode_command_spans(frame_buffer.peek())?
                        else {
                            break;
                        };
                        if span_consumed != consumed {
                            return Err(crate::FastCacheError::Protocol(
                                "command span length changed during handoff".into(),
                            ));
                        }
                        let owner = frame_buffer.split_prefix(consumed).map_err(|error| {
                            crate::FastCacheError::Protocol(format!("handoff split error: {error}"))
                        })?;
                        if let Err(error) = engine
                            .execute_resp_spanned_into(span_frame, owner, &mut write_buffer)
                            .await
                        {
                            RespCodec::encode(
                                &Frame::Error(format!("ERR {error}")),
                                &mut write_buffer,
                            );
                        }
                    }
                }
            }

            if !write_buffer.is_empty() {
                WriteSubmitter::submit(&write_handoff, write_buffer).await?;
            }
            if consumed_total > 0 {
                frame_buffer.advance(consumed_total).map_err(|error| {
                    crate::FastCacheError::Protocol(format!("handoff advance error: {error}"))
                })?;
            }
        }
    }
}

pub(super) struct WriteSubmitter;

impl WriteSubmitter {
    #[inline]
    pub(super) async fn submit(handoff: &WriteHandoff, buf: Vec<u8>) -> Result<()> {
        let bytes = bytes::Bytes::from(buf);
        handoff
            .write_fire_and_forget(bytes)
            .await
            .map_err(|error| {
                crate::FastCacheError::Protocol(format!("write handoff error: {error}"))
            })?;
        Ok(())
    }
}

pub(super) struct SnapshotTask;

impl SnapshotTask {
    pub(super) fn spawn(engine: EngineHandle) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(engine.snapshot_interval());
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            let mut last_writes = 0_u64;

            loop {
                ticker.tick().await;
                let Ok(stats) = engine.stats_snapshot().await else {
                    continue;
                };
                if stats.total_writes.saturating_sub(last_writes) < engine.snapshot_min_writes() {
                    continue;
                }
                match engine.snapshot().await {
                    Ok(path) => {
                        last_writes = stats.total_writes;
                        tracing::info!("snapshot written to {}", path.display());
                    }
                    Err(error) => {
                        tracing::warn!("snapshot attempt failed: {error}");
                    }
                }
            }
        })
    }
}
