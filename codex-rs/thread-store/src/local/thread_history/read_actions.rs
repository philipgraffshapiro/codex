use super::*;
use crate::ThreadStore;
use codex_protocol::protocol::EventMsg;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutRecorder;

pub(in crate::local) async fn list_turns(
    store: &LocalThreadStore,
    params: ListTurnsParams,
) -> ThreadStoreResult<TurnPage> {
    validate_thread_for_paginated_reads(
        store,
        params.thread_id,
        params.include_archived,
        "list_turns",
    )
    .await?;
    validate_page_size(params.page_size)?;
    let lineage = store
        .resolve_rollout_lineage(params.thread_id, /*initial_path*/ None)
        .await?;
    let pool = store.thread_history_db().await?;
    let page = page_turn_rows(
        pool,
        params.thread_id,
        &lineage,
        params.cursor.as_deref(),
        params.page_size,
        params.sort_direction,
        params.items_view,
    )
    .await?;
    let mut turns = Vec::with_capacity(page.rows.len());
    for turn in page.rows {
        let items = match params.items_view {
            StoredTurnItemsView::NotLoaded => Vec::new(),
            StoredTurnItemsView::Summary
                if matches!(turn.status, StoredTurnStatus::Interrupted)
                    && turn.first_user_item_id.is_none()
                    && turn.final_agent_item_id.is_none() =>
            {
                // Synthetic fork-boundary rows are interrupted without local summary IDs.
                // Load their summary from the earliest visible source turn.
                load_inherited_summary_items(pool, &lineage, &turn).await?
            }
            StoredTurnItemsView::Summary => turn.summary_items,
        };
        turns.push(StoredTurn {
            turn_id: turn.turn_id,
            root_turn_id: turn.root_turn_id,
            items,
            items_view: params.items_view,
            status: turn.status,
            error: turn.error,
            started_at: turn.started_at,
            completed_at: turn.completed_at,
            duration_ms: turn.duration_ms,
        });
    }

    Ok(TurnPage {
        turns,
        next_cursor: page.next_cursor,
        backwards_cursor: page.backwards_cursor,
    })
}

pub(in crate::local) async fn list_items(
    store: &LocalThreadStore,
    params: ListItemsParams,
) -> ThreadStoreResult<ItemPage> {
    validate_thread_for_paginated_reads(
        store,
        params.thread_id,
        params.include_archived,
        "list_items",
    )
    .await?;
    validate_page_size(params.page_size)?;
    let lineage = store
        .resolve_rollout_lineage(params.thread_id, /*initial_path*/ None)
        .await?;
    let pool = store.thread_history_db().await?;
    let page = page_item_rows(pool, &lineage, &params).await?;

    Ok(ItemPage {
        items: page.rows.into_iter().map(|row| row.item).collect(),
        next_cursor: page.next_cursor,
        backwards_cursor: page.backwards_cursor,
    })
}

pub(in crate::local::thread_history) async fn validate_thread_for_paginated_reads(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    include_archived: bool,
    operation: &'static str,
) -> ThreadStoreResult<()> {
    let Some(state_db) = store.state_db().await else {
        return Err(ThreadStoreError::Unsupported { operation });
    };
    let metadata =
        state_db
            .get_thread(thread_id)
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to read thread metadata: {err}"),
            })?;
    let metadata = if let Some(metadata) = metadata {
        metadata
    } else {
        let thread = match store
            .read_thread(crate::ReadThreadParams {
                thread_id,
                include_archived,
                include_history: false,
            })
            .await
        {
            Ok(thread) => thread,
            Err(ThreadStoreError::InvalidRequest { message })
                if message == format!("no rollout found for thread id {thread_id}") =>
            {
                return Err(ThreadStoreError::Unsupported { operation });
            }
            Err(ThreadStoreError::ThreadNotFound { thread_id: missing })
                if missing == thread_id =>
            {
                return Err(ThreadStoreError::Unsupported { operation });
            }
            Err(error) => return Err(error),
        };
        if matches!(thread.history_mode, ThreadHistoryMode::Legacy) {
            return Err(ThreadStoreError::Unsupported { operation });
        }
        let Some(path) = thread.rollout_path else {
            return Err(ThreadStoreError::Unsupported { operation });
        };
        let (items, rollout_id, parse_errors) = RolloutRecorder::load_rollout_items(&path)
            .await
            .map_err(|error| ThreadStoreError::Internal {
                message: format!("failed to read rollout for thread {thread_id}: {error}"),
            })?;
        if rollout_id != Some(thread_id)
            || parse_errors != 0
            || items.iter().any(|item| {
                matches!(
                    item,
                    RolloutItem::EventMsg(EventMsg::TurnStarted(_) | EventMsg::TurnComplete(_))
                )
            })
        {
            return Err(ThreadStoreError::Unsupported { operation });
        }
        return Ok(());
    };
    if metadata.archived_at.is_some() && !include_archived {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!("thread {thread_id} is archived"),
        });
    }
    match metadata.history_mode {
        ThreadHistoryMode::Legacy => Err(ThreadStoreError::Unsupported { operation }),
        ThreadHistoryMode::Paginated => Ok(()),
    }
}

async fn load_inherited_summary_items(
    pool: &sqlx::SqlitePool,
    lineage: &RolloutLineage,
    turn: &StoredTurnRow,
) -> ThreadStoreResult<Vec<StoredThreadItem>> {
    let source = find_source_turn(pool, lineage, turn.turn_id.as_str()).await?;
    let Some(segment) = lineage
        .segments()
        .iter()
        .find(|segment| segment.rollout_id() == source.rollout_id)
    else {
        return Ok(Vec::new());
    };
    let start_ordinal = sqlite_integer(segment.start_ordinal(), "rollout ordinal")?;
    let end_ordinal = segment
        .end_ordinal()
        .map(|ordinal| sqlite_integer(ordinal, "rollout ordinal"))
        .transpose()?;
    let rows = sqlx::query(
        r#"
SELECT turn_id, item_id, updated_at_ordinal, created_at_ms, started_at_ms, completed_at_ms, item_json
FROM thread_items
WHERE thread_id = ?
  AND turn_id = ?
  AND rollout_ordinal >= ?
  AND (? IS NULL OR rollout_ordinal < ?)
  AND (item_id = ? OR item_id = ?)
ORDER BY rollout_ordinal ASC
        "#,
    )
    .bind(source.rollout_id.to_string())
    .bind(turn.turn_id.as_str())
    .bind(start_ordinal)
    .bind(end_ordinal)
    .bind(end_ordinal)
    .bind(source.first_user_item_id)
    .bind(source.final_agent_item_id)
    .fetch_all(pool)
    .await
    .map_err(super::super::thread_history_error)?;
    rows.into_iter().map(stored_thread_item).collect()
}

pub(super) fn stored_updated_at_ordinal(updated_at_ordinal: i64) -> ThreadStoreResult<u64> {
    u64::try_from(updated_at_ordinal).map_err(|_| ThreadStoreError::Internal {
        message: format!("invalid stored item updated-at ordinal: {updated_at_ordinal}"),
    })
}

pub(in crate::local::thread_history) fn invalid_cursor(cursor: &str) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: format!("invalid cursor: {cursor}"),
    }
}
