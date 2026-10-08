use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use serde::Deserialize;
use serde::Serialize;
use sqlx::Row;

use super::super::LocalThreadStore;
use super::super::rollout_lineage::RolloutLineage;
use super::segment_paging::page_item_rows;
use super::segment_paging::page_turn_rows;
use super::segment_paging::validate_page_size;
use super::sqlite_integer;
use super::turn_lookup::find_source_turn;
use crate::ItemPage;
use crate::ListItemsParams;
use crate::ListTurnsParams;
use crate::StoredThreadItem;
use crate::StoredTurn;
use crate::StoredTurnError;
use crate::StoredTurnItemsView;
use crate::StoredTurnStatus;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use crate::TurnPage;

#[cfg(test)]
#[path = "read_tests.rs"]
mod tests;

#[derive(Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub(super) enum CursorScope {
    Turns,
    ItemsByCreatedAtOrdinal,
    ItemsByUpdatedAtOrdinal,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct HistoryCursor {
    pub requested_thread_id: ThreadId,
    pub rollout_ordinal: u64,
    pub include_anchor: bool,
    pub scope: CursorScope,
}

#[derive(Clone, Copy)]
pub(super) struct RolloutHistoryPosition {
    pub rollout_ordinal: i64,
}

pub(super) struct StoredTurnRow {
    pub position: RolloutHistoryPosition,
    pub turn_id: String,
    pub root_turn_id: Option<String>,
    pub status: StoredTurnStatus,
    pub error: Option<StoredTurnError>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub duration_ms: Option<i64>,
    pub first_user_item_id: Option<String>,
    pub final_agent_item_id: Option<String>,
    pub summary_items: Vec<StoredThreadItem>,
}

#[derive(sqlx_macros::FromRow)]
pub(super) struct StoredSummaryColumns {
    summary_first_user_turn_id: Option<String>,
    summary_first_user_item_id: Option<String>,
    summary_first_user_rollout_ordinal: Option<i64>,
    summary_first_user_updated_at_ordinal: Option<i64>,
    summary_first_user_created_at_ms: Option<i64>,
    summary_first_user_started_at_ms: Option<i64>,
    summary_first_user_completed_at_ms: Option<i64>,
    summary_first_user_item_json: Option<String>,
    summary_final_agent_turn_id: Option<String>,
    summary_final_agent_item_id: Option<String>,
    summary_final_agent_rollout_ordinal: Option<i64>,
    summary_final_agent_updated_at_ordinal: Option<i64>,
    summary_final_agent_created_at_ms: Option<i64>,
    summary_final_agent_started_at_ms: Option<i64>,
    summary_final_agent_completed_at_ms: Option<i64>,
    summary_final_agent_item_json: Option<String>,
}

struct StoredSummaryItemColumns {
    turn_id: Option<String>,
    item_id: Option<String>,
    rollout_ordinal: Option<i64>,
    updated_at_ordinal: Option<i64>,
    created_at_ms: Option<i64>,
    started_at_ms: Option<i64>,
    completed_at_ms: Option<i64>,
    item_json: Option<String>,
}

pub(super) struct StoredThreadItemRow {
    pub position: RolloutHistoryPosition,
    pub item: StoredThreadItem,
}

#[path = "read_actions.rs"]
mod actions;
pub(super) use actions::invalid_cursor;
pub(in crate::local) use actions::list_items;
pub(in crate::local) use actions::list_turns;
use actions::stored_updated_at_ordinal;
pub(super) use actions::validate_thread_for_paginated_reads;
pub(super) fn parse_cursor(
    cursor: Option<&str>,
    requested_thread_id: ThreadId,
    scope: CursorScope,
) -> ThreadStoreResult<Option<HistoryCursor>> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let cursor_value: HistoryCursor =
        serde_json::from_str(cursor).map_err(|_| invalid_cursor(cursor))?;
    if cursor_value.requested_thread_id != requested_thread_id || cursor_value.scope != scope {
        return Err(invalid_cursor(cursor));
    }
    Ok(Some(cursor_value))
}

pub(super) fn serialize_cursor(
    requested_thread_id: ThreadId,
    scope: CursorScope,
    rollout_ordinal: i64,
    include_anchor: bool,
) -> ThreadStoreResult<String> {
    let rollout_ordinal =
        u64::try_from(rollout_ordinal).map_err(|_| invalid_cursor("negative rollout ordinal"))?;
    serde_json::to_string(&HistoryCursor {
        requested_thread_id,
        rollout_ordinal,
        include_anchor,
        scope,
    })
    .map_err(super::thread_history_error)
}

pub(super) fn stored_turn_row(row: sqlx::sqlite::SqliteRow) -> ThreadStoreResult<StoredTurnRow> {
    let status = match row.try_get::<String, _>("status")?.as_str() {
        "completed" => StoredTurnStatus::Completed,
        "interrupted" => StoredTurnStatus::Interrupted,
        "failed" => StoredTurnStatus::Failed,
        "inProgress" => StoredTurnStatus::InProgress,
        status => {
            return Err(ThreadStoreError::Internal {
                message: format!("unknown stored turn status: {status}"),
            });
        }
    };
    let error_json = row.try_get::<Option<String>, _>("error_json")?;
    let error = error_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(super::thread_history_error)?;
    Ok(StoredTurnRow {
        position: RolloutHistoryPosition {
            rollout_ordinal: row.try_get("rollout_ordinal")?,
        },
        turn_id: row.try_get("turn_id")?,
        root_turn_id: row.try_get("root_turn_id")?,
        status,
        error,
        started_at: row.try_get("started_at")?,
        completed_at: row.try_get("completed_at")?,
        duration_ms: row.try_get("duration_ms")?,
        first_user_item_id: row.try_get("first_user_item_id")?,
        final_agent_item_id: row.try_get("final_agent_item_id")?,
        summary_items: Vec::new(),
    })
}

impl StoredSummaryColumns {
    pub(super) fn into_stored_items(self) -> ThreadStoreResult<Vec<StoredThreadItem>> {
        let mut summary_items = [
            StoredSummaryItemColumns {
                turn_id: self.summary_first_user_turn_id,
                item_id: self.summary_first_user_item_id,
                rollout_ordinal: self.summary_first_user_rollout_ordinal,
                updated_at_ordinal: self.summary_first_user_updated_at_ordinal,
                created_at_ms: self.summary_first_user_created_at_ms,
                started_at_ms: self.summary_first_user_started_at_ms,
                completed_at_ms: self.summary_first_user_completed_at_ms,
                item_json: self.summary_first_user_item_json,
            }
            .into_stored_item()?,
            StoredSummaryItemColumns {
                turn_id: self.summary_final_agent_turn_id,
                item_id: self.summary_final_agent_item_id,
                rollout_ordinal: self.summary_final_agent_rollout_ordinal,
                updated_at_ordinal: self.summary_final_agent_updated_at_ordinal,
                created_at_ms: self.summary_final_agent_created_at_ms,
                started_at_ms: self.summary_final_agent_started_at_ms,
                completed_at_ms: self.summary_final_agent_completed_at_ms,
                item_json: self.summary_final_agent_item_json,
            }
            .into_stored_item()?,
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        summary_items.sort_by_key(|(rollout_ordinal, _)| *rollout_ordinal);
        Ok(summary_items.into_iter().map(|(_, item)| item).collect())
    }
}

impl StoredSummaryItemColumns {
    fn into_stored_item(self) -> ThreadStoreResult<Option<(i64, StoredThreadItem)>> {
        let Some(item_id) = self.item_id else {
            return Ok(None);
        };
        let (
            Some(turn_id),
            Some(rollout_ordinal),
            Some(updated_at_ordinal),
            Some(created_at_ms),
            Some(item_json),
        ) = (
            self.turn_id,
            self.rollout_ordinal,
            self.updated_at_ordinal,
            self.created_at_ms,
            self.item_json,
        )
        else {
            return Err(ThreadStoreError::Internal {
                message: "stored summary item is missing joined columns".to_string(),
            });
        };
        Ok(Some((
            rollout_ordinal,
            StoredThreadItem {
                turn_id,
                item_id,
                updated_at_ordinal: stored_updated_at_ordinal(updated_at_ordinal)?,
                created_at_ms,
                started_at_ms: self.started_at_ms,
                completed_at_ms: self.completed_at_ms,
                item_json: item_json.into_bytes(),
            },
        )))
    }
}

pub(super) fn stored_thread_item_row(
    row: sqlx::sqlite::SqliteRow,
) -> ThreadStoreResult<StoredThreadItemRow> {
    let rollout_ordinal = row.try_get::<i64, _>("rollout_ordinal")?;
    if rollout_ordinal < 0 {
        return Err(ThreadStoreError::Internal {
            message: format!("invalid stored item rollout ordinal: {rollout_ordinal}"),
        });
    }
    Ok(StoredThreadItemRow {
        position: RolloutHistoryPosition { rollout_ordinal },
        item: stored_thread_item(row)?,
    })
}

fn stored_thread_item(row: sqlx::sqlite::SqliteRow) -> ThreadStoreResult<StoredThreadItem> {
    let updated_at_ordinal = stored_updated_at_ordinal(row.try_get("updated_at_ordinal")?)?;
    Ok(StoredThreadItem {
        turn_id: row.try_get("turn_id")?,
        item_id: row.try_get("item_id")?,
        updated_at_ordinal,
        created_at_ms: row.try_get("created_at_ms")?,
        started_at_ms: row.try_get("started_at_ms")?,
        completed_at_ms: row.try_get("completed_at_ms")?,
        item_json: row.try_get::<String, _>("item_json")?.into_bytes(),
    })
}
