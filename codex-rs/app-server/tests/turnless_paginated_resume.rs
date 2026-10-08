use anyhow::Result;
use app_test_support::TestAppServer;
use app_test_support::create_fake_paginated_rollout;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use sqlx::sqlite::SqliteConnectOptions;
use tempfile::TempDir;
use tokio::time::timeout;

#[tokio::test]
async fn resume_before_first_turn_has_empty_cursors() -> Result<()> {
    let codex_home = TempDir::new()?;
    let thread_id = create_fake_paginated_rollout(
        codex_home.path(),
        "2026-01-02T00-00-00",
        "2026-01-02T00:00:00Z",
        "queued before first turn",
        /*model_provider*/ None,
        /*git_info*/ None,
    )?;
    let mut server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;

    let resume_id = server
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.clone(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let resumed: ThreadResumeResponse = timeout(
        std::time::Duration::from_secs(25),
        server.read_response(resume_id),
    )
    .await??;
    assert_eq!(resumed.thread.id, thread_id);
    assert!(resumed.thread.turns.is_empty());
    assert_eq!(resumed.turns_backwards_cursor, None);
    assert_eq!(resumed.items_backwards_cursor, None);
    Ok(())
}

#[tokio::test]
async fn resume_loaded_turnless_paginated_thread_without_metadata_row() -> Result<()> {
    let codex_home = TempDir::new()?;
    let thread_id = create_fake_paginated_rollout(
        codex_home.path(),
        "2026-01-02T00-00-00",
        "2026-01-02T00:00:00Z",
        "queued before first turn",
        /*model_provider*/ None,
        /*git_info*/ None,
    )?;
    let mut server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;

    let first_resume_id = server
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.clone(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let _: ThreadResumeResponse = timeout(
        std::time::Duration::from_secs(25),
        server.read_response(first_resume_id),
    )
    .await??;

    let db_path = codex_home.path().join("state_5.sqlite");
    let pool = sqlx::SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(false),
    )
    .await?;
    let deleted = sqlx::query("DELETE FROM threads WHERE id = ?")
        .bind(&thread_id)
        .execute(&pool)
        .await?;
    assert_eq!(deleted.rows_affected(), 1);
    pool.close().await;

    let resume_id = server
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.clone(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let resumed: ThreadResumeResponse = timeout(
        std::time::Duration::from_secs(25),
        server.read_response(resume_id),
    )
    .await??;
    assert_eq!(resumed.thread.id, thread_id);
    assert_eq!(resumed.turns_backwards_cursor, None);
    assert_eq!(resumed.items_backwards_cursor, None);
    Ok(())
}
