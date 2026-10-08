//! Bounded source-to-destination import and independent persisted read-back.
use crate::{
    EffectFactConsumerProgress, ImportPhase, ImportProgress, OperationsCatalog, OperationsError,
    catalog::{commit, commit_read, load_progress, signed},
    cursor, events, members,
    wire::MemberWire,
};
use forge_domain::WorkspaceId;
use forge_persistence::{
    AgentOperationsExportPage as Page, AgentOperationsExportRecord as Record, WorkspaceStore,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

impl OperationsCatalog {
    /// Import one bounded source page. Source read finishes before destination writes begin.
    ///
    /// # Errors
    /// Rejects unavailable source pages, non-importing state, conflicts and storage failures.
    pub fn import_next_page(
        &mut self,
        source: &WorkspaceStore,
        migration: Uuid,
    ) -> Result<ImportProgress, OperationsError> {
        let p = self
            .progress(migration)?
            .ok_or(OperationsError::Conflict("import not prepared"))?;
        let next = p
            .next
            .ok_or(OperationsError::Conflict("import already copied"))?;
        let page = source.export_agent_operations(migration, next)?;
        self.import_page(&page)
    }
    /// Reconcile one bounded page of Forge-owned effect facts into the local
    /// Orchestration projection. Source observation completes before the local
    /// projection/cursor transaction begins; no acknowledgement is written back
    /// to Forge.
    ///
    /// # Errors
    /// Rejects unverified imports, missing/foreign delegation seals, identity
    /// mismatches, sequence gaps, conflicting replays and storage failures.
    pub fn reconcile_effect_facts(
        &mut self,
        source: &WorkspaceStore,
        migration: Uuid,
    ) -> Result<EffectFactConsumerProgress, OperationsError> {
        let import = self
            .progress(migration)?
            .ok_or(OperationsError::Conflict("effect fact import missing"))?;
        if import.phase() != ImportPhase::Verified {
            return Err(OperationsError::Conflict("effect fact import not verified"));
        }
        if import.destination_id() != self.destination {
            return Err(OperationsError::Conflict("effect fact catalog destination"));
        }
        let delegation = source
            .agent_operations_delegation()?
            .ok_or(OperationsError::Conflict(
                "effect fact source delegation missing",
            ))?;
        if delegation.migration_id() != migration
            || delegation.workspace_id() != import.workspace_id()
            || delegation.destination_id() != self.destination
        {
            return Err(OperationsError::Conflict(
                "effect fact source delegation identity",
            ));
        }
        let after = self
            .effect_fact_progress(migration)?
            .map_or(0, EffectFactConsumerProgress::last_sequence);
        let page = source.effect_fact_outbox(after, 64)?;
        self.apply_effect_fact_page(migration, &page)
    }

    /// Apply an exact typed source page and atomically persist its checkpoint.
    ///
    /// # Errors
    /// Rejects wrong scope, out-of-order or changed replays, failed readback and uncertain commits.
    pub fn import_page(&mut self, page: &Page) -> Result<ImportProgress, OperationsError> {
        self.import_page_inner(page, None)
    }

    /// Apply one page only when the full durable progress snapshot is still
    /// the one observed by the caller. The fence is checked in the same
    /// immediate transaction that owns the page mutation.
    ///
    /// # Errors
    /// Rejects stale progress fences, wrong scope, out-of-order or changed
    /// replays, failed readback and uncertain commits.
    pub fn import_page_expected(
        &mut self,
        page: &Page,
        expected: &ImportProgress,
    ) -> Result<ImportProgress, OperationsError> {
        self.import_page_inner(page, Some(expected))
    }

    fn import_page_inner(
        &mut self,
        page: &Page,
        expected: Option<&ImportProgress>,
    ) -> Result<ImportProgress, OperationsError> {
        if page.destination_id() != self.destination {
            return Err(OperationsError::Conflict("catalog destination"));
        }
        if page.records().len() > 64 {
            return Err(OperationsError::Bounds("source page"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let p = load_progress(&tx, page.migration_id())?
            .ok_or(OperationsError::Conflict("import not prepared"))?;
        if expected.is_some_and(|value| value != &p) {
            return Err(OperationsError::Conflict("import progress changed"));
        }
        check_header(&p, page)?;
        if p.phase == ImportPhase::Aborted {
            return Err(OperationsError::Conflict("import aborted"));
        }
        let requested = cursor::encode(page.requested_cursor())?;
        let next = page.next_cursor().map(cursor::encode).transpose()?;
        let digest = page_digest(page, |r| source_record_digest(page.workspace_id(), r))?;
        let prior:Option<(Vec<u8>,Option<Vec<u8>>,i64)>=tx.query_row("SELECT substr(digest,1,33),substr(next_cursor,1,513),record_count FROM ops_import_pages WHERE migration_id=?1 AND cursor=?2",params![page.migration_id().as_bytes(),requested],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        if let Some((old_hash, old_next, count)) = prior {
            if old_hash != digest
                || old_next != next
                || usize::try_from(count).ok() != Some(page.records().len())
            {
                return Err(OperationsError::Conflict("page replay payload"));
            }
            return Ok(p);
        }
        if p.phase != ImportPhase::Importing || p.next != Some(page.requested_cursor()) {
            return Err(OperationsError::Conflict("page order"));
        }
        for record in page.records() {
            put_record(&tx, page.workspace_id(), record)?;
        }
        let observed = page_digest(page, |r| stored_record_digest(&tx, page.workspace_id(), r))?;
        if observed != digest {
            return Err(OperationsError::Corrupt("page write readback"));
        }
        tx.execute("INSERT INTO ops_import_pages(migration_id,ordinal,cursor,next_cursor,table_kind,record_count,digest) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![p.migration.as_bytes(),signed(p.pages)?,requested,next,i64::try_from(cursor::kind(page.requested_cursor())).map_err(|_|OperationsError::Bounds("table kind"))?,i64::try_from(page.records().len()).map_err(|_|OperationsError::Bounds("record count"))?,digest.as_slice()])?;
        let count = p
            .records
            .checked_add(
                u64::try_from(page.records().len())
                    .map_err(|_| OperationsError::Bounds("record count"))?,
            )
            .ok_or(OperationsError::Bounds("record count"))?;
        let pages = p
            .pages
            .checked_add(1)
            .ok_or(OperationsError::Bounds("page count"))?;
        let chained = chain(p.digest, digest);
        let changed=tx.execute("UPDATE ops_imports SET next_cursor=?1,page_count=?2,record_count=?3,import_digest=?4,phase=?5 WHERE migration_id=?6 AND phase=0 AND page_count=?7",params![next,signed(pages)?,signed(count)?,chained.as_slice(),i32::from(page.next_cursor().is_none()),p.migration.as_bytes(),signed(p.pages)?])?;
        if changed != 1 {
            return Err(OperationsError::Conflict("import progress CAS"));
        }
        let result =
            load_progress(&tx, p.migration)?.ok_or(OperationsError::Corrupt("import progress"))?;
        commit(tx, p.migration)?;
        Ok(result)
    }
    /// Verify one committed page against a fresh frozen-source read. Final verification
    /// also checks complete per-table counts, detecting extra destination records.
    ///
    /// # Errors
    /// Rejects source/destination differences, inconsistent checkpoints and storage failures.
    pub fn verify_next_page(
        &mut self,
        source: &WorkspaceStore,
        migration: Uuid,
    ) -> Result<ImportProgress, OperationsError> {
        let p = self
            .progress(migration)?
            .ok_or(OperationsError::Conflict("import not prepared"))?;
        if p.phase == ImportPhase::Verified {
            return Ok(p);
        }
        if p.phase != ImportPhase::Imported || p.verified_pages >= p.pages {
            return Err(OperationsError::Conflict("import not ready to verify"));
        }
        let saved = read_page(&self.connection, migration, p.verified_pages)?;
        let requested = cursor::decode(&saved.cursor)?;
        if p.verified_pages == 0
            && requested != forge_persistence::AgentOperationsExportCursor::default()
        {
            return Err(OperationsError::Corrupt("first verification cursor"));
        }
        if p.verified_pages > 0
            && read_page(&self.connection, migration, p.verified_pages - 1)?
                .next
                .as_deref()
                != Some(saved.cursor.as_slice())
        {
            return Err(OperationsError::Corrupt("verification cursor chain"));
        }
        let page = source.export_agent_operations(migration, requested)?;
        self.verify_page(&page)
    }

    /// Verify one already observed frozen-source page and atomically advance
    /// the persisted verification cursor. The page must have been fetched
    /// outside the catalog owner; this method performs no source I/O.
    ///
    /// # Errors
    /// Rejects source/destination differences, inconsistent checkpoints and storage failures.
    pub fn verify_page(&mut self, page: &Page) -> Result<ImportProgress, OperationsError> {
        self.verify_page_inner(page, None)
    }

    /// Verify one page only when the full durable progress snapshot is still
    /// the one observed by the caller. The fence is checked in the same
    /// immediate transaction that owns the verification CAS.
    ///
    /// # Errors
    /// Rejects stale progress fences, source/destination differences,
    /// inconsistent checkpoints and storage failures.
    pub fn verify_page_expected(
        &mut self,
        page: &Page,
        expected: &ImportProgress,
    ) -> Result<ImportProgress, OperationsError> {
        self.verify_page_inner(page, Some(expected))
    }

    fn verify_page_inner(
        &mut self,
        page: &Page,
        expected: Option<&ImportProgress>,
    ) -> Result<ImportProgress, OperationsError> {
        let p = self
            .progress(page.migration_id())?
            .ok_or(OperationsError::Conflict("import not prepared"))?;
        if expected.is_some_and(|value| value != &p) {
            return Err(OperationsError::Conflict("import progress changed"));
        }
        if p.phase == ImportPhase::Verified {
            check_header(&p, page)?;
            return Ok(p);
        }
        if p.phase != ImportPhase::Imported || p.verified_pages >= p.pages {
            return Err(OperationsError::Conflict("import not ready to verify"));
        }
        let saved = read_page(&self.connection, page.migration_id(), p.verified_pages)?;
        let requested = cursor::decode(&saved.cursor)?;
        if p.verified_pages == 0
            && requested != forge_persistence::AgentOperationsExportCursor::default()
        {
            return Err(OperationsError::Corrupt("first verification cursor"));
        }
        if p.verified_pages > 0
            && read_page(&self.connection, page.migration_id(), p.verified_pages - 1)?
                .next
                .as_deref()
                != Some(saved.cursor.as_slice())
        {
            return Err(OperationsError::Corrupt("verification cursor chain"));
        }
        check_header(&p, page)?;
        let page_digest_value =
            page_digest(page, |r| source_record_digest(page.workspace_id(), r))?;
        if page_digest_value != saved.digest
            || page.next_cursor().map(cursor::encode).transpose()? != saved.next
            || usize::try_from(saved.count).ok() != Some(page.records().len())
        {
            return Err(OperationsError::Conflict(
                "source differs from imported page",
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let locked = load_progress(&tx, page.migration_id())?
            .ok_or(OperationsError::Corrupt("import missing"))?;
        if expected.is_some_and(|value| value != &locked) {
            return Err(OperationsError::Conflict("import progress changed"));
        }
        if locked != p {
            return Err(OperationsError::Conflict("verification progress changed"));
        }
        let observed = page_digest(page, |r| stored_record_digest(&tx, page.workspace_id(), r))?;
        if observed != page_digest_value {
            return Err(OperationsError::Corrupt(
                "destination differs from frozen source",
            ));
        }
        let verified = p
            .verified_pages
            .checked_add(1)
            .ok_or(OperationsError::Bounds("verification count"))?;
        let verify_digest = chain(p.verify_digest, observed);
        let complete = verified == p.pages;
        if complete {
            if page.next_cursor().is_some() {
                return Err(OperationsError::Corrupt("source export incomplete"));
            }
            if verify_digest != p.digest {
                return Err(OperationsError::Corrupt("import chain"));
            }
            verify_counts(&tx, &p)?;
        }
        let changed=tx.execute("UPDATE ops_imports SET verified_pages=?1,verify_digest=?2,phase=?3 WHERE migration_id=?4 AND phase=1 AND verified_pages=?5",params![signed(verified)?,verify_digest.as_slice(),if complete{2}else{1},page.migration_id().as_bytes(),signed(p.verified_pages)?])?;
        if changed != 1 {
            return Err(OperationsError::Conflict("verification CAS"));
        }
        let result = load_progress(&tx, page.migration_id())?
            .ok_or(OperationsError::Corrupt("verification readback"))?;
        commit(tx, page.migration_id())?;
        Ok(result)
    }

    /// Read one consistent verification target without entering a source
    /// authority. The cursor is absent only after the import is verified.
    ///
    /// # Errors
    /// Rejects missing, incomplete, corrupt, or out-of-order verification
    /// progress and reports catalog read failures.
    pub fn verification_target(
        &self,
        migration: Uuid,
    ) -> Result<ImportVerificationTarget, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let progress = load_progress(&tx, migration)?
            .ok_or(OperationsError::Conflict("import not prepared"))?;
        let requested_cursor = if progress.phase == ImportPhase::Verified {
            None
        } else {
            if progress.phase != ImportPhase::Imported || progress.verified_pages >= progress.pages
            {
                return Err(OperationsError::Conflict("import not ready to verify"));
            }
            let saved = read_page(&tx, migration, progress.verified_pages)?;
            let requested = cursor::decode(&saved.cursor)?;
            if progress.verified_pages == 0
                && requested != forge_persistence::AgentOperationsExportCursor::default()
            {
                return Err(OperationsError::Corrupt("first verification cursor"));
            }
            if progress.verified_pages > 0
                && read_page(&tx, migration, progress.verified_pages - 1)?
                    .next
                    .as_deref()
                    != Some(saved.cursor.as_slice())
            {
                return Err(OperationsError::Corrupt("verification cursor chain"));
            }
            Some(requested)
        };
        commit_read(tx, self.destination)?;
        Ok(ImportVerificationTarget {
            progress,
            requested_cursor,
        })
    }
    /// Discard a pre-membership import's owned rows. The audit/checkpoint ledger stays.
    ///
    /// # Errors
    /// Rejects unknown imports or committed membership handoffs and reports storage or uncertain-commit failures.
    pub fn abort_import(&mut self, migration: Uuid) -> Result<ImportProgress, OperationsError> {
        self.abort_import_inner(migration, None)
    }

    /// Abort one import only when its full durable progress snapshot is still
    /// the one observed by the caller. The fence is checked in the same
    /// immediate transaction that owns the abort mutation.
    ///
    /// # Errors
    /// Rejects stale progress fences, unknown imports, committed membership
    /// handoffs, and uncertain durable commits.
    pub fn abort_import_expected(
        &mut self,
        migration: Uuid,
        expected: &ImportProgress,
    ) -> Result<ImportProgress, OperationsError> {
        self.abort_import_inner(migration, Some(expected))
    }

    fn abort_import_inner(
        &mut self,
        migration: Uuid,
        expected: Option<&ImportProgress>,
    ) -> Result<ImportProgress, OperationsError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let p =
            load_progress(&tx, migration)?.ok_or(OperationsError::Conflict("import missing"))?;
        if expected.is_some_and(|value| value != &p) {
            return Err(OperationsError::Conflict("import progress changed"));
        }
        if p.phase == ImportPhase::Aborted {
            return Ok(p);
        }
        // Membership commit is the durable handoff intent. Source activation
        // may already be admitted or committed with its response still pending.
        // Serialize abort against that commit before deleting imported state.
        let membership_committed: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_logical_bindings WHERE migration_id=?1 UNION ALL SELECT 1 FROM ops_legacy_adoptions WHERE migration_id=?1)",
            params![migration.as_bytes()],
            |row| row.get(0),
        )?;
        if membership_committed {
            return Err(OperationsError::Conflict("membership handoff committed"));
        }
        for table in [
            "ops_inbox",
            "ops_presence",
            "ops_events",
            "ops_activity_coordinators",
            "ops_import_effect_references",
            "ops_members",
            "ops_scopes",
        ] {
            tx.execute(
                &format!("DELETE FROM {table} WHERE workspace_id=?1"),
                params![p.workspace.into_uuid().as_bytes()],
            )?;
        }
        tx.execute(
            "UPDATE ops_imports SET phase=3 WHERE migration_id=?1",
            params![migration.as_bytes()],
        )?;
        let result =
            load_progress(&tx, migration)?.ok_or(OperationsError::Corrupt("abort readback"))?;
        commit(tx, migration)?;
        Ok(result)
    }
}

/// One source-free verification readback and the exact next source cursor.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportVerificationTarget {
    progress: ImportProgress,
    requested_cursor: Option<forge_persistence::AgentOperationsExportCursor>,
}

impl ImportVerificationTarget {
    #[must_use]
    pub const fn progress(&self) -> &ImportProgress {
        &self.progress
    }

    #[must_use]
    pub const fn requested_cursor(&self) -> Option<forge_persistence::AgentOperationsExportCursor> {
        self.requested_cursor
    }
}

fn check_header(p: &ImportProgress, page: &Page) -> Result<(), OperationsError> {
    if p.migration != page.migration_id()
        || p.workspace != page.workspace_id()
        || p.destination != page.destination_id()
        || p.source_revision != page.freeze_revision()
    {
        return Err(OperationsError::Conflict("source page header"));
    }
    Ok(())
}
fn json_hash<T: serde::Serialize>(value: &T) -> Result<[u8; 32], OperationsError> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > 16 * 1024 {
        return Err(OperationsError::Bounds("canonical record"));
    }
    Ok(Sha256::digest(bytes).into())
}
fn source_record_digest(
    workspace: WorkspaceId,
    r: Record<'_>,
) -> Result<[u8; 32], OperationsError> {
    match r {
        Record::Participant(v) => {
            if v.workspace_id() != workspace {
                return Err(OperationsError::Conflict("member scope"));
            }
            json_hash(&MemberWire::from_agent(v))
        }
        Record::Presence(v) => presence_digest(&events::PresenceRecord::new(
            v.agent_id(),
            v.workspace_id(),
            v.activity_id(),
            v.session_id(),
            v.last_seen(),
            v.revision(),
        )),
        Record::Event(v) => {
            if v.workspace_id() != workspace {
                return Err(OperationsError::Conflict("event scope"));
            }
            events::event_digest(v)
        }
        Record::Inbox(v) => receipt_digest(&receipt(workspace, v)),
        Record::Coordinator(v) => coordinator_digest(*v),
    }
}
fn receipt(
    workspace: WorkspaceId,
    v: &forge_persistence::AgentOperationsInboxReceipt,
) -> events::InboxReceiptRecord {
    events::InboxReceiptRecord::new(
        v.key().recipient_agent_id(),
        workspace,
        v.key().recipient_session_id(),
        v.key().event_id(),
        v.revision(),
        v.acknowledged_at(),
    )
}
fn presence_digest(v: &events::PresenceRecord) -> Result<[u8; 32], OperationsError> {
    json_hash(&(
        v.workspace_id().into_uuid().into_bytes(),
        v.agent_id().into_uuid().into_bytes(),
        v.activity_id().into_uuid().into_bytes(),
        v.session_id().into_uuid().into_bytes(),
        v.last_authenticated_at().get(),
        v.revision().get(),
    ))
}
fn receipt_digest(v: &events::InboxReceiptRecord) -> Result<[u8; 32], OperationsError> {
    json_hash(&(
        v.workspace_id().into_uuid().into_bytes(),
        v.agent_id().into_uuid().into_bytes(),
        v.session_id().into_uuid().into_bytes(),
        v.event_id().into_uuid().into_bytes(),
        v.receipt_revision().get(),
        v.acknowledged_at().map(forge_domain::TimestampMillis::get),
    ))
}
fn coordinator_digest(
    v: forge_domain::ActivityCoordinatorBinding,
) -> Result<[u8; 32], OperationsError> {
    json_hash(&(
        v.workspace_id().into_uuid().into_bytes(),
        v.activity_id().into_uuid().into_bytes(),
        v.agent_id().into_uuid().into_bytes(),
        v.revision().get(),
        v.updated_at().get(),
    ))
}
fn put_record(
    tx: &rusqlite::Transaction<'_>,
    workspace: WorkspaceId,
    r: Record<'_>,
) -> Result<(), OperationsError> {
    match r {
        Record::Participant(v) => {
            if v.workspace_id() != workspace {
                return Err(OperationsError::Conflict("participant workspace"));
            }
            members::put(tx, v)
        }
        Record::Presence(v) => {
            if v.workspace_id() != workspace {
                return Err(OperationsError::Conflict("presence workspace"));
            }
            events::put_presence(
                tx,
                &events::PresenceRecord::new(
                    v.agent_id(),
                    v.workspace_id(),
                    v.activity_id(),
                    v.session_id(),
                    v.last_seen(),
                    v.revision(),
                ),
            )
        }
        Record::Event(v) => {
            if v.workspace_id() != workspace {
                return Err(OperationsError::Conflict("event workspace"));
            }
            members::scope(
                tx,
                workspace.into_uuid().into_bytes(),
                v.activity_id().into_uuid().into_bytes(),
            )?;
            events::put_event(tx, v)
        }
        Record::Inbox(v) => events::put_receipt(tx, &receipt(workspace, v)),
        Record::Coordinator(v) => {
            if v.workspace_id() != workspace {
                return Err(OperationsError::Conflict("coordinator workspace"));
            }
            members::put_coordinator(tx, *v)
        }
    }
}
fn stored_record_digest(
    c: &Connection,
    workspace: WorkspaceId,
    r: Record<'_>,
) -> Result<[u8; 32], OperationsError> {
    match r {
        Record::Participant(v) => json_hash(&MemberWire::from_agent(
            &members::get(c, workspace, v.id())?
                .ok_or(OperationsError::Corrupt("missing member"))?,
        )),
        Record::Presence(v) => presence_digest(
            &events::get_presence(c, workspace, v.agent_id())?
                .ok_or(OperationsError::Corrupt("missing presence"))?,
        ),
        Record::Event(v) => events::event_digest(
            &events::get_event(c, workspace, v.id())?
                .ok_or(OperationsError::Corrupt("missing event"))?,
        ),
        Record::Inbox(v) => receipt_digest(
            &events::get_receipt(
                c,
                workspace,
                v.key().recipient_agent_id(),
                v.key().recipient_session_id(),
                v.key().event_id(),
            )?
            .ok_or(OperationsError::Corrupt("missing receipt"))?,
        ),
        Record::Coordinator(v) => coordinator_digest(
            members::get_coordinator(c, workspace, v.activity_id())?
                .ok_or(OperationsError::Corrupt("missing coordinator"))?,
        ),
    }
}
fn frame(d: &mut Sha256, bytes: &[u8]) {
    d.update((bytes.len() as u64).to_be_bytes());
    d.update(bytes);
}
fn page_digest<'a>(
    page: &'a Page,
    mut record: impl FnMut(Record<'a>) -> Result<[u8; 32], OperationsError>,
) -> Result<[u8; 32], OperationsError> {
    let mut d = Sha256::new();
    frame(&mut d, b"orchestration-operations-page-v1");
    frame(&mut d, page.migration_id().as_bytes());
    frame(&mut d, page.workspace_id().into_uuid().as_bytes());
    frame(&mut d, page.destination_id().as_bytes());
    d.update(page.freeze_revision().to_be_bytes());
    frame(&mut d, &cursor::encode(page.requested_cursor())?);
    match page.next_cursor() {
        None => d.update([0]),
        Some(next) => {
            d.update([1]);
            frame(&mut d, &cursor::encode(next)?);
        }
    }
    d.update((page.records().len() as u64).to_be_bytes());
    for r in page.records() {
        d.update(record(r)?);
    }
    Ok(d.finalize().into())
}
fn chain(previous: [u8; 32], page: [u8; 32]) -> [u8; 32] {
    let mut d = Sha256::new();
    d.update(b"orchestration-operations-chain-v1");
    d.update(previous);
    d.update(page);
    d.finalize().into()
}
struct SavedPage {
    cursor: Vec<u8>,
    next: Option<Vec<u8>>,
    count: i64,
    digest: [u8; 32],
}
fn read_page(c: &Connection, migration: Uuid, ordinal: u64) -> Result<SavedPage, OperationsError> {
    let (cursor,next,count,digest,kind)=c.query_row("SELECT substr(cursor,1,513),substr(next_cursor,1,513),record_count,substr(digest,1,33),table_kind FROM ops_import_pages WHERE migration_id=?1 AND ordinal=?2",params![migration.as_bytes(),signed(ordinal)?],|r|Ok((r.get::<_,Vec<u8>>(0)?,r.get::<_,Option<Vec<u8>>>(1)?,r.get::<_,i64>(2)?,r.get::<_,Vec<u8>>(3)?,r.get::<_,i64>(4)?)))?;
    if usize::try_from(kind).ok() != Some(cursor::kind(cursor::decode(&cursor)?)) {
        return Err(OperationsError::Corrupt("page table kind"));
    }
    Ok(SavedPage {
        cursor,
        next,
        count,
        digest: digest
            .try_into()
            .map_err(|_| OperationsError::Corrupt("page digest"))?,
    })
}

fn verify_counts(c: &Connection, p: &ImportProgress) -> Result<(), OperationsError> {
    let (pages, records): (i64, i64) = c.query_row(
        "SELECT COUNT(*),COALESCE(SUM(record_count),0) FROM ops_import_pages WHERE migration_id=?1",
        params![p.migration.as_bytes()],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if u64::try_from(pages).ok() != Some(p.pages) || u64::try_from(records).ok() != Some(p.records)
    {
        return Err(OperationsError::Corrupt("page ledger totals"));
    }
    for (kind, table) in [
        "ops_members",
        "ops_presence",
        "ops_events",
        "ops_inbox",
        "ops_activity_coordinators",
    ]
    .iter()
    .enumerate()
    {
        let expected:i64=c.query_row("SELECT COALESCE(SUM(record_count),0) FROM ops_import_pages WHERE migration_id=?1 AND table_kind=?2",params![p.migration.as_bytes(),i64::try_from(kind).map_err(|_|OperationsError::Bounds("table kind"))?],|r|r.get(0))?;
        let actual: i64 = c.query_row(
            &format!("SELECT COUNT(*) FROM {table} WHERE workspace_id=?1"),
            params![p.workspace.into_uuid().as_bytes()],
            |r| r.get(0),
        )?;
        if expected != actual {
            return Err(OperationsError::Corrupt("destination table count"));
        }
    }
    Ok(())
}
