(() => {
  const CHAT_TRANSPORT_VERSION = 1;
  const CHAT_JOURNAL_PREFIX = "heterarchy:chat-journal:";
  const CHAT_WATCH_PREFIX = "heterarchy:chat-watch:";
  const CHAT_JOURNAL_LIMIT = 128;
  const CHAT_WATCH_LIMIT = 32;
  const CHAT_BODY_MAX_BYTES = 16 * 1024;
  const CHAT_ID_MAX_BYTES = 512;
  const CHAT_WAIT_MS = 90000;
  const CHAT_POLL_MS = 500;
  const CHAT_TERMINAL_WATCH_STATES = new Set([
    "recorded",
    "failed",
    "unknown",
  ]);

  function isRecord(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
  }

  function byteLength(value) {
    return new TextEncoder().encode(value).length;
  }

  function boundedId(value) {
    return (
      typeof value === "string" &&
      value.length > 0 &&
      byteLength(value) <= CHAT_ID_MAX_BYTES
    );
  }

  function boundedBody(value) {
    return (
      typeof value === "string" &&
      value.length > 0 &&
      byteLength(value) <= CHAT_BODY_MAX_BYTES
    );
  }

  function validExpiry(value) {
    return (
      value === null ||
      value === undefined ||
      (Number.isSafeInteger(value) && value >= 0)
    );
  }

  function normalizedExpiry(value) {
    return value === null || value === undefined ? null : value;
  }

  function expiredAt(expiresAtMs, now = Date.now()) {
    return Number.isSafeInteger(expiresAtMs) && now >= expiresAtMs;
  }

  function chatJournalKey(messageId) {
    return `${CHAT_JOURNAL_PREFIX}${messageId}`;
  }

  function chatWatchKey(messageId) {
    return `${CHAT_WATCH_PREFIX}${messageId}`;
  }

  function dispatchTargetMatchesRecord(record, dispatch) {
    const dispatchExpiry = normalizedExpiry(dispatch.expires_at_ms);
    return (
      isRecord(record) &&
      record.schemaVersion === CHAT_TRANSPORT_VERSION &&
      record.messageId === dispatch.message_id &&
      record.workspaceId === dispatch.workspace_id &&
      record.profileId === dispatch.profile_id &&
      record.participationId === dispatch.participation_id &&
      record.runtimeSessionId === dispatch.runtime_session_id &&
      record.providerConversationId === dispatch.provider_conversation_id &&
      normalizedExpiry(record.expiresAtMs) === dispatchExpiry &&
      record.body === dispatch.body
    );
  }

  function validChatDispatch(dispatch) {
    return (
      isRecord(dispatch) &&
      boundedId(dispatch.message_id) &&
      Number.isSafeInteger(dispatch.revision) &&
      dispatch.revision > 0 &&
      (dispatch.action === "execute" || dispatch.action === "inspect") &&
      validExpiry(dispatch.expires_at_ms) &&
      boundedId(dispatch.workspace_id) &&
      boundedId(dispatch.profile_id) &&
      boundedId(dispatch.participation_id) &&
      boundedId(dispatch.runtime_session_id) &&
      boundedId(dispatch.provider_conversation_id) &&
      boundedBody(dispatch.body)
    );
  }

  function validChatResponse(response) {
    return isRecord(response) && typeof response.status === "string";
  }

  function deliveryResolvedState(state) {
    return state === "delivered" || state === "failed";
  }

  function journalEvictable(record) {
    return (
      isRecord(record) &&
      (record.state === "failed" ||
        (record.state === "delivered" && record.responseWatchTerminal === true))
    );
  }

  function terminalWatchState(state) {
    return CHAT_TERMINAL_WATCH_STATES.has(state);
  }

  function canFailExpiredWithoutInspection(record) {
    return (
      isRecord(record) &&
      ["dispatched", "ready", "waiting"].includes(record.state) &&
      !boundedId(record.userTurnId)
    );
  }

  function ackMatchesPendingKind(kind, state) {
    if (kind === "delivery") {
      return ["queued", "effect_pending", "delivered", "failed", "unknown"].includes(state);
    }
    if (kind === "failure") return state === "failed" || state === "unknown";
    if (kind === "unknown") return state === "unknown";
    if (kind === "inspect") {
      return ["queued", "effect_pending", "delivered", "failed", "unknown"].includes(state);
    }
    return false;
  }

  function createChatTransport(deps) {
    let pendingNative = null;
    const activeWatches = new Map();
    const inspectedMessages = new Set();
    const activeMessages = new Set();
    const activeInspections = new Set();
    const composerFlights = new Map();
    let storageMutationFlight = Promise.resolve();

    async function loadJournal(messageId) {
      const key = chatJournalKey(messageId);
      const stored = await chrome.storage.local.get(key);
      const value = stored[key];
      return isRecord(value) ? value : null;
    }

    async function loadWatch(messageId) {
      const key = chatWatchKey(messageId);
      const stored = await chrome.storage.local.get(key);
      const value = stored[key];
      return isRecord(value) ? value : null;
    }

    function enqueueStorageMutation(operation) {
      const next = storageMutationFlight.then(operation, operation);
      storageMutationFlight = next.catch(() => {});
      return next;
    }

    async function saveJournal(record) {
      const next = { ...record, updatedAtMs: Date.now() };
      return enqueueStorageMutation(async () => {
        await ensureStorageCapacity(
          CHAT_JOURNAL_PREFIX,
          CHAT_JOURNAL_LIMIT,
          chatJournalKey(next.messageId),
          journalEvictable,
        );
        await chrome.storage.local.set({ [chatJournalKey(next.messageId)]: next });
        return next;
      });
    }

    async function saveWatch(record) {
      const next = { ...record, updatedAtMs: Date.now() };
      return enqueueStorageMutation(async () => {
        await ensureStorageCapacity(
          CHAT_WATCH_PREFIX,
          CHAT_WATCH_LIMIT,
          chatWatchKey(next.messageId),
          (value) => terminalWatchState(value.state),
        );
        await chrome.storage.local.set({ [chatWatchKey(next.messageId)]: next });
        return next;
      });
    }

    async function ensureStorageCapacity(prefix, limit, key, isEvictable) {
      const all = await chrome.storage.local.get(null);
      const entries = Object.entries(all).filter(
        ([key, value]) => key.startsWith(prefix) && isRecord(value),
      );
      if (entries.some(([entryKey]) => entryKey === key)) return;
      const needed = entries.length - limit + 1;
      if (needed <= 0) return;
      const removable = entries
        .filter(([, value]) => isEvictable(value))
        .sort(
          ([, left], [, right]) =>
            Number(left.updatedAtMs ?? 0) - Number(right.updatedAtMs ?? 0),
        );
      if (removable.length < needed) {
        throw new Error(`${prefix} capacity is full of unresolved records`);
      }
      const keys = removable.slice(0, needed).map(([entryKey]) => entryKey);
      await chrome.storage.local.remove(keys);
    }

    async function trimOwnedStorage(prefix, limit, isEvictable) {
      const all = await chrome.storage.local.get(null);
      const entries = Object.entries(all).filter(
        ([key, value]) => key.startsWith(prefix) && isRecord(value),
      );
      const removable = entries
        .filter(([, value]) => isEvictable(value))
        .sort(
          ([, left], [, right]) =>
            Number(left.updatedAtMs ?? 0) - Number(right.updatedAtMs ?? 0),
        );
      const removeCount = Math.max(0, entries.length - limit);
      if (removable.length < removeCount) return;
      await chrome.storage.local.remove(
        removable.slice(0, removeCount).map(([entryKey]) => entryKey),
      );
    }

    async function resumeStoredWatches() {
      const all = await chrome.storage.local.get(null);
      const journals = Object.entries(all).filter(
        ([key, value]) =>
          key.startsWith(CHAT_JOURNAL_PREFIX) &&
          isRecord(value) &&
          boundedId(value.messageId),
      );
      for (const [, journal] of journals) {
        if (
          journal.state !== "delivered" ||
          journal.responseWatchTerminal === true ||
          journal.responseCaptured === true ||
          !boundedId(journal.userTurnId)
        ) {
          continue;
        }
        try {
          let watch = await loadWatch(journal.messageId);
          if (watch === null) {
            watch = await saveWatch({
              schemaVersion: CHAT_TRANSPORT_VERSION,
              messageId: journal.messageId,
              revision: journal.revision,
              workspaceId: journal.workspaceId,
              runtimeSessionId: journal.runtimeSessionId,
              providerConversationId: journal.providerConversationId,
              documentId: journal.documentId,
              userTurnId: journal.userTurnId,
              baselineTurnIds: Array.isArray(journal.baselineTurnIds)
                ? journal.baselineTurnIds
                : [],
              capturedTurnIds: [],
              state: "watching",
              failure: null,
            });
          } else if (watch.state === "incoming_pending") {
            continue;
          }
          startWatch(journal, watch);
        } catch (error) {
          await updateJournal(journal, {
            responseWatchTerminal: true,
            responseWatchState: "failed",
            failure: `response watch unavailable: ${errorMessage(error)}`,
          });
        }
      }
    }

    async function initializeJournal(dispatch, stage) {
      const record = {
        schemaVersion: CHAT_TRANSPORT_VERSION,
        browserSessionId: await deps.currentBrowserSessionId(),
        messageId: dispatch.message_id,
        revision: dispatch.revision,
        workspaceId: dispatch.workspace_id,
        profileId: dispatch.profile_id,
        participationId: dispatch.participation_id,
        runtimeSessionId: dispatch.runtime_session_id,
        providerConversationId: dispatch.provider_conversation_id,
        expiresAtMs: normalizedExpiry(dispatch.expires_at_ms),
        body: dispatch.body,
        action: dispatch.action,
        state: "dispatched",
        stage,
        tabId: null,
        windowId: null,
        documentId: null,
        navigationEpoch: 0,
        baselineCaptured: false,
        baselineTurnIds: [],
        userTurnId: null,
        observedBody: null,
        responseCaptured: false,
        responseWatchTerminal: false,
        responseWatchState: null,
        failure: null,
      };
      return saveJournal(record);
    }

    async function updateJournal(record, patch) {
      return saveJournal({ ...record, ...patch });
    }

    async function updateWatch(record, patch) {
      return saveWatch({ ...record, ...patch });
    }

    function postChat(request, pending) {
      if (!deps.postNative({ type: "chat", request })) {
        deps.schedule(deps.requestNext, 1000);
        return false;
      }
      pendingNative = pending;
      return true;
    }

    async function handleNativeResponse(response) {
      if (!validChatResponse(response)) {
        deps.schedule(deps.requestNext, 5000);
        return;
      }
      switch (response.status) {
        case "idle":
          if (await resumePendingFailure()) return;
          if (await resumePendingWatch()) return;
          if (await inspectPendingJournal()) return;
          deps.schedule(deps.requestNext, 1000);
          return;
        case "dispatch":
          await handleDispatch(response);
          return;
        case "ack":
          await handleAck(response);
          return;
        case "recorded":
          await handleRecorded(response);
          return;
        case "error":
          await handleError(response);
          return;
        default:
          console.error(
            "Heterarchy chat companion returned an unknown status",
            response.status,
          );
          deps.schedule(deps.requestNext, 5000);
      }
    }

    async function handleDispatch(dispatch) {
      if (!validChatDispatch(dispatch)) {
        console.error("Heterarchy chat dispatch failed validation");
        deps.schedule(deps.requestNext, 5000);
        return;
      }
      // Keep the provisioning poll alive while browser-side chat observation
      // waits for a user turn or a finished assistant turn.
      deps.schedule(deps.requestNext, 1000);
      let record = await loadJournal(dispatch.message_id);
      if (record !== null && !dispatchTargetMatchesRecord(record, dispatch)) {
        console.error("Heterarchy chat dispatch fence changed", dispatch.message_id);
        await reportUnknown(dispatch);
        return;
      }
      if (record === null) {
        try {
          record = await initializeJournal(dispatch, "dispatch_received");
        } catch (error) {
          await reportDispatchFailure(dispatch, errorMessage(error));
          return;
        }
      }
      if (dispatch.revision < record.revision) {
        console.error("Heterarchy chat dispatch revision is stale", dispatch.message_id);
        deps.schedule(deps.requestNext, 1000);
        return;
      }
      const revisionAdvanced = dispatch.revision > record.revision;
      if (revisionAdvanced) {
        try {
          record = await updateJournal(record, {
            revision: dispatch.revision,
            stage: "native_revision_adopted",
          });
        } catch (error) {
          await reportDispatchFailure(dispatch, errorMessage(error));
          return;
        }
      }
      if (
        dispatch.action === "execute" &&
        expiredAt(record.expiresAtMs) &&
        canFailExpiredWithoutInspection(record)
      ) {
        await markJournalFailed(record, "chat dispatch expired before browser send");
        return;
      }
      if (deliveryResolvedState(record.state)) {
        await reconcileTerminalDispatch(dispatch, record);
        return;
      }
      if (dispatch.action === "inspect") {
        if (activeInspections.has(record.messageId)) {
          deps.schedule(deps.requestNext, 1000);
          return;
        }
        activeInspections.add(record.messageId);
        try {
          await inspectDispatch(dispatch, record);
        } finally {
          activeInspections.delete(record.messageId);
        }
        return;
      }
      if (revisionAdvanced) {
        // A newer native revision may re-dispatch an uncertain message, but a
        // browser effect is never repeated under that new revision.
        await inspectDispatch(dispatch, record);
        return;
      }
      activeMessages.add(record.messageId);
      try {
        await executeDispatch(dispatch, record);
      } finally {
        activeMessages.delete(record.messageId);
      }
    }

    async function inspectPendingJournal() {
      if (pendingNative !== null) return false;
      const all = await chrome.storage.local.get(null);
      const candidates = Object.entries(all)
        .filter(
          ([key, value]) =>
            key.startsWith(CHAT_JOURNAL_PREFIX) &&
            isRecord(value) &&
            value.state !== "delivered" &&
            value.state !== "failed" &&
            boundedId(value.messageId) &&
            !activeMessages.has(value.messageId) &&
            !activeInspections.has(value.messageId) &&
            !inspectedMessages.has(value.messageId),
        )
        .sort(
          ([, left], [, right]) =>
            Number(left.updatedAtMs ?? 0) - Number(right.updatedAtMs ?? 0),
        );
      const candidate = candidates[0]?.[1];
      if (!isRecord(candidate)) return false;
      inspectedMessages.add(candidate.messageId);
      if (
        !postChat(
          { operation: "inspect", message_id: candidate.messageId },
          { kind: "inspect", messageId: candidate.messageId },
        )
      ) {
        inspectedMessages.delete(candidate.messageId);
        return false;
      }
      return true;
    }

    async function resumePendingFailure() {
      if (pendingNative !== null) return false;
      const all = await chrome.storage.local.get(null);
      const candidate = Object.entries(all)
        .filter(
          ([key, value]) =>
            key.startsWith(CHAT_JOURNAL_PREFIX) &&
            isRecord(value) &&
            value.state === "failure_report_pending" &&
            boundedId(value.messageId),
        )
        .sort(
          ([, left], [, right]) =>
            Number(left.updatedAtMs ?? 0) - Number(right.updatedAtMs ?? 0),
        )[0]?.[1];
      if (!isRecord(candidate)) return false;
      if (
        !postChat(
          {
            operation: "fail",
            message_id: candidate.messageId,
            expected_revision: candidate.revision,
          },
          { kind: "failure", messageId: candidate.messageId },
        )
      ) {
        return false;
      }
      return true;
    }

    async function resumePendingWatch() {
      if (pendingNative !== null) return false;
      const all = await chrome.storage.local.get(null);
      const watches = Object.entries(all)
        .filter(
          ([key, value]) =>
            key.startsWith(CHAT_WATCH_PREFIX) &&
            isRecord(value) &&
            boundedId(value.messageId) &&
            !terminalWatchState(value.state),
        )
        .sort(
          ([, left], [, right]) =>
            Number(left.updatedAtMs ?? 0) - Number(right.updatedAtMs ?? 0),
        );
      for (const [, watch] of watches) {
        const record = await loadJournal(watch.messageId);
        if (record === null) continue;
        if (watch.state === "incoming_pending") {
          if (await retryIncomingWatch(record, watch)) return true;
          continue;
        }
        if (!activeWatches.has(watch.messageId)) {
          startWatch(record, watch);
          return true;
        }
      }
      return false;
    }

    async function retryIncomingWatch(record, watch) {
      if (
        !boundedId(watch.pendingTurnId) ||
        (watch.pendingRole !== "assistant" && watch.pendingRole !== "user") ||
        !boundedBody(watch.pendingBody)
      ) {
        await finalizeWatch(record, watch, "failed", "pending incoming turn is invalid");
        return true;
      }
      if (
        !postChat(
          {
            operation: "incoming",
            reply_to_message_id: record.messageId,
            provider_conversation_id: record.providerConversationId,
            provider_turn_id: watch.pendingTurnId,
            role: watch.pendingRole,
            body: watch.pendingBody,
          },
          {
            kind: "incoming",
            messageId: record.messageId,
            watchKey: chatWatchKey(record.messageId),
            providerTurnId: watch.pendingTurnId,
          },
        )
      ) {
        return false;
      }
      return true;
    }

    async function reconcileTerminalDispatch(dispatch, record) {
      if (record.state === "delivered" && boundedId(record.userTurnId)) {
        if (typeof record.observedBody !== "string") {
          await markJournalUnknown(record, "observed provider body is missing");
          return;
        }
        await reportDelivery(
          record,
          record.userTurnId,
          record.observedBody,
        );
        return;
      }
      if (record.state === "failed") {
        await reportFailure(record);
        return;
      }
      if (record.state === "failure_report_pending") {
        await reportFailure(record);
        return;
      }
      await reportUnknown(dispatch);
    }

    async function executeDispatch(dispatch, record) {
      if (
        record.state !== "dispatched" &&
        record.state !== "ready" &&
        record.state !== "submitting" &&
        record.state !== "waiting"
      ) {
        await inspectDispatch(dispatch, record);
        return;
      }
      let surface;
      try {
        surface = await prepareSurface(record, record.state === "submitting");
      } catch (error) {
        await markJournalUnknown(record, errorMessage(error));
        return;
      }
      record = surface.record;
      if (
        expiredAt(record.expiresAtMs) &&
        canFailExpiredWithoutInspection(record)
      ) {
        await markJournalFailed(record, "chat dispatch expired before browser send");
        return;
      }
      const observation = surface.observation;
      if (!record.baselineCaptured) {
        record = await updateJournal(record, {
          state: "ready",
          baselineCaptured: true,
          baselineTurnIds: stableTurnIds(observation.turns),
          stage: "baseline_observed",
        });
      }
      if (record.state === "submitting") {
        await reconcileSubmitted(record);
        return;
      }
      const releaseComposer = await acquireComposerSlot(record);
      if (releaseComposer === null) return;
      try {
        const composerReady = await waitForComposer(record);
        if (!composerReady) return;
        let freshObservation;
        try {
          freshObservation = await readChatDocument(record, true);
        } catch (error) {
          await markJournalUnknown(record, errorMessage(error));
          return;
        }
        if (expiredAt(record.expiresAtMs)) {
          await markJournalFailed(record, "chat dispatch expired before browser send");
          return;
        }
        record = await updateJournal(record, {
          state: "submitting",
          stage: "send_started",
          baselineCaptured: true,
          baselineTurnIds: stableTurnIds(freshObservation.turns),
        });
        let submission;
        try {
          submission = await submitChatBody(record);
        } catch (error) {
          await markJournalUnknown(record, errorMessage(error));
          return;
        }
        if (!submission || submission.submitted !== true) {
          if (
            submission?.reason === "composer_is_busy" ||
            submission?.reason === "composer_unavailable" ||
            submission?.reason === "composer_form_unavailable" ||
            submission?.reason === "send_button_unavailable" ||
            submission?.reason === "composer_not_empty"
          ) {
            await updateJournal(record, {
              state: "waiting",
              stage: "composer_waiting",
              failure: submission.reason,
            });
            deps.schedule(deps.requestNext, 1000);
            return;
          }
          await markJournalFailed(
            record,
            typeof submission?.reason === "string"
              ? submission.reason
              : "composer submission was not confirmed",
          );
          return;
        }
        record = await updateJournal(record, {
          state: "submitted",
          stage: "message_submitted",
        });
        await reconcileSubmitted(record);
      } finally {
        releaseComposer();
      }
    }

    async function acquireComposerSlot(record) {
      const key = `${record.runtimeSessionId}:${record.providerConversationId}`;
      const deadline = Date.now() + CHAT_WAIT_MS;
      let waitingPersisted = false;
      while (true) {
        const owner = composerFlights.get(key);
        if (owner === undefined || owner === record.messageId) {
          composerFlights.set(key, record.messageId);
          return () => {
            if (composerFlights.get(key) === record.messageId) {
              composerFlights.delete(key);
            }
          };
        }
        if (Date.now() >= deadline) {
          await updateJournal(record, {
            state: "waiting",
            stage: "composer_waiting_for_owned_sender",
          });
          deps.schedule(deps.requestNext, 1000);
          return null;
        }
        if (!waitingPersisted) {
          await updateJournal(record, {
            state: "waiting",
            stage: "composer_waiting_for_owned_sender",
          });
          waitingPersisted = true;
        }
        await sleep(CHAT_POLL_MS);
      }
    }

    async function inspectDispatch(dispatch, record) {
      if (record.state === "delivered") {
        await reconcileTerminalDispatch(dispatch, record);
        return;
      }
      if (record.state === "failed") {
        await reportFailure(record);
        return;
      }
      if (record.state === "failure_report_pending") {
        await reportFailure(record);
        return;
      }
      if (record.state === "waiting") {
        await updateJournal(record, {
          stage: "composer_waiting_after_restart",
        });
        deps.schedule(deps.requestNext, 1000);
        return;
      }
      if (record.state === "unknown") {
        await inspectUnknown(dispatch, record);
        return;
      }
      try {
        const surface = await prepareSurface(record, true);
        record = surface.record;
        if (!record.baselineCaptured) {
          record = await updateJournal(record, {
            baselineCaptured: true,
            baselineTurnIds: stableTurnIds(surface.observation.turns),
            state: "inspecting",
            stage: "inspect_baseline",
          });
          await markJournalUnknown(record, "chat journal baseline was missing");
          return;
        }
        await reconcileSubmitted(record);
      } catch (error) {
        await markJournalUnknown(record, errorMessage(error));
      }
    }

    async function inspectUnknown(dispatch, record) {
      try {
        const surface = await prepareSurface(record, true);
        record = surface.record;
        if (!record.baselineCaptured) {
          await updateJournal(record, {
            stage: "unknown_baseline_missing",
            failure: "trusted browser baseline is unavailable",
          });
          deps.schedule(deps.requestNext, 5000);
          return;
        }
        const candidates = surface.observation.turns.filter(
          (turn) =>
            turn.role === "user" &&
            turn.stableId &&
            !record.baselineTurnIds.includes(turn.id) &&
            turn.body === record.body,
        );
        if (candidates.length > 1) {
          await updateJournal(record, {
            stage: "unknown_ambiguous_user_turn",
            failure: "multiple matching user turns observed",
          });
          deps.schedule(deps.requestNext, 5000);
          return;
        }
        const candidate = candidates[0];
        if (candidate !== undefined && boundedId(candidate.id)) {
          const next = await updateJournal(record, {
            state: "delivery_reporting",
            stage: "unknown_reconciled_user_turn",
            userTurnId: candidate.id,
            observedBody: candidate.body,
          });
          await reportDelivery(next, candidate.id, candidate.body);
          return;
        }
        await waitForUnknownUserTurn(record);
      } catch (error) {
        await updateJournal(record, {
          state: "unknown",
          stage: "unknown_inspection_failed",
          failure: errorMessage(error),
        });
        deps.schedule(deps.requestNext, 5000);
      }
    }

    async function waitForUnknownUserTurn(record) {
      const deadline = Date.now() + CHAT_WAIT_MS;
      while (Date.now() < deadline) {
        try {
          const observation = await readChatDocument(record, true);
          const candidates = observation.turns.filter(
            (turn) =>
              turn.role === "user" &&
              turn.stableId &&
              !record.baselineTurnIds.includes(turn.id) &&
              turn.body === record.body,
          );
          if (candidates.length > 1) {
            await updateJournal(record, {
              stage: "unknown_ambiguous_user_turn",
              failure: "multiple matching user turns observed",
            });
            deps.schedule(deps.requestNext, 5000);
            return;
          }
          const candidate = candidates[0];
          if (candidate !== undefined && boundedId(candidate.id)) {
            const next = await updateJournal(record, {
              state: "delivery_reporting",
              stage: "unknown_reconciled_user_turn",
              userTurnId: candidate.id,
              observedBody: candidate.body,
            });
            await reportDelivery(next, candidate.id, candidate.body);
            return;
          }
        } catch (error) {
          await updateJournal(record, {
            state: "unknown",
            stage: "unknown_inspection_failed",
            failure: errorMessage(error),
          });
          deps.schedule(deps.requestNext, 5000);
          return;
        }
        await sleep(CHAT_POLL_MS);
      }
      await updateJournal(record, {
        state: "unknown",
        stage: "unknown_reinspection_timeout",
        failure: "matching user turn was not observed before timeout",
      });
      deps.schedule(deps.requestNext, 5000);
    }

    async function waitForComposer(record) {
      const waiting = await updateJournal(record, {
        state: "waiting",
        stage: "composer_waiting",
      });
      const deadline = Date.now() + CHAT_WAIT_MS;
      while (Date.now() < deadline) {
        let status;
        try {
          status = await readChatComposerStatus(waiting);
        } catch (error) {
          await markJournalUnknown(waiting, errorMessage(error));
          return false;
        }
        if (status.ready === true) return true;
        if (
          status.reason !== "composer_is_busy" &&
          status.reason !== "composer_unavailable" &&
          status.reason !== "composer_form_unavailable" &&
          status.reason !== "send_button_unavailable" &&
          status.reason !== "composer_not_empty"
        ) {
          await markJournalFailed(waiting, status.reason ?? "composer is unavailable");
          return false;
        }
        await sleep(CHAT_POLL_MS);
      }
      await updateJournal(waiting, {
        state: "waiting",
        stage: "composer_waiting",
        failure: "owned composer remained busy during this polling pass",
      });
      deps.schedule(deps.requestNext, 1000);
      return false;
    }

    async function reconcileSubmitted(record) {
      let observation;
      try {
        observation = await readChatDocument(record, true);
      } catch (error) {
        await markJournalUnknown(record, errorMessage(error));
        return;
      }
      const candidates = observation.turns.filter(
        (turn) =>
          turn.role === "user" &&
          turn.stableId &&
          !record.baselineTurnIds.includes(turn.id) &&
          turn.body === record.body,
      );
      if (candidates.length > 1) {
        await markJournalUnknown(record, "multiple matching user turns observed");
        return;
      }
      const candidate = candidates[0];
      if (candidate === undefined) {
        await waitForSubmittedTurn(record);
        return;
      }
      if (!boundedId(candidate.id)) {
        await markJournalUnknown(record, "provider user turn identity is unavailable");
        return;
      }
      record = await updateJournal(record, {
        state: "delivery_reporting",
        stage: "user_turn_observed",
        userTurnId: candidate.id,
        observedBody: candidate.body,
      });
      await reportDelivery(record, candidate.id, candidate.body);
    }

    async function waitForSubmittedTurn(record) {
      const deadline = Date.now() + CHAT_WAIT_MS;
      while (Date.now() < deadline) {
        try {
          const observation = await readChatDocument(record, true);
          const candidates = observation.turns.filter(
            (turn) =>
              turn.role === "user" &&
              turn.stableId &&
              !record.baselineTurnIds.includes(turn.id) &&
              turn.body === record.body,
          );
          if (candidates.length > 1) {
            await markJournalUnknown(record, "multiple matching user turns observed");
            return;
          }
          const candidate = candidates[0];
          if (candidate !== undefined) {
            if (!boundedId(candidate.id)) {
              await markJournalUnknown(record, "provider user turn identity is unavailable");
              return;
            }
            const next = await updateJournal(record, {
              state: "delivery_reporting",
              stage: "user_turn_observed",
              userTurnId: candidate.id,
              observedBody: candidate.body,
            });
            await reportDelivery(next, candidate.id, candidate.body);
            return;
          }
        } catch (error) {
          await markJournalUnknown(record, errorMessage(error));
          return;
        }
        await sleep(CHAT_POLL_MS);
      }
      await markJournalUnknown(record, "provider user turn was not observed before timeout");
    }

    async function prepareSurface(record, allowExistingDocument) {
      let surface = await deps.reusableRuntimeSurface(
        record.runtimeSessionId,
        record.providerConversationId,
      );
      if (surface === null) {
        surface = await deps.openRuntimeOwnedTab(
          `https://chatgpt.com/c/${encodeURIComponent(record.providerConversationId)}`,
        );
      }
      if (!Number.isInteger(surface?.tabId) || !Number.isInteger(surface?.windowId)) {
        throw new Error("chat runtime surface has no identity");
      }
      let next = {
        ...record,
        tabId: surface.tabId,
        windowId: surface.windowId,
      };
      await waitForChatTab(next);
      const observation = await readChatDocument(next, true);
      if (
        allowExistingDocument &&
        record.documentId !== null &&
        observation.documentId !== record.documentId
      ) {
        throw new Error("chat document identity changed");
      }
      next = await updateJournal(next, {
        documentId: observation.documentId,
        navigationEpoch:
          record.documentId === null || record.documentId === observation.documentId
            ? record.navigationEpoch
            : record.navigationEpoch + 1,
        lastUrl: observation.url,
      });
      await deps.storeRuntimeSurface(
        next.runtimeSessionId,
        next.tabId,
        next.windowId,
        next.providerConversationId,
        next.documentId,
        next.navigationEpoch,
      );
      return { record: next, observation };
    }

    async function waitForChatTab(record) {
      const deadline = Date.now() + 30000;
      while (Date.now() < deadline) {
        const tab = await chrome.tabs.get(record.tabId);
        if (
          tab.status === "complete" &&
          deps.conversationIdFromUrl(tab.url) === record.providerConversationId
        ) {
          return;
        }
        await sleep(CHAT_POLL_MS);
      }
      throw new Error("ChatGPT conversation tab load timed out");
    }

    async function readChatDocument(record, includeTurns) {
      if (
        !(await deps.journalOwnsCurrentBrowserSession(record)) ||
        !Number.isInteger(record.tabId)
      ) {
        throw new Error("chat runtime surface identity is stale or missing");
      }
      const tab = await chrome.tabs.get(record.tabId);
      if (deps.conversationIdFromUrl(tab.url) !== record.providerConversationId) {
        throw new Error("chat conversation target changed");
      }
      const target = { tabId: record.tabId };
      if (typeof record.documentId === "string") {
        target.documentIds = [record.documentId];
      }
      const results = await chrome.scripting.executeScript({
        target,
        func: observeChatDocument,
        args: [includeTurns],
      });
      const observation = deps.topLevelInjectionResult(results);
      if (
        !isRecord(observation) ||
        typeof observation.documentId !== "string" ||
        !isRecord(observation.result) ||
        typeof observation.result.url !== "string" ||
        deps.conversationIdFromUrl(observation.result.url) !==
          record.providerConversationId
      ) {
        throw new Error("chat document observation is unavailable");
      }
      if (
        record.documentId !== null &&
        observation.documentId !== record.documentId
      ) {
        throw new Error("chat document identity changed");
      }
      return {
        documentId: observation.documentId,
        url: observation.result.url,
        turns: Array.isArray(observation.result.turns)
          ? observation.result.turns
          : [],
      };
    }

    async function submitChatBody(record) {
      if (!Number.isInteger(record.tabId) || typeof record.documentId !== "string") {
        throw new Error("chat submit surface identity is unavailable");
      }
        const results = await chrome.scripting.executeScript({
        target: { tabId: record.tabId, documentIds: [record.documentId] },
        func: submitChatPrompt,
        args: [record.body, record.expiresAtMs],
      });
      const result = deps.topLevelInjectionResult(results)?.result;
      if (!isRecord(result)) {
        throw new Error("chat composer observation is unavailable");
      }
      return result;
    }

    async function readChatComposerStatus(record) {
      if (!Number.isInteger(record.tabId) || typeof record.documentId !== "string") {
        throw new Error("chat composer surface identity is unavailable");
      }
      const results = await chrome.scripting.executeScript({
        target: { tabId: record.tabId, documentIds: [record.documentId] },
        func: inspectChatComposer,
      });
      const result = deps.topLevelInjectionResult(results)?.result;
      if (!isRecord(result) || typeof result.ready !== "boolean") {
        throw new Error("chat composer status is unavailable");
      }
      return result;
    }

    async function reportDelivery(record, providerTurnId, observedBody) {
      if (pendingNative !== null) return;
      if (!boundedId(providerTurnId)) {
        await markJournalUnknown(record, "provider turn identity is unavailable");
        return;
      }
      const receiptBody =
        typeof observedBody === "string"
          ? observedBody
          : typeof record.observedBody === "string"
            ? record.observedBody
            : null;
      if (
        receiptBody === null ||
        !boundedBody(receiptBody) ||
        receiptBody !== record.body
      ) {
        await markJournalUnknown(record, "observed provider body does not match the durable body");
        return;
      }
      const next = await updateJournal(record, {
        state: "delivery_reporting",
        stage: "delivery_observation_pending",
        userTurnId: providerTurnId,
        observedBody: receiptBody,
      });
      postChat(
        {
          operation: "observe_delivery",
          message_id: next.messageId,
          expected_revision: next.revision,
          provider_conversation_id: next.providerConversationId,
          provider_turn_id: providerTurnId,
          body: receiptBody,
        },
        { kind: "delivery", messageId: next.messageId },
      );
    }

    async function handleAck(response) {
      if (!boundedId(response.message_id) || !Number.isSafeInteger(response.revision)) {
        deps.schedule(deps.requestNext, 5000);
        return;
      }
      const pending = pendingNative;
      if (pending !== null && pending.messageId !== response.message_id) {
        pendingNative = null;
        deps.schedule(deps.requestNext, 1000);
        return;
      }
      const record = await loadJournal(response.message_id);
      if (record === null || pending?.messageId !== response.message_id) {
        pendingNative = null;
        deps.schedule(deps.requestNext, 1000);
        return;
      }
      if (!ackMatchesPendingKind(pending.kind, response.state)) {
        pendingNative = null;
        deps.schedule(deps.requestNext, 1000);
        return;
      }
      if (response.revision < record.revision) {
        // A late lower-revision acknowledgement cannot change durable state;
        // release only the in-memory request fence so recovery can inspect.
        pendingNative = null;
        deps.schedule(deps.requestNext, 1000);
        return;
      }
      pendingNative = null;
      const acknowledged =
        response.revision === record.revision
          ? record
          : await updateJournal(record, {
              revision: response.revision,
              stage: "native_revision_acknowledged",
            });
      if (pending.kind === "inspect") {
        const readbackState = ["delivered", "failed", "unknown"].includes(
          response.state,
        )
          ? response.state
          : acknowledged.state;
        await updateJournal(acknowledged, {
          state: readbackState,
          stage: `inspect_readback_${response.state}`,
        });
        deps.schedule(deps.requestNext, 1000);
        return;
      }
      switch (response.state) {
        case "delivered": {
          const next = await updateJournal(acknowledged, {
            state: "delivered",
            stage: "delivery_confirmed",
            responseCaptured: acknowledged.responseCaptured === true,
            responseWatchTerminal: acknowledged.responseWatchTerminal === true,
            responseWatchState:
              typeof acknowledged.responseWatchState === "string"
                ? acknowledged.responseWatchState
              : "starting",
          });
          if (!next.responseWatchTerminal) {
            try {
              await startResponseWatch(next);
            } catch (error) {
              await updateJournal(next, {
                responseWatchTerminal: true,
                responseWatchState: "failed",
                failure: `response watch unavailable: ${errorMessage(error)}`,
              });
            }
          }
          deps.schedule(deps.requestNext, 1000);
          return;
        }
        case "failed":
          await updateJournal(acknowledged, {
            state: "failed",
            stage: "delivery_failed",
          });
          deps.schedule(deps.requestNext, 1000);
          return;
        case "unknown":
          await updateJournal(acknowledged, {
            state: "unknown",
            stage: "delivery_unknown",
          });
          deps.schedule(deps.requestNext, 1000);
          return;
        case "queued":
        case "effect_pending":
          await updateJournal(acknowledged, { stage: `native_${response.state}` });
          deps.schedule(deps.requestNext, 1000);
          return;
        default:
          console.error("Heterarchy chat acknowledgement state is invalid");
          await markJournalUnknown(acknowledged, "invalid native chat acknowledgement");
      }
    }

    async function startResponseWatch(record) {
      let watch = await loadWatch(record.messageId);
      if (watch === null) {
        watch = await saveWatch({
          schemaVersion: CHAT_TRANSPORT_VERSION,
          messageId: record.messageId,
          revision: record.revision,
          workspaceId: record.workspaceId,
          runtimeSessionId: record.runtimeSessionId,
          providerConversationId: record.providerConversationId,
          documentId: record.documentId,
          userTurnId: record.userTurnId,
          baselineTurnIds: record.baselineTurnIds,
          capturedTurnIds: [],
          state: "watching",
          failure: null,
        });
      }
      startWatch(record, watch);
    }

    async function finalizeWatch(record, watch, state, failure = null) {
      const next = await updateWatch(watch, { state, failure });
      await updateJournal(record, {
        responseCaptured: state === "recorded",
        responseWatchTerminal: true,
        responseWatchState: state,
        responseWatchFailure: failure,
      });
      return next;
    }

    function startWatch(record, watch) {
      if (activeWatches.has(watch.messageId) || terminalWatchState(watch.state)) {
        return;
      }
      const flight = watchResponse(record, watch).finally(() => {
        activeWatches.delete(watch.messageId);
      });
      activeWatches.set(watch.messageId, flight);
    }

    async function watchResponse(record, watch) {
      const deadline = Date.now() + CHAT_WAIT_MS;
      while (Date.now() < deadline) {
        if (watch.state === "incoming_pending") return;
        let observation;
        try {
          observation = await readChatDocument(record, true);
        } catch (error) {
          await finalizeWatch(record, watch, "unknown", errorMessage(error));
          deps.schedule(deps.requestNext, 1000);
          return;
        }
        const userIndex = observation.turns.findIndex(
          (turn) => turn.stableId && turn.id === record.userTurnId,
        );
        if (userIndex < 0) {
          await sleep(CHAT_POLL_MS);
          continue;
        }
        const afterUser = observation.turns.slice(userIndex + 1);
        const nextUserIndex = afterUser.findIndex(
          (turn) => turn.stableId && turn.role === "user",
        );
        const beforeNextUser =
          nextUserIndex >= 0 ? afterUser.slice(0, nextUserIndex) : afterUser;
        const candidates = beforeNextUser.filter(
          (turn) =>
            turn.stableId &&
            turn.finished &&
            turn.role === "assistant" &&
            !watch.baselineTurnIds.includes(turn.id) &&
            !watch.capturedTurnIds.includes(turn.id),
        );
        const candidate = candidates[0];
        if (candidate === undefined) {
          if (nextUserIndex >= 0) {
            await finalizeWatch(
              record,
              watch,
              watch.capturedTurnIds.length > 0 ? "recorded" : "unknown",
              watch.capturedTurnIds.length > 0
                ? null
                : "next user turn boundary arrived before a finished response",
            );
            deps.schedule(deps.requestNext, 1000);
            return;
          }
          if (watch.capturedTurnIds.length > 0) {
            const quietPolls = Number(watch.quietPolls ?? 0) + 1;
            if (quietPolls >= 2) {
              await finalizeWatch(record, watch, "recorded");
              deps.schedule(deps.requestNext, 1000);
              return;
            }
            watch = await updateWatch(watch, { quietPolls });
          }
          await sleep(CHAT_POLL_MS);
          continue;
        }
        if (!boundedId(candidate.id)) {
          await finalizeWatch(
            record,
            watch,
            "failed",
            "provider turn identity is unavailable",
          );
          deps.schedule(deps.requestNext, 1000);
          return;
        }
        if (!boundedBody(candidate.body)) {
          await finalizeWatch(
            record,
            watch,
            "failed",
            byteLength(candidate.body) > CHAT_BODY_MAX_BYTES
              ? "provider public turn exceeds the 16 KiB body limit"
              : "provider public turn body is empty",
          );
          deps.schedule(deps.requestNext, 1000);
          return;
        }
        watch = await updateWatch(watch, {
          state: "incoming_pending",
          pendingTurnId: candidate.id,
          pendingRole: candidate.role,
          pendingBody: candidate.body,
          quietPolls: 0,
        });
        if (!postChat(
          {
            operation: "incoming",
            reply_to_message_id: record.messageId,
            provider_conversation_id: record.providerConversationId,
            provider_turn_id: candidate.id,
            role: candidate.role,
            body: candidate.body,
          },
          {
            kind: "incoming",
            messageId: record.messageId,
            watchKey: chatWatchKey(record.messageId),
            providerTurnId: candidate.id,
          },
        )) {
          await updateWatch(watch, {
            state: "watching",
            pendingTurnId: null,
            pendingRole: null,
            pendingBody: null,
          });
          deps.schedule(deps.requestNext, 1000);
          return;
        }
        return;
      }
      await updateWatch(watch, {
        state: "watching",
        failure: "finished provider response was not observed in this polling pass; continuing bounded polling",
        waitingSinceMs: watch.waitingSinceMs ?? Date.now(),
      });
      deps.schedule(deps.requestNext, 1000);
    }

    async function handleRecorded(response) {
      if (!boundedId(response.message_id) || pendingNative?.kind !== "incoming") {
        deps.schedule(deps.requestNext, 1000);
        return;
      }
      const pending = pendingNative;
      pendingNative = null;
      const watch = await loadWatch(pending.messageId);
      const record = await loadJournal(pending.messageId);
      if (watch === null || record === null) {
        deps.schedule(deps.requestNext, 1000);
        return;
      }
      const nextWatch = await updateWatch(watch, {
        state: "watching",
        capturedTurnIds: watch.capturedTurnIds.includes(pending.providerTurnId)
          ? watch.capturedTurnIds
          : [...watch.capturedTurnIds, pending.providerTurnId],
        recordedMessageIds: [
          ...(Array.isArray(watch.recordedMessageIds)
            ? watch.recordedMessageIds
            : []),
          response.message_id,
        ],
        pendingTurnId: null,
        pendingRole: null,
        pendingBody: null,
        quietPolls: 0,
      });
      await updateJournal(record, {
        responseCaptured: false,
        responseWatchTerminal: false,
        responseWatchState: "watching",
      });
      startWatch(record, nextWatch);
      deps.schedule(deps.requestNext, 1000);
    }

    async function handleError(response) {
      const pending = pendingNative;
      pendingNative = null;
      if (pending?.kind === "incoming") {
        const watch = await loadWatch(pending.messageId);
        if (watch !== null) {
          await updateWatch(watch, {
            state: "incoming_pending",
            failure: "native incoming message recording failed; retrying the same turn identity",
            retryCount: Number(watch.retryCount ?? 0) + 1,
          });
        }
      } else if (pending?.messageId !== undefined) {
        const record = await loadJournal(pending.messageId);
        if (record !== null) {
          if (pending.kind === "failure") {
            await updateJournal(record, {
              state: "failure_report_pending",
              stage: "failure_report_unknown",
              failure: "native failure acknowledgement was unavailable; retrying the same failure report",
            });
          } else {
            await updateJournal(record, {
              state: "unknown",
              stage: "unknown_report_unavailable",
              failure: "native chat operation failed",
            });
          }
        }
      }
      console.error("Heterarchy native chat operation failed", response.code);
      deps.schedule(deps.requestNext, 5000);
    }

    async function reportFailure(record) {
      if (pendingNative !== null) return;
      const next = await updateJournal(record, {
        state: "failure_report_pending",
        stage: "failure_report_pending",
      });
      postChat(
        {
          operation: "fail",
          message_id: next.messageId,
          expected_revision: next.revision,
        },
        { kind: "failure", messageId: next.messageId },
      );
    }

    async function reportDispatchFailure(dispatch, failure) {
      console.error("Heterarchy chat dispatch failed before browser send", failure);
      postChat(
        {
          operation: "fail",
          message_id: dispatch.message_id,
          expected_revision: dispatch.revision,
        },
        { kind: "failure", messageId: dispatch.message_id },
      );
    }

    async function reportUnknown(dispatch) {
      if (pendingNative !== null) return;
      const record = await loadJournal(dispatch.message_id);
      if (record !== null && record.revision === dispatch.revision) {
        await markJournalUnknown(record, "chat dispatch could not be reconciled", false);
      }
      postChat(
        {
          operation: "mark_unknown",
          message_id: dispatch.message_id,
          expected_revision: dispatch.revision,
        },
        { kind: "unknown", messageId: dispatch.message_id },
      );
    }

    async function markJournalFailed(record, failure, sendNative = true) {
      const next = await updateJournal(record, {
        state: "failure_report_pending",
        stage: "effect_failed",
        failure,
      });
      if (sendNative && pendingNative === null) {
        postChat(
          {
            operation: "fail",
            message_id: next.messageId,
            expected_revision: next.revision,
          },
          { kind: "failure", messageId: next.messageId },
        );
      }
      return next;
    }

    async function markJournalUnknown(record, failure, sendNative = true) {
      const next = await updateJournal(record, {
        state: "unknown",
        stage: "effect_unknown",
        failure,
      });
      if (sendNative && pendingNative === null) {
        postChat(
          {
            operation: "mark_unknown",
            message_id: next.messageId,
            expected_revision: next.revision,
          },
          { kind: "unknown", messageId: next.messageId },
        );
      }
      return next;
    }

    function onNativeDisconnect() {
      const pending = pendingNative;
      pendingNative = null;
      if (pending === null) return;
      void (async () => {
        if (pending.kind === "incoming") {
          const watch = await loadWatch(pending.messageId);
          if (watch !== null) {
            await updateWatch(watch, {
              state: "incoming_pending",
              failure: "native connection ended during incoming recording; retrying the same turn identity",
              retryCount: Number(watch.retryCount ?? 0) + 1,
            });
          }
          return;
        }
        const record = await loadJournal(pending.messageId);
        if (record !== null && pending.kind === "failure") {
          await updateJournal(record, {
            state: "failure_report_pending",
            stage: "native_connection_failure_report_unknown",
            failure: "native connection ended after failure admission; retrying the same failure report",
          });
        } else if (record !== null && !deliveryResolvedState(record.state)) {
          await updateJournal(record, {
            state: "unknown",
            stage: "native_connection_unknown",
            failure: "native connection ended after chat operation admission",
          });
        }
      })();
    }

    async function handleNativeError(error) {
      if (pendingNative === null) return false;
      await handleError({ status: "error", code: error?.code });
      return true;
    }

    function errorMessage(error) {
      return error instanceof Error ? error.message : "chat browser observation failed";
    }

    function stableTurnIds(turns) {
      return turns
        .filter((turn) => isRecord(turn) && turn.stableId && boundedId(turn.id))
        .map((turn) => turn.id);
    }

    function sleep(delayMs) {
      return new Promise((resolve) => setTimeout(resolve, delayMs));
    }

    void trimOwnedStorage(
      CHAT_JOURNAL_PREFIX,
      CHAT_JOURNAL_LIMIT,
      journalEvictable,
    );
    void trimOwnedStorage(
      CHAT_WATCH_PREFIX,
      CHAT_WATCH_LIMIT,
      terminalWatchState,
    );
    void resumeStoredWatches();

    return {
      handleNativeResponse,
      handleNativeError,
      onNativeDisconnect,
    };
  }

  function observeChatDocument(includeTurns) {
    const completedResponseSelectors = [
      '.turn-action-controls button[aria-label="복사"]',
      '.turn-action-controls button[aria-label="응답 평가"]',
      '.turn-action-controls button[aria-label="응답 다시 생성"]',
      '.turn-action-controls button[aria-label="Copy"]',
      '.turn-action-controls button[aria-label="Rate response"]',
      '.turn-action-controls button[aria-label="Regenerate response"]',
    ].join(",");
    const visible = (element) =>
      element instanceof HTMLElement &&
      element.isConnected &&
      element.getClientRects().length > 0 &&
      element.getAttribute("aria-hidden") !== "true";
    const publicUnitBody = (element, role) => {
      const content =
        role === "assistant"
          ? element.querySelector('[data-markdown-text-style="assistant-message"]')
          : element.querySelector('[data-user-message-bubble="true"] [dir="auto"]');
      if (!(content instanceof HTMLElement)) return "";
      return content.innerText ?? content.textContent ?? "";
    };
    const turns = [];
    if (includeTurns === true) {
      const nodes = document.querySelectorAll("[data-content-search-unit-key]");
      for (const element of nodes) {
        if (!visible(element)) continue;
        const id = element.getAttribute("data-content-search-unit-key");
        if (typeof id !== "string" || id.length === 0) continue;
        const role =
          element.querySelector('[data-user-message-bubble="true"]') !== null
            ? "user"
            : element.querySelector(
                  '[data-markdown-text-style="assistant-message"]',
                ) !== null
              ? "assistant"
              : null;
        if (role === null) continue;
        const unsafeAssistantSelectors =
          '[data-testid*="tool" i], [data-testid*="reason" i], ' +
          '[data-testid*="thought" i], [data-testid*="analysis" i]';
        if (
          role === "assistant" &&
          element.querySelector(
            '[data-markdown-text-style="assistant-message"]',
          ) === null
        ) {
          continue;
        }
        if (
          role === "assistant" &&
          (element.closest(unsafeAssistantSelectors) !== null ||
            element.querySelector(unsafeAssistantSelectors) !== null)
        ) {
          continue;
        }
        const text = publicUnitBody(element, role);
        if (text.length === 0) continue;
        const turn = element.closest("[data-content-search-turn-key]");
        const completionControls =
          turn?.querySelectorAll(completedResponseSelectors) ?? [];
        const hasCompletedControls = Array.from(completionControls).some(visible);
        turns.push({
          id,
          stableId: true,
          role,
          body: text,
          finished:
            role === "user" ||
            hasCompletedControls,
        });
      }
    }
    return { url: globalThis.location.href, turns };
  }

  function inspectChatComposer() {
    const composer = document.querySelector(
      '#prompt-textarea, div[contenteditable="true"][role="textbox"][data-composer-markdown], div[contenteditable="true"][role="textbox"]',
    );
    if (
      !(composer instanceof HTMLElement) ||
      !composer.isConnected ||
      composer.getAttribute("aria-disabled") === "true" ||
      composer.getAttribute("contenteditable") === "false"
    ) {
      return { ready: false, reason: "composer_unavailable" };
    }
    const form = composer.closest("form");
    const sendScope = form ?? document;
    const stopButton = sendScope.querySelector(
      'button[data-testid="stop-button"], ' +
        'button[data-testid="composer-stop-button"], ' +
        'button[aria-label="Stop streaming"], ' +
        'button[aria-label="Stop generating"], ' +
        'button[aria-label="Stop answering"], ' +
        'button[aria-label="중지"], ' +
        'button[aria-label*="중지"]',
    );
    if (stopButton instanceof HTMLButtonElement) {
      return { ready: false, reason: "composer_is_busy" };
    }
    const existing = (composer.innerText ?? composer.textContent ?? "").trim();
    if (existing.length !== 0) {
      return { ready: false, reason: "composer_not_empty" };
    }
    const sendButton =
      sendScope.querySelector('button[type="submit"][aria-label="보내기"]') ??
      sendScope.querySelector('button[type="submit"][aria-label^="Send" i]') ??
      sendScope.querySelector('button[data-testid="send-button"], button[aria-label="보내기"], button[aria-label^="Send" i]');
    if (
      !(sendButton instanceof HTMLButtonElement) ||
      !sendButton.isConnected
    ) {
      return { ready: false, reason: "send_button_unavailable" };
    }
    return { ready: true, reason: null };
  }

  function submitChatPrompt(body, expiresAtMs = null) {
    const isExpired =
      Number.isSafeInteger(expiresAtMs) && Date.now() >= expiresAtMs;
    if (isExpired) {
      return { submitted: false, reason: "dispatch_expired" };
    }
    const composer = document.querySelector(
      '#prompt-textarea, div[contenteditable="true"][role="textbox"][data-composer-markdown], div[contenteditable="true"][role="textbox"]',
    );
    if (
      !(composer instanceof HTMLElement) ||
      !composer.isConnected ||
      composer.getAttribute("aria-disabled") === "true" ||
      composer.getAttribute("contenteditable") === "false"
    ) {
      return { submitted: false, reason: "composer_unavailable" };
    }
    const form = composer.closest("form");
    const sendScope = form ?? document;
    const stopButton = sendScope.querySelector(
      'button[data-testid="stop-button"], ' +
        'button[data-testid="composer-stop-button"], ' +
        'button[aria-label="Stop streaming"], ' +
        'button[aria-label="Stop generating"], ' +
        'button[aria-label="Stop answering"], ' +
        'button[aria-label="중지"], ' +
        'button[aria-label*="중지"]',
    );
    if (stopButton instanceof HTMLButtonElement) {
      return { submitted: false, reason: "composer_is_busy" };
    }
    const existing = (composer.innerText ?? composer.textContent ?? "").trim();
    if (existing.length !== 0 || typeof body !== "string" || body.length === 0) {
      return { submitted: false, reason: "composer_not_empty" };
    }
    composer.focus();
    const selection = document.getSelection();
    if (
      selection === null ||
      document.activeElement !== composer ||
      !composer.isConnected
    ) {
      return { submitted: false, reason: "composer_focus_failed" };
    }
    selection.selectAllChildren(composer);
    if (
      !selection.rangeCount ||
      !composer.contains(selection.anchorNode) ||
      !composer.contains(selection.focusNode)
    ) {
      return { submitted: false, reason: "composer_selection_failed" };
    }
    if (!document.execCommand("insertText", false, body)) {
      return { submitted: false, reason: "composer_insert_failed" };
    }
    const observed = composer.textContent ?? "";
    if (observed.replace(/\s+/g, " ").trim() !== body.replace(/\s+/g, " ").trim()) {
      return { submitted: false, reason: "composer_body_mismatch" };
    }
    const sendButton =
      sendScope.querySelector('button[type="submit"][aria-label="보내기"]') ??
      sendScope.querySelector('button[type="submit"][aria-label^="Send" i]') ??
      sendScope.querySelector('button[data-testid="send-button"], button[aria-label="보내기"], button[aria-label^="Send" i]');
    if (
      !(sendButton instanceof HTMLButtonElement) ||
      !sendButton.isConnected ||
      sendButton.disabled ||
      sendButton.getAttribute("aria-disabled") === "true"
    ) {
      selection.selectAllChildren(composer);
      document.execCommand("delete", false);
      return { submitted: false, reason: "send_button_unavailable" };
    }
    if (Number.isSafeInteger(expiresAtMs) && Date.now() >= expiresAtMs) {
      selection.selectAllChildren(composer);
      document.execCommand("delete", false);
      return { submitted: false, reason: "dispatch_expired" };
    }
    sendButton.click();
    return { submitted: true };
  }

  globalThis.createHeterarchyChatTransport = createChatTransport;
  if (globalThis.__HETERARCHY_CHAT_TEST__ === true) {
    globalThis.__HETERARCHY_CHAT_TRANSPORT_TEST__ = {
      observeChatDocument,
      inspectChatComposer,
      submitChatPrompt,
      validChatDispatch,
    };
  }
})();
