type PersistentWorkerRegistration = {
  kind?: unknown;
  projectId?: unknown;
  wakeTargetId?: unknown;
  conversationId?: unknown;
};

type PersistentWorkerTarget = {
  project_id?: unknown;
  wake_target_id?: unknown;
  wake_revision?: unknown;
  status?: unknown;
};

declare function chatConversationId(rawUrl: unknown): string | null;

function persistentWorkerConversationMatchesRegistration(
  registration: PersistentWorkerRegistration | null | undefined,
  rawUrl: unknown,
): boolean {
  if (
    registration?.kind !== "worker" ||
    typeof registration.projectId !== "string" ||
    !registration.projectId ||
    typeof registration.wakeTargetId !== "string" ||
    !registration.wakeTargetId ||
    typeof registration.conversationId !== "string" ||
    !registration.conversationId
  ) {
    return false;
  }
  return chatConversationId(rawUrl) === registration.conversationId;
}

function persistentWorkerWakeMatchesRegistration(
  registration: PersistentWorkerRegistration | null | undefined,
  target: PersistentWorkerTarget | null | undefined,
  rawUrl: unknown,
): boolean {
  if (!persistentWorkerConversationMatchesRegistration(registration, rawUrl)) {
    return false;
  }
  const wakeRevision = Number(target?.wake_revision);
  return Boolean(
    target?.status === "waking" &&
      target.project_id === registration?.projectId &&
      target.wake_target_id === registration?.wakeTargetId &&
      Number.isSafeInteger(wakeRevision) &&
      wakeRevision > 0,
  );
}
