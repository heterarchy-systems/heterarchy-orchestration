"use strict";
function persistentWorkerConversationMatchesRegistration(registration, rawUrl) {
    if (registration?.kind !== "worker" ||
        typeof registration.projectId !== "string" ||
        !registration.projectId ||
        typeof registration.wakeTargetId !== "string" ||
        !registration.wakeTargetId ||
        typeof registration.conversationId !== "string" ||
        !registration.conversationId) {
        return false;
    }
    return chatConversationId(rawUrl) === registration.conversationId;
}
function persistentWorkerWakeMatchesRegistration(registration, target, rawUrl) {
    if (!persistentWorkerConversationMatchesRegistration(registration, rawUrl)) {
        return false;
    }
    const wakeRevision = Number(target?.wake_revision);
    return Boolean(target?.status === "waking" &&
        target.project_id === registration?.projectId &&
        target.wake_target_id === registration?.wakeTargetId &&
        Number.isSafeInteger(wakeRevision) &&
        wakeRevision > 0);
}
