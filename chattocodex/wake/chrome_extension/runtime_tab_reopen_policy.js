// A backend WAKING delivery is the only authority that can create a Chat tab.
// These values deliberately live beside the browser-side policy so the runtime
// adapter can be tested without importing broker or consumer-group semantics.
const MAX_RUNTIME_OPEN_IN_FLIGHT = 1;
const MIN_RUNTIME_OPEN_INTERVAL_MS = 12_000;
const runtimeOpeningConversationIds = new Set();
let runtimeOpenInFlightCount = 0;
let runtimeLastOpenedAt = 0;

function runtimeTargetPriority(target) {
  switch (target?.status) {
    case "waking":
      return 0;
    case "busy":
    case "active":
      return 1;
    case "idle":
      return 2;
    default:
      return 3;
  }
}

function runtimeTargetIdentity(registration, target) {
  if (!registration || !target) {
    return null;
  }
  const kind = registration.kind === "main" ? "main" : "worker";
  const targetId =
    kind === "main" ? target.main_target_id : target.wake_target_id;
  const wakeRevision = Number(target.wake_revision);
  if (
    typeof targetId !== "string" ||
    !targetId ||
    !Number.isSafeInteger(wakeRevision) ||
    wakeRevision <= 0
  ) {
    return null;
  }
  return {
    kind,
    targetId,
    wakeRevision,
    key: `${kind}:${targetId}:${wakeRevision}`,
  };
}

function isRuntimeTargetBoundToRegistration(registration, target, identity) {
  if (!identity || !registration || !target) {
    return false;
  }
  const registeredTargetId =
    identity.kind === "main"
      ? registration.mainTargetId
      : registration.wakeTargetId;
  return (
    registeredTargetId === identity.targetId &&
    target.project_id === registration.projectId
  );
}

function runtimeRegistrationBindingMatches(current, expected) {
  if (!current || !expected) {
    return false;
  }
  const currentKind = current.kind === "main" ? "main" : "worker";
  const expectedKind = expected.kind === "main" ? "main" : "worker";
  const currentTargetId =
    currentKind === "main" ? current.mainTargetId : current.wakeTargetId;
  const expectedTargetId =
    expectedKind === "main" ? expected.mainTargetId : expected.wakeTargetId;
  return (
    currentKind === expectedKind &&
    currentTargetId === expectedTargetId &&
    current.projectId === expected.projectId &&
    current.conversationId === expected.conversationId
  );
}

function hasNewerRuntimeAdmission(registration, identity) {
  return (
    Number(registration?.lastAdmittedWakeRevision || 0) > identity?.wakeRevision
  );
}

function hasRuntimeRecoveryAdmission(registration, identity) {
  return (
    registration?.lastAdmittedDeliveryKey === identity?.key &&
    Number(registration?.lastAdmittedWakeRevision) === identity?.wakeRevision
  );
}
