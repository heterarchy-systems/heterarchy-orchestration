function chatConversationId(rawUrl) {
  try {
    const url = new URL(String(rawUrl));
    if (url.origin !== "https://chatgpt.com") {
      return null;
    }
    const segments = url.pathname.split("/").filter(Boolean);
    const conversationMarker = segments.lastIndexOf("c");
    if (
      conversationMarker < 0 ||
      conversationMarker + 1 >= segments.length
    ) {
      return null;
    }
    const conversationId = segments[conversationMarker + 1];
    return conversationId || null;
  } catch (_error) {
    return null;
  }
}

function normalizedChatConversationUrl(rawUrl) {
  try {
    const url = new URL(String(rawUrl));
    if (!chatConversationId(url.href)) {
      return null;
    }
    return `${url.origin}${url.pathname.replace(/\/+$/, "")}`;
  } catch (_error) {
    return null;
  }
}

// ChatGPT currently decorates GPT Project route segments with a human-readable
// slug and redirects project landings to `/project`.  The stable `g-p-...`
// identifier remains the security boundary; the slug is presentation only.
const CHATGPT_STABLE_PROJECT_REF_PATTERN =
  /^(g-p-[0-9a-f]{32})(?:-[a-z0-9][a-z0-9-]*)?$/i;

function canonicalChatGPTProjectRef(rawSegment) {
  const segment = typeof rawSegment === "string" ? rawSegment.trim() : "";
  if (!segment || segment.includes("/")) {
    return null;
  }
  const stableMatch = segment.match(CHATGPT_STABLE_PROJECT_REF_PATTERN);
  return stableMatch ? stableMatch[1] : segment;
}

function chatGPTProjectRoute(rawUrl) {
  try {
    const url = new URL(String(rawUrl));
    if (url.origin !== "https://chatgpt.com" || url.search || url.hash) {
      return null;
    }
    const segments = url.pathname.split("/").filter(Boolean);
    if (segments[0] !== "g" || !segments[1]) {
      return null;
    }
    const projectRef = canonicalChatGPTProjectRef(segments[1]);
    if (!projectRef) {
      return null;
    }
    const tail = segments.slice(2);
    if (tail.length === 0 || (tail.length === 1 && tail[0] === "project")) {
      return {
        projectRef,
        projectPath: `/g/${projectRef}`,
        routeKind: "project",
        conversationId: null,
      };
    }
    if (tail.length === 2 && tail[0] === "c" && tail[1]) {
      return {
        projectRef,
        projectPath: `/g/${projectRef}`,
        routeKind: "conversation",
        conversationId: tail[1],
      };
    }
    return null;
  } catch (_error) {
    return null;
  }
}

function chatGPTProjectConversationContext(rawUrl) {
  const route = chatGPTProjectRoute(rawUrl);
  if (!route || route.routeKind !== "conversation") {
    return null;
  }
  const url = new URL(String(rawUrl));
  return {
    provider: "chatgpt",
    projectRef: route.projectRef,
    projectPath: route.projectPath,
    conversationUrl: `${url.origin}${url.pathname.replace(/\/+$/, "")}`,
    conversationId: route.conversationId,
  };
}

function chatGPTProjectLandingMatches({ rawUrl, projectPath, projectRef }) {
  const canonicalRef = canonicalChatGPTProjectRef(projectRef);
  if (!canonicalRef || canonicalRef !== projectRef) {
    return false;
  }
  const expected = chatGPTProjectRoute(
    new URL(String(projectPath), "https://chatgpt.com").href,
  );
  const current = chatGPTProjectRoute(rawUrl);
  return (
    expected?.routeKind === "project" &&
    expected.projectRef === canonicalRef &&
    expected.projectPath === `/g/${canonicalRef}` &&
    current?.routeKind === "project" &&
    current.projectRef === canonicalRef
  );
}
