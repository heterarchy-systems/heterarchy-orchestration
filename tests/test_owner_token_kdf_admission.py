import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from chattocodex.auth.owner_token_service import (
    OwnerTokenService,
    _derive_digest,
)


def test_concurrent_owner_token_verification_bounds_scrypt_execution(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = OwnerTokenService(state_root=tmp_path / "state")
    owner_token = service.generate()
    candidates = [owner_token, "wrong-owner-token"] * 4
    caller_count = len(candidates)
    callers_ready = threading.Barrier(caller_count + 1)
    counter_lock = threading.Lock()
    active_derivations = 0
    max_active_derivations = 0

    def measured_digest(*, token: str, salt: bytes) -> bytes:
        nonlocal active_derivations, max_active_derivations
        with counter_lock:
            active_derivations += 1
            max_active_derivations = max(max_active_derivations, active_derivations)
        try:
            time.sleep(0.02)
            return _derive_digest(token=token, salt=salt)
        finally:
            with counter_lock:
                active_derivations -= 1

    def verify_after_release(candidate: str) -> bool:
        callers_ready.wait(timeout=2)
        return service.verify(candidate=candidate)

    monkeypatch.setattr(
        "chattocodex.auth.owner_token_service._derive_digest",
        measured_digest,
    )
    with ThreadPoolExecutor(max_workers=caller_count) as executor:
        futures = [
            executor.submit(verify_after_release, candidate) for candidate in candidates
        ]
        callers_ready.wait(timeout=2)
        outcomes = [future.result(timeout=5) for future in futures]

    assert outcomes == [True, False] * 4
    assert max_active_derivations == 1
