import pytest
from pydantic import ValidationError

from chattocodex.control_panel.control_panel_dto import RemoteMode
from chattocodex.schemas.control_panel_schemas import (
    ControlPanelPreferencesRequest,
    normalize_custom_origin,
)
from chattocodex.schemas.wake_schemas import WakeTargetRetireBatchRequest


def test_custom_origin_normalization_accepts_hostname() -> None:
    assert (
        normalize_custom_origin(value="mcp.example.com/") == "https://mcp.example.com"
    )


def test_custom_origin_rejects_path() -> None:
    with pytest.raises(ValueError):
        normalize_custom_origin(value="https://mcp.example.com/path")


def test_quick_mode_rejects_custom_origin() -> None:
    with pytest.raises(ValidationError):
        ControlPanelPreferencesRequest(
            workspace_root="/tmp/projects",
            remote_mode=RemoteMode.QUICK_TUNNEL,
            custom_origin="https://mcp.example.com",
            mcp_port=8765,
        )


def test_batch_retirement_schema_accepts_nonempty_unique_project_targets() -> None:
    payload = WakeTargetRetireBatchRequest.model_validate(
        {
            "project_id": "prj_sample_12345678",
            "targets": [
                {
                    "wake_target_id": f"wtgt_{index:032x}",
                    "expected_revision": index,
                }
                for index in (1, 2)
            ],
        }
    )

    assert len(payload.targets) == 2


@pytest.mark.parametrize(
    "targets",
    (
        [],
        [
            {
                "wake_target_id": "wtgt_00000000000000000000000000000001",
                "expected_revision": 0,
            }
        ]
        * 33,
        [
            {
                "wake_target_id": "wtgt_00000000000000000000000000000001",
                "expected_revision": 0,
            },
            {
                "wake_target_id": "wtgt_00000000000000000000000000000001",
                "expected_revision": 1,
            },
        ],
    ),
    ids=("empty", "more-than-32", "duplicate-id"),
)
def test_batch_retirement_schema_rejects_invalid_target_collection(
    targets: list[dict[str, object]],
) -> None:
    with pytest.raises(ValidationError):
        WakeTargetRetireBatchRequest.model_validate(
            {"project_id": "prj_sample_12345678", "targets": targets}
        )
