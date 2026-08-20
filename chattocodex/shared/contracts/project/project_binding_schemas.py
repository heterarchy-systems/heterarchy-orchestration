"""Pydantic contracts for the local project binding boundary."""

from datetime import datetime
from typing import Literal, Self

from pydantic import field_validator, model_validator

from chattocodex.shared.contracts.project.project_schemas import (
    ProjectId,
    ProjectName,
    ProjectRoot,
)
from chattocodex.shared.contracts.schema_base import StrictSchemaModel
from chattocodex.shared.contracts.types.workspace.project_binding_types import (
    AdapterAppLabel,
    AdapterAppReference,
    AdapterProjectLabel,
    AdapterProjectPath,
    AdapterProjectReference,
    AdapterProvider,
    ConnectorEpoch,
    ProjectBindingRevision,
)
from chattocodex.shared.element_collect.project_enums import (
    AdapterProviderKind,
)


class ProjectBindingBoundaryModel(StrictSchemaModel):
    """Base configuration shared by project binding API contracts."""


class ProjectBindingAdapterMetadata(ProjectBindingBoundaryModel):
    """Optional browser-observed metadata that never authorizes a binding."""

    provider: AdapterProvider | None = None
    project_ref: AdapterProjectReference | None = None
    project_label: AdapterProjectLabel | None = None
    project_path: AdapterProjectPath | None = None
    app_ref: AdapterAppReference | None = None
    app_label: AdapterAppLabel | None = None
    observed_at: datetime | None = None

    @model_validator(mode="after")
    def require_consistent_chatgpt_project_identity(self) -> Self:
        """Require a complete exact ChatGPT project identity at the API boundary. Args: None. Returns: Self."""
        if self.provider != "chatgpt":
            return self
        if self.project_ref is None or self.project_path is None:
            raise ValueError(
                "chatgpt adapter metadata requires both project_ref and project_path"
            )
        if self.project_path != f"/g/{self.project_ref}":
            raise ValueError("chatgpt project_path must exactly match /g/{project_ref}")
        if (self.app_ref is None) != (self.app_label is None):
            raise ValueError(
                "chatgpt connector identity requires both app_ref and app_label"
            )
        return self

    @field_validator("observed_at")
    @classmethod
    def require_timezone(cls, value: datetime | None) -> datetime | None:
        """Require adapter observation timestamps to include timezone data.

        Args:
            value: Optional adapter observation timestamp.

        Returns:
            The validated timestamp or ``None``.

        Raises:
            ValueError: If a supplied timestamp is timezone-naive.
        """
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("adapter observation timestamp must be timezone-aware")
        return value


class ProjectBindingRecord(ProjectBindingBoundaryModel):
    """Durable canonical local-project binding record."""

    schema_version: Literal[1] = 1
    project_id: ProjectId
    project_name: ProjectName
    project_root: ProjectRoot
    adapter_metadata: ProjectBindingAdapterMetadata | None = None
    connector_epoch: ConnectorEpoch = 0
    revision: ProjectBindingRevision
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require durable binding timestamps to include timezone data.

        Args:
            value: Timestamp read from or written to durable state.

        Returns:
            The validated timezone-aware timestamp.

        Raises:
            ValueError: If ``value`` is timezone-naive.
        """
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("binding timestamp must be timezone-aware")
        return value


class LegacyLabelOnlyProjectBindingAdapterMetadata(ProjectBindingBoundaryModel):
    """Narrow reader for the known 0.1.11 label-only connector record shape."""

    provider: AdapterProviderKind
    project_ref: AdapterProjectReference
    project_label: AdapterProjectLabel | None = None
    project_path: AdapterProjectPath
    app_ref: None = None
    app_label: AdapterAppLabel
    observed_at: datetime | None = None

    @model_validator(mode="after")
    def require_exact_legacy_project_identity(self) -> Self:
        """Accept only the exact historical ChatGPT project identity shape. Args: None. Returns: Self."""
        if self.project_path != f"/g/{self.project_ref}":
            raise ValueError("legacy chatgpt project_path must match /g/{project_ref}")
        return self

    @field_validator("observed_at")
    @classmethod
    def require_timezone(cls, value: datetime | None) -> datetime | None:
        """Require a legacy observation timestamp to be timezone-aware.

        Args:
            value: Optional persisted adapter observation timestamp.

        Returns:
            The validated timestamp or ``None``.

        Raises:
            ValueError: If a supplied timestamp is timezone-naive.
        """
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("adapter observation timestamp must be timezone-aware")
        return value


class LegacyLabelOnlyProjectBindingRecord(ProjectBindingBoundaryModel):
    """Persistence-only compatibility record for known 0.1.11 state."""

    schema_version: Literal[1] = 1
    project_id: ProjectId
    project_name: ProjectName
    project_root: ProjectRoot
    adapter_metadata: LegacyLabelOnlyProjectBindingAdapterMetadata
    connector_epoch: ConnectorEpoch = 0
    revision: ProjectBindingRevision
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require legacy durable timestamps to include timezone data.

        Args:
            value: Timestamp read from known legacy state.

        Returns:
            The validated timezone-aware timestamp.

        Raises:
            ValueError: If ``value`` is timezone-naive.
        """
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("binding timestamp must be timezone-aware")
        return value


class ProjectBindingUpsertRequest(ProjectBindingBoundaryModel):
    """Request to create a binding or update adapter observation metadata."""

    adapter_metadata: ProjectBindingAdapterMetadata | None = None
    expected_revision: ProjectBindingRevision | None = None


class ProjectBindingView(ProjectBindingBoundaryModel):
    """Opaque-safe view of a canonical local project binding."""

    project_id: ProjectId
    project_name: ProjectName
    project_root: ProjectRoot
    adapter_metadata: ProjectBindingAdapterMetadata | None = None
    connector_epoch: ConnectorEpoch
    revision: ProjectBindingRevision
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require binding view timestamps to include timezone data.

        Args:
            value: Timestamp returned by the local Control Panel.

        Returns:
            The validated timezone-aware timestamp.

        Raises:
            ValueError: If ``value`` is timezone-naive.
        """
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("binding view timestamp must be timezone-aware")
        return value


class ProjectBindingResponse(ProjectBindingBoundaryModel):
    """Successful response containing one project binding."""

    ok: Literal[True] = True
    binding: ProjectBindingView


class ProjectBindingsResponse(ProjectBindingBoundaryModel):
    """Successful response containing all valid local project bindings."""

    ok: Literal[True] = True
    bindings: tuple[ProjectBindingView, ...]


class ConnectorAppIdentity(ProjectBindingBoundaryModel):
    app_ref: AdapterAppReference
    app_label: AdapterAppLabel


class ProjectConnectorView(ProjectBindingBoundaryModel):
    project_id: ProjectId
    provider: AdapterProvider
    project_ref: AdapterProjectReference
    project_path: AdapterProjectPath
    app_identity: ConnectorAppIdentity
    connector_epoch: ConnectorEpoch
    revision: ProjectBindingRevision


class ProjectConnectorResponse(ProjectBindingBoundaryModel):
    ok: Literal[True] = True
    connector: ProjectConnectorView
