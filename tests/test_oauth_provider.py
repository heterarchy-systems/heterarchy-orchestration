import asyncio
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from mcp.server.auth.provider import (
    AuthorizationCode,
    AuthorizationParams,
    RefreshToken,
    RegistrationError,
    TokenError,
)
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken
from pydantic import AnyUrl

from chattocodex.orchestrator_mcp.auth.single_user_oauth_provider import (
    SingleUserOAuthProvider,
)
from chattocodex.shared.security.owner_token_service import OwnerTokenService

_PUBLIC_URL = "https://agent.example.test"
_RESOURCE_URL = f"{_PUBLIC_URL}/mcp"
_REDIRECT_URI = "https://chatgpt.com/aip/gpt/oauth/callback"


def _provider(*, state_root: Path) -> SingleUserOAuthProvider:
    return SingleUserOAuthProvider(
        state_root=state_root,
        public_url=_PUBLIC_URL,
        resource_url=_RESOURCE_URL,
        allowed_redirect_hosts=("chatgpt.com",),
        access_token_ttl_seconds=3600,
        refresh_token_ttl_seconds=86_400,
    )


def _client(*, client_id: str = "chatgpt-test") -> OAuthClientInformationFull:
    return OAuthClientInformationFull(
        client_id=client_id,
        redirect_uris=[AnyUrl(_REDIRECT_URI)],
        scope="chattocodex offline_access",
    )


def _authorization_params(*, scopes: list[str]) -> AuthorizationParams:
    return AuthorizationParams(
        state="state-value",
        scopes=scopes,
        code_challenge="A" * 43,
        redirect_uri=AnyUrl(_REDIRECT_URI),
        redirect_uri_provided_explicitly=True,
        resource=_RESOURCE_URL,
    )


async def _approve_authorization(
    *,
    provider: SingleUserOAuthProvider,
    client: OAuthClientInformationFull,
    owner_token: str,
) -> tuple[str, str]:
    approval_url = await provider.authorize(
        client,
        _authorization_params(scopes=["chattocodex", "offline_access"]),
    )
    pending_id = parse_qs(urlparse(approval_url).query)["pending_id"][0]
    redirect_url = await provider.approve_owner_authorization(
        pending_id=pending_id,
        owner_token=owner_token,
    )
    assert redirect_url is not None
    authorization_code = parse_qs(urlparse(redirect_url).query)["code"][0]
    return pending_id, authorization_code


def _one_success_and_invalid_grant(
    outcomes: list[OAuthToken | BaseException],
) -> OAuthToken:
    successful = [outcome for outcome in outcomes if isinstance(outcome, OAuthToken)]
    rejected = [outcome for outcome in outcomes if isinstance(outcome, TokenError)]
    assert len(successful) == 1
    assert len(rejected) == 1
    assert rejected[0].error == "invalid_grant"
    return successful[0]


def test_oauth_authorization_tokens_and_refresh_survive_provider_restart(
    tmp_path: Path,
) -> None:
    async def verify() -> None:
        owner_token = OwnerTokenService(state_root=tmp_path).generate()
        provider = _provider(state_root=tmp_path)
        client = _client()
        await provider.register_client(client)

        approval_url = await provider.authorize(
            client,
            _authorization_params(scopes=["chattocodex", "offline_access"]),
        )
        pending_id = parse_qs(urlparse(approval_url).query)["pending_id"][0]
        assert (
            await provider.approve_owner_authorization(
                pending_id=pending_id,
                owner_token="wrong-owner-token",
            )
            is None
        )
        redirect_url = await provider.approve_owner_authorization(
            pending_id=pending_id,
            owner_token=owner_token,
        )
        assert redirect_url is not None
        redirect_query = parse_qs(urlparse(redirect_url).query)
        authorization_code_text = redirect_query["code"][0]
        assert redirect_query["state"] == ["state-value"]

        restarted_provider = _provider(state_root=tmp_path)
        restarted_client = await restarted_provider.get_client(client.client_id)
        assert restarted_client is not None
        authorization_code = await restarted_provider.load_authorization_code(
            restarted_client,
            authorization_code_text,
        )
        assert authorization_code is not None
        token_pair = await restarted_provider.exchange_authorization_code(
            restarted_client,
            authorization_code,
        )
        assert token_pair.refresh_token is not None

        second_restart = _provider(state_root=tmp_path)
        access_token = await second_restart.load_access_token(token_pair.access_token)
        assert access_token is not None
        assert access_token.resource == _RESOURCE_URL
        assert "chattocodex" in access_token.scopes

        refresh_token = await second_restart.load_refresh_token(
            restarted_client,
            token_pair.refresh_token,
        )
        assert refresh_token is not None
        refreshed_pair = await second_restart.exchange_refresh_token(
            restarted_client,
            refresh_token,
            [],
        )
        assert refreshed_pair.refresh_token is not None
        assert refreshed_pair.refresh_token != token_pair.refresh_token
        assert (
            await second_restart.load_refresh_token(
                restarted_client,
                token_pair.refresh_token,
            )
            is None
        )
        refreshed_access = await second_restart.load_access_token(
            refreshed_pair.access_token
        )
        assert refreshed_access is not None
        assert "chattocodex" in refreshed_access.scopes

    asyncio.run(verify())


def test_oauth_rejects_untrusted_redirect_and_scope_expansion(tmp_path: Path) -> None:
    async def verify() -> None:
        OwnerTokenService(state_root=tmp_path).generate()
        provider = _provider(state_root=tmp_path)
        bad_client = OAuthClientInformationFull(
            client_id="bad-client",
            redirect_uris=[AnyUrl("https://evil.example/callback")],
        )
        with pytest.raises(RegistrationError):
            await provider.register_client(bad_client)

        client = _client(client_id="scope-client")
        await provider.register_client(client)
        approval_url = await provider.authorize(
            client,
            _authorization_params(scopes=["chattocodex"]),
        )
        pending_id = parse_qs(urlparse(approval_url).query)["pending_id"][0]
        owner_token = OwnerTokenService(state_root=tmp_path).generate()
        redirect_url = await provider.approve_owner_authorization(
            pending_id=pending_id,
            owner_token=owner_token,
        )
        assert redirect_url is not None
        authorization_code_text = parse_qs(urlparse(redirect_url).query)["code"][0]
        authorization_code = await provider.load_authorization_code(
            client,
            authorization_code_text,
        )
        assert authorization_code is not None
        token_pair = await provider.exchange_authorization_code(
            client,
            authorization_code,
        )
        assert token_pair.refresh_token is not None
        refresh_token = await provider.load_refresh_token(
            client, token_pair.refresh_token
        )
        assert refresh_token is not None

        with pytest.raises(TokenError):
            await provider.exchange_refresh_token(
                client,
                refresh_token,
                ["chattocodex", "offline_access"],
            )

    asyncio.run(verify())


def test_pending_authorization_is_consumed_once_under_concurrent_approval(
    tmp_path: Path,
) -> None:
    async def verify() -> None:
        owner_token = OwnerTokenService(state_root=tmp_path).generate()
        first_provider = _provider(state_root=tmp_path)
        second_provider = _provider(state_root=tmp_path)
        client = _client(client_id="concurrent-approval-client")
        await first_provider.register_client(client)
        approval_url = await first_provider.authorize(
            client,
            _authorization_params(scopes=["chattocodex"]),
        )
        pending_id = parse_qs(urlparse(approval_url).query)["pending_id"][0]

        outcomes = await asyncio.gather(
            first_provider.approve_owner_authorization(
                pending_id=pending_id,
                owner_token=owner_token,
            ),
            second_provider.approve_owner_authorization(
                pending_id=pending_id,
                owner_token=owner_token,
            ),
        )

        redirects = [outcome for outcome in outcomes if outcome is not None]
        assert len(redirects) == 1
        authorization_code = parse_qs(urlparse(redirects[0]).query)["code"][0]
        assert await first_provider.load_authorization_code(client, authorization_code)
        assert len(list((tmp_path / "auth" / "codes").glob("*.json"))) == 1
        assert not list((tmp_path / "auth" / "pending").glob("*.json"))

    asyncio.run(verify())


def test_authorization_code_has_exactly_one_concurrent_exchange_winner(
    tmp_path: Path,
) -> None:
    async def verify() -> None:
        owner_token = OwnerTokenService(state_root=tmp_path).generate()
        setup_provider = _provider(state_root=tmp_path)
        client = _client(client_id="concurrent-code-client")
        await setup_provider.register_client(client)
        _, raw_code = await _approve_authorization(
            provider=setup_provider,
            client=client,
            owner_token=owner_token,
        )
        first_provider = _provider(state_root=tmp_path)
        second_provider = _provider(state_root=tmp_path)
        first_code = await first_provider.load_authorization_code(client, raw_code)
        second_code = await second_provider.load_authorization_code(client, raw_code)
        assert isinstance(first_code, AuthorizationCode)
        assert isinstance(second_code, AuthorizationCode)

        outcomes = await asyncio.gather(
            first_provider.exchange_authorization_code(client, first_code),
            second_provider.exchange_authorization_code(client, second_code),
            return_exceptions=True,
        )

        winner = _one_success_and_invalid_grant(outcomes)
        assert await first_provider.load_access_token(winner.access_token)
        assert winner.refresh_token is not None
        assert await first_provider.load_refresh_token(client, winner.refresh_token)
        assert not list((tmp_path / "auth" / "codes").glob("*.json"))
        assert len(list((tmp_path / "auth" / "access").glob("*.json"))) == 1
        assert len(list((tmp_path / "auth" / "refresh").glob("*.json"))) == 1

    asyncio.run(verify())


def test_refresh_token_has_exactly_one_concurrent_rotation_winner(
    tmp_path: Path,
) -> None:
    async def verify() -> None:
        owner_token = OwnerTokenService(state_root=tmp_path).generate()
        setup_provider = _provider(state_root=tmp_path)
        client = _client(client_id="concurrent-refresh-client")
        await setup_provider.register_client(client)
        _, raw_code = await _approve_authorization(
            provider=setup_provider,
            client=client,
            owner_token=owner_token,
        )
        code = await setup_provider.load_authorization_code(client, raw_code)
        assert code is not None
        initial_pair = await setup_provider.exchange_authorization_code(client, code)
        assert initial_pair.refresh_token is not None
        first_provider = _provider(state_root=tmp_path)
        second_provider = _provider(state_root=tmp_path)
        first_refresh = await first_provider.load_refresh_token(
            client, initial_pair.refresh_token
        )
        second_refresh = await second_provider.load_refresh_token(
            client, initial_pair.refresh_token
        )
        assert isinstance(first_refresh, RefreshToken)
        assert isinstance(second_refresh, RefreshToken)

        outcomes = await asyncio.gather(
            first_provider.exchange_refresh_token(client, first_refresh, []),
            second_provider.exchange_refresh_token(client, second_refresh, []),
            return_exceptions=True,
        )

        winner = _one_success_and_invalid_grant(outcomes)
        assert (
            await first_provider.load_refresh_token(client, initial_pair.refresh_token)
            is None
        )
        assert await first_provider.load_access_token(winner.access_token)
        assert winner.refresh_token is not None
        assert await first_provider.load_refresh_token(client, winner.refresh_token)
        assert len(list((tmp_path / "auth" / "access").glob("*.json"))) == 2
        assert len(list((tmp_path / "auth" / "refresh").glob("*.json"))) == 1

    asyncio.run(verify())
