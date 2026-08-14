from chattocodex.control_panel.cloudflare_process_supervisor import (
    _QUICK_URL_PATTERN,
)


def test_quick_tunnel_url_pattern_extracts_trycloudflare_origin() -> None:
    line = "Your quick Tunnel has been created! Visit it at https://silver-river-42.trycloudflare.com"
    match = _QUICK_URL_PATTERN.search(line)
    assert match is not None
    assert match.group(0) == "https://silver-river-42.trycloudflare.com"
