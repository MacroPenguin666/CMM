"""
Load and query the China policy source registry.
"""

from pathlib import Path
from typing import Optional

import yaml


REGISTRY_PATH = Path(__file__).parent / "registry.yaml"


def load_registry(path: Path = REGISTRY_PATH) -> dict:
    """Load the full source registry from YAML."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_all_sources(registry: Optional[dict] = None) -> list[dict]:
    """Return a flat list of every source entry across all sections."""
    if registry is None:
        registry = load_registry()

    sources = []
    list_sections = [
        "central_government",
        "ministries",
        "regulators",
        "party_bodies",
        "judiciary",
        "state_media",
        "open_resources",
        "direct_feeds",
    ]
    for section in list_sections:
        items = registry.get(section, [])
        if isinstance(items, list):
            sources.extend(items)
    return sources


def get_rsshub_feeds(registry: Optional[dict] = None) -> list[dict]:
    """Return sources that have at least one RSSHub route configured."""
    if registry is None:
        registry = load_registry()
    rsshub_base = registry.get("rsshub_base", "https://rsshub.app")
    feeds = []
    for src in get_all_sources(registry):
        routes = src.get("rsshub_routes", [])
        if routes:
            for r in routes:
                feeds.append(
                    {
                        "name": src["name"],
                        "name_cn": src.get("name_cn", ""),
                        "category": src.get("category", ""),
                        "url": f"{rsshub_base}{r['route']}",
                        "description": r.get("description", ""),
                    }
                )
    return feeds


def get_direct_feeds(registry: Optional[dict] = None) -> list[dict]:
    """Return direct RSS feeds (no RSSHub needed, work from outside China)."""
    if registry is None:
        registry = load_registry()
    feeds = registry.get("direct_feeds", [])
    return [
        {
            "name": f["name"],
            "name_cn": f.get("name_cn", ""),
            "category": f.get("category", ""),
            "url": f["url"],
            "description": f.get("notes", ""),
        }
        for f in feeds
    ]


def get_all_feeds(registry: Optional[dict] = None) -> list[dict]:
    """Return all fetchable feeds: direct feeds + RSSHub feeds."""
    if registry is None:
        registry = load_registry()
    return get_direct_feeds(registry) + get_rsshub_feeds(registry)


def get_sources_by_category(
    category: str, registry: Optional[dict] = None
) -> list[dict]:
    """Filter sources by category tag."""
    return [
        s for s in get_all_sources(registry) if s.get("category") == category
    ]


def get_sources_by_content_type(
    content_type: str, registry: Optional[dict] = None
) -> list[dict]:
    """Return sources that publish a given content type."""
    return [
        s
        for s in get_all_sources(registry)
        if content_type in s.get("content_types", [])
    ]


def get_wechat_accounts(registry: Optional[dict] = None) -> list[dict]:
    """Return the list of tracked WeChat public accounts."""
    if registry is None:
        registry = load_registry()
    social = registry.get("social_media", {})
    return social.get("wechat_accounts", [])


# ---------------------------------------------------------------------------
# Quick summary when run directly
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    reg = load_registry()
    all_src = get_all_sources(reg)
    direct = get_direct_feeds(reg)
    rsshub = get_rsshub_feeds(reg)
    wechat = get_wechat_accounts(reg)

    print(f"Total sources:        {len(all_src)}")
    print(f"Direct RSS feeds:     {len(direct)}  (work from anywhere)")
    print(f"RSSHub feeds:         {len(rsshub)}  (need self-hosted instance in China)")
    print(f"WeChat accounts:      {len(wechat)}")
    print()

    categories = {}
    for s in all_src:
        cat = s.get("category", "unknown")
        categories[cat] = categories.get(cat, 0) + 1
    print("By category:")
    for cat, count in sorted(categories.items()):
        print(f"  {cat}: {count}")

    print()
    print("Direct feeds (verified working):")
    for f in direct:
        print(f"  {f['name']}: {f['url']}")
