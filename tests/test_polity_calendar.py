from backend.fetchers.polity_calendar import EVENTS, TYPE_META, get_calendar_data


def test_every_event_has_required_fields():
    for ev in EVENTS:
        assert ev["id"] and ev["name"] and ev["category"] and ev["type"]
        assert ev["category"] in ("party", "state")
        assert ev["type"] in TYPE_META
        assert "confirmed" in ev
        assert ev.get("summary") and ev.get("source")
        # Either a real date or an explanatory note, never neither
        assert ev.get("date_start") or ev.get("date_note")
        if ev["confirmed"]:
            assert ev.get("date_start"), f"{ev['id']} marked confirmed but has no date_start"


def test_event_ids_are_unique():
    ids = [ev["id"] for ev in EVENTS]
    assert len(ids) == len(set(ids))


def test_dates_are_iso_and_ordered():
    from datetime import date
    for ev in EVENTS:
        if ev.get("date_start"):
            date.fromisoformat(ev["date_start"])
        if ev.get("date_end"):
            date.fromisoformat(ev["date_end"])
            assert ev["date_end"] >= ev["date_start"]


def test_get_calendar_data_splits_past_and_upcoming():
    data = get_calendar_data(today="2026-07-19")
    assert data["today"] == "2026-07-19"
    all_ids = {ev["id"] for ev in data["past"] + data["upcoming"] + data["unscheduled"]}
    assert all_ids == {ev["id"] for ev in EVENTS}

    for ev in data["past"]:
        assert ev["date_start"] <= "2026-07-19"
    for ev in data["upcoming"]:
        assert ev["date_start"] > "2026-07-19"
    for ev in data["unscheduled"]:
        assert not ev.get("date_start")

    # past sorted most-recent-first, upcoming sorted soonest-first
    assert data["past"] == sorted(data["past"], key=lambda e: e["date_start"], reverse=True)
    assert data["upcoming"] == sorted(data["upcoming"], key=lambda e: e["date_start"])


def test_next_is_soonest_upcoming_or_none():
    data = get_calendar_data(today="2026-07-19")
    if data["upcoming"]:
        assert data["next"] == data["upcoming"][0]
    else:
        assert data["next"] is None


def test_every_event_carries_type_label_and_color():
    data = get_calendar_data(today="2026-07-19")
    for ev in data["past"] + data["upcoming"] + data["unscheduled"]:
        assert ev["type_label"]
        assert ev["type_color"].startswith("#")


def test_api_endpoint_returns_calendar():
    import backend.api as api
    client = api.app.test_client()
    resp = client.get("/api/polity/calendar")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "past" in data and "upcoming" in data and "unscheduled" in data
    assert "type_meta" in data
