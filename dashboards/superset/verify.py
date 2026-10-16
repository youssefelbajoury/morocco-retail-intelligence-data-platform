"""Phase 12 verification: assert the Superset BI layer is fully seeded."""

import json
import sys

import seed

EXPECTED_DATASETS = {
    "sales_analytics",
    "inventory_analytics",
    "purchases_analytics",
    "platform_health",
}
EXPECTED_DASHBOARDS = {
    "Sales Performance": 9,
    "Executive Overview": 8,
    "Inventory Health": 8,
    "Procurement": 8,
    "Platform Health": 9,
}


def main():
    token = seed.login()
    failures = []

    def check(ok, msg):
        print(("ok   " if ok else "FAIL ") + msg)
        if not ok:
            failures.append(msg)

    status, out = seed.api("GET", "/api/v1/dataset/?q=(page_size:1000)", token=token)
    ds = {d["table_name"]: d for d in out.get("result", [])}
    check(status == 200 and EXPECTED_DATASETS <= set(ds), f"datasets present ({sorted(ds)})")

    status, out = seed.api("GET", "/api/v1/chart/?q=(page_size:1000)", token=token)
    charts = out.get("result", [])
    by_name = {c["slice_name"]: c["id"] for c in charts}
    check(
        status == 200 and len(charts) == sum(EXPECTED_DASHBOARDS.values()),
        f"42 charts (got {len(charts)})",
    )
    dupes = len(charts) - len(by_name)
    check(dupes == 0, f"no duplicate slice names ({dupes})")

    status, out = seed.api("GET", "/api/v1/dashboard/?q=(page_size:1000)", token=token)
    dashes = {d["dashboard_title"]: d for d in out.get("result", [])}
    check(
        status == 200 and set(EXPECTED_DASHBOARDS) <= set(dashes),
        f"5 dashboards (got {sorted(dashes)})",
    )

    canonical = {c["id"] for c in charts}
    for title, expected_charts in EXPECTED_DASHBOARDS.items():
        d = dashes.get(title)
        if d is None:
            continue
        status, full = seed.api("GET", f"/api/v1/dashboard/{d['id']}", token=token)
        r = full["result"]
        pos = (
            json.loads(r["position_json"])
            if isinstance(r["position_json"], str)
            else r["position_json"]
        )
        md = (
            json.loads(r["json_metadata"])
            if isinstance(r["json_metadata"], str)
            else r["json_metadata"]
        )
        cids = []
        for v in pos.values():
            if isinstance(v, dict) and v.get("type") == "CHART":
                cids.append(v["meta"]["chartId"])
        check(
            len(cids) == expected_charts and pos.get("DASHBOARD_VERSION_KEY") == "v2",
            f"{title}: {len(cids)} chart slots, v2 layout",
        )
        orphans = [c for c in cids if c not in canonical]
        check(len(orphans) == 0, f"{title}: no orphaned chart refs ({orphans})")
        nf = md.get("native_filter_configuration", [])
        check(len(nf) >= 2, f"{title}: native filters configured ({len(nf)})")
        for f in nf:
            ok = bool(f.get("chartsInScope")) and (
                f.get("filterType") in ("filter_time", "filter_select")
            )
            check(ok, f"{title}: filter '{f.get('name')}' ({f.get('filterType')})")
            if f.get("filterType") == "filter_select":
                targets = f.get("targets", [])
                has_ids = all(
                    t.get("datasetId") is not None and t.get("datasetUuid") for t in targets
                )
                check(
                    has_ids and bool(targets),
                    f"{title}: filter '{f.get('name')}' targets carry datasetId+datasetUuid",
                )

    status, out = seed.api("GET", "/api/v1/database/?page_size=100", token=token)
    db = next((d for d in out.get("result", []) if d["database_name"] == "Retail Warehouse"), None)
    check(status == 200 and db is not None, "database 'Retail Warehouse' present")

    for name, sql in seed.DATASETS:
        status, out = seed.api(
            "POST",
            "/api/v1/sqllab/execute/",
            {
                "database_id": db["id"],
                "sql": sql,
                "client_id": seed.secrets.token_urlsafe(16),
            },
            token=token,
        )
        check(
            status == 200 and not out.get("errors"), f"dataset SQL executes: {name} (HTTP {status})"
        )

    print()
    if failures:
        print(f"{len(failures)} FAILURES")
        sys.exit(1)
    print("all checks passed")


if __name__ == "__main__":
    main()
