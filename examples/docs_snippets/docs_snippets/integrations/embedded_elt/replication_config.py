replication_config = {
    "SOURCE": "MY_POSTGRES",
    "TARGET": "MY_DUCKDB",
    "defaults": {"mode": "full-refresh", "object": "{stream_schema}_{stream_table}"},
    "streams": {
        "public.accounts": None,
        "public.users": None,
        "public.finance_departments": {"object": "departments"},
    },
}
