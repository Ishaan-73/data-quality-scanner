"""Click CLI for the Data Quality Scanner."""

from __future__ import annotations

import sys
from typing import Optional

import click

from dqs.config.defaults import (
    CHECK_WITHIN_DIM_WEIGHTS,
    DEFAULT_THRESHOLDS,
    DIMENSION_CHECK_IDS,
    DIMENSION_WEIGHTS,
    MVP_PHASE,
    effective_weight,
)
from dqs.checks import CHECK_REGISTRY


@click.group()
@click.version_option(package_name="dqs")
def cli() -> None:
    """Data Quality Scanner — validate data across warehouses and lakehouses."""


# ---------------------------------------------------------------------------
# dqs scan
# ---------------------------------------------------------------------------

@cli.command("scan")
@click.option("--config", "config_path", required=True, help="Path to scan YAML config file.")
@click.option(
    "--mode",
    type=click.Choice(["live", "synthetic"]),
    default=None,
    help="Override mode from config (live=Mode A, synthetic=Mode B).",
)
@click.option(
    "--output",
    "output_format",
    type=click.Choice(["console", "json", "csv"]),
    default="console",
    show_default=True,
    help="Output format.",
)
@click.option("--output-file", default=None, help="Write output to this file path.")
@click.option("--mvp-only", is_flag=True, default=False, help="Run MVP-phase checks only.")
@click.option("--skip-screener", is_flag=True, default=False, help="Skip PII screener.")
@click.option("--screener-only", is_flag=True, default=False, help="Run PII screener only, skip DQS checks.")
def scan_cmd(
    config_path: str,
    mode: Optional[str],
    output_format: str,
    output_file: Optional[str],
    mvp_only: bool,
    skip_screener: bool,
    screener_only: bool,
) -> None:
    """Run a data quality scan from a YAML config file."""
    from dqs.scanner import scan_from_file

    try:
        if screener_only:
            _run_screener_only(config_path, output_format, output_file)
            return

        report = scan_from_file(
            config_path=config_path,
            mode=mode,
            output_format=output_format,
            output_path=output_file,
            mvp_only=mvp_only,
            skip_screener=skip_screener,
        )
        if output_format in ("json", "csv") and not output_file:
            from dqs.reporter import render
            click.echo(render(report, output_format=output_format))
        sys.exit(0 if report.checks_failed == 0 else 1)
    except Exception as exc:
        click.echo(f"[ERROR] {exc}", err=True)
        raise SystemExit(2) from exc


def _run_screener_only(config_path: str, output_format: str, output_file: Optional[str]) -> None:
    import yaml, json
    from pathlib import Path
    from dqs.config.models import ScanConfig, ScreenerConfig
    from dqs.connectors import get_connector
    from dqs.screener import run_screener
    from dqs.scanner import _get_profiles

    raw = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    config = ScanConfig.model_validate(raw)
    cfg = config.screener or ScreenerConfig()

    connector = get_connector(config.connector)
    connector.connect()
    try:
        profiles = _get_profiles(connector, config)
        manifests = run_screener(config, connector, profiles)
    finally:
        connector.close()

    if output_format == "json":
        data = [m.model_dump(mode="json") for m in manifests]
        content = json.dumps(data, indent=2, default=str)
        if output_file:
            Path(output_file).write_text(content, encoding="utf-8")
        else:
            click.echo(content)
    else:
        for m in manifests:
            flagged = [c for c in m.columns if c.status in ("HIGH", "MEDIUM", "REVIEW")]
            if flagged:
                click.echo(f"\n[{m.gate_decision}] {m.table}")
                for col in flagged:
                    click.echo(f"  {col.status:6s}  {col.column:30s}  {col.pii_type or ''}")


# ---------------------------------------------------------------------------
# dqs list-checks
# ---------------------------------------------------------------------------

@cli.command("list-checks")
@click.option("--dimension", default=None, help="Filter by dimension name.")
@click.option("--phase", type=click.Choice(["mvp", "phase2", "later"]), default=None)
def list_checks_cmd(dimension: Optional[str], phase: Optional[str]) -> None:
    """List all 33 quality checks with IDs, dimensions, and weights."""
    try:
        from rich.table import Table
        from rich.console import Console
        from rich import box

        console = Console()
        table = Table(
            "ID", "Check Name", "Dimension", "Dim Weight", "Check Weight",
            "Effective Weight", "Threshold", "Phase",
            box=box.ROUNDED,
            title="Data Quality Check Registry",
            header_style="bold cyan",
        )

        for check_id, check in sorted(CHECK_REGISTRY.items()):
            if dimension and check.dimension != dimension:
                continue
            chk_phase = MVP_PHASE.get(check_id, "?")
            if phase and chk_phase != phase:
                continue

            dim_w = DIMENSION_WEIGHTS.get(check.dimension, 0.0)
            chk_w = CHECK_WITHIN_DIM_WEIGHTS.get(check.dimension, {}).get(check_id, 0.0)
            eff_w = effective_weight(check_id, check.dimension)
            thresh = DEFAULT_THRESHOLDS.get(check_id)
            thresh_str = "advisory" if thresh is None else str(thresh)

            phase_color = {"mvp": "green", "phase2": "yellow", "later": "dim"}.get(chk_phase, "white")

            table.add_row(
                str(check_id),
                check.name,
                check.dimension,
                f"{dim_w:.0%}",
                f"{chk_w:.0%}",
                f"{eff_w:.4f}",
                thresh_str,
                f"[{phase_color}]{chk_phase}[/]",
            )

        console.print(table)

    except ImportError:
        # Plain text fallback
        header = f"{'ID':>3}  {'Check Name':<45} {'Dimension':<15} {'Eff.Weight':>10}  {'Phase'}"
        click.echo(header)
        click.echo("-" * len(header))
        for check_id, check in sorted(CHECK_REGISTRY.items()):
            if dimension and check.dimension != dimension:
                continue
            chk_phase = MVP_PHASE.get(check_id, "?")
            if phase and chk_phase != phase:
                continue
            eff_w = effective_weight(check_id, check.dimension)
            click.echo(f"{check_id:>3}  {check.name:<45} {check.dimension:<15} {eff_w:>10.4f}  {chk_phase}")


# ---------------------------------------------------------------------------
# dqs wizard
# ---------------------------------------------------------------------------

@cli.command("wizard")
@click.option("--config", "config_path", required=True, help="Path to a YAML file containing at least the connector block.")
@click.option("--tables", default=None, help="Comma-separated list of tables to profile. Omit to discover all tables.")
@click.option("--out", "output_path", default="questionnaire.yaml", show_default=True, help="Output questionnaire YAML path.")
@click.option("--form", "emit_form", is_flag=True, default=False, help="Also write an HTML form alongside the YAML.")
@click.option("--form-out", default="questionnaire_form.html", show_default=True, help="HTML form output path.")
def wizard_cmd(config_path: str, tables: Optional[str], output_path: str, emit_form: bool, form_out: str) -> None:
    """Introspect schema and generate a pre-filled questionnaire YAML (+ optional HTML form)."""
    import yaml
    from pathlib import Path
    from dqs.config.models import ScanConfig
    from dqs.connectors import get_connector
    from dqs.wizard.introspect import build_questionnaire
    from dqs.wizard.form import build_form

    raw = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    config = ScanConfig.model_validate(raw)
    connector = get_connector(config.connector)

    # Determine table list
    if tables:
        table_list = [t.strip() for t in tables.split(",") if t.strip()]
    else:
        connector.connect()
        try:
            rows = connector.execute_query(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema NOT IN ('information_schema','pg_catalog') "
                "ORDER BY table_name"
            )
            table_list = [r[0] for r in rows]
        finally:
            connector.close()

    if not table_list:
        click.echo("[ERROR] No tables found. Use --tables to specify them.", err=True)
        raise SystemExit(1)

    click.echo(f"Profiling {len(table_list)} table(s)...")
    profiles = {}
    connector.connect()
    try:
        for t in table_list:
            click.echo(f"  → {t}")
            try:
                profiles[t] = connector.get_schema_profile(t)
            except Exception as e:
                click.echo(f"    [WARN] Could not profile {t}: {e}", err=True)
    finally:
        connector.close()

    connector_cfg = config.connector.model_dump(exclude_none=True)
    q = build_questionnaire(connector_cfg, profiles, scan_name=config.scan_name)

    out_text = yaml.dump(q, sort_keys=False, allow_unicode=True, default_flow_style=False)
    Path(output_path).write_text(out_text, encoding="utf-8")
    click.echo(f"\nQuestionnaire written → {output_path}")
    click.echo("Fill in the TODO fields, then run:")
    click.echo(f"  dqs generate-config --questionnaire {output_path} --out scan_config.yaml")

    if emit_form:
        html = build_form(q)
        Path(form_out).write_text(html, encoding="utf-8")
        click.echo(f"HTML form written    → {form_out}")


# ---------------------------------------------------------------------------
# dqs generate-config
# ---------------------------------------------------------------------------

@cli.command("generate-config")
@click.option("--questionnaire", "q_path", required=True, help="Path to filled questionnaire YAML.")
@click.option("--out", "output_path", default="scan_config.yaml", show_default=True, help="Output scan config YAML path.")
def generate_config_cmd(q_path: str, output_path: str) -> None:
    """Convert a filled questionnaire YAML into a full scan_config.yaml."""
    import yaml
    from pathlib import Path
    from dqs.wizard.generate import generate_scan_config

    q = yaml.safe_load(Path(q_path).read_text(encoding="utf-8"))
    config = generate_scan_config(q)

    total_checks = len(config["checks"])
    check_ids_used = sorted({c["check_id"] for c in config["checks"]})

    out_text = yaml.dump(config, sort_keys=False, allow_unicode=True, default_flow_style=False)
    Path(output_path).write_text(out_text, encoding="utf-8")

    click.echo(f"Scan config written → {output_path}")
    click.echo(f"  {total_checks} check entries across {len(check_ids_used)} check types: {check_ids_used}")
    click.echo(f"Run with:  dqs scan --config {output_path} --mode live")


# ---------------------------------------------------------------------------
# dqs init
# ---------------------------------------------------------------------------

@cli.command("init")
@click.option(
    "--output", "output_path",
    default="scan_config.yaml",
    show_default=True,
    help="Output path for the generated config file.",
)
@click.option(
    "--dialect",
    type=click.Choice(["snowflake", "bigquery", "redshift", "databricks", "postgres", "duckdb", "synapse"]),
    default="snowflake",
    show_default=True,
    help="Target data warehouse dialect.",
)
def init_cmd(output_path: str, dialect: str) -> None:
    """Generate an example scan YAML configuration file."""
    import yaml
    from pathlib import Path

    example = _build_example_config(dialect)
    Path(output_path).write_text(
        yaml.dump(example, sort_keys=False, allow_unicode=True, default_flow_style=False),
        encoding="utf-8",
    )
    click.echo(f"Example config written to: {output_path}")
    click.echo("Edit the file to match your tables/columns, then run:")
    click.echo(f"  dqs scan --config {output_path} --mode live")


# ---------------------------------------------------------------------------
# dqs profile
# ---------------------------------------------------------------------------

@cli.command("profile")
@click.option("--config", "config_path", required=True, help="Path to scan YAML config file.")
@click.option("--table", required=True, help="Fully qualified table name to profile.")
@click.option(
    "--output",
    "output_format",
    type=click.Choice(["console", "json"]),
    default="console",
)
def profile_cmd(config_path: str, table: str, output_format: str) -> None:
    """Profile a single table's schema and statistics."""
    import json
    import yaml
    from pathlib import Path
    from dqs.config.models import ScanConfig
    from dqs.connectors import get_connector

    raw = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    config = ScanConfig.model_validate(raw)
    connector = get_connector(config.connector)

    with connector:
        profile = connector.get_schema_profile(table)

    if output_format == "json":
        click.echo(profile.model_dump_json(indent=2))
    else:
        click.echo(f"Table: {profile.table_name}  ({profile.row_count:,} rows)")
        click.echo(f"{'Column':<30} {'Type':<15} {'Nulls':>7} {'Unique':>10} {'Mean':>12}")
        click.echo("-" * 80)
        for col in profile.columns:
            mean_str = f"{col.mean:.2f}" if col.mean is not None else "—"
            unique_str = str(col.unique_count) if col.unique_count is not None else "?"
            click.echo(
                f"{col.name:<30} {col.dtype:<15} {col.null_rate:>6.1%} "
                f"{unique_str:>10} {mean_str:>12}"
            )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_example_config(dialect: str) -> dict:
    connector_examples = {
        "snowflake": {
            "dialect": "snowflake",
            "account": "your_account",
            "username": "your_user",
            "password": "your_password",
            "database": "MY_DB",
            "schema": "PUBLIC",
            "warehouse": "COMPUTE_WH",
            "role": "SYSADMIN",
        },
        "bigquery": {
            "dialect": "bigquery",
            "project": "my-gcp-project",
            "credentials_path": "/path/to/service_account.json",
        },
        "redshift": {
            "dialect": "redshift",
            "host": "my-cluster.us-east-1.redshift.amazonaws.com",
            "port": 5439,
            "database": "dev",
            "username": "awsuser",
            "password": "your_password",
        },
        "databricks": {
            "dialect": "databricks",
            "server_hostname": "adb-1234567890.azuredatabricks.net",
            "http_path": "/sql/1.0/warehouses/abc123",
            "access_token": "your_token",
        },
        "postgres": {
            "dialect": "postgres",
            "host": "localhost",
            "port": 5432,
            "database": "mydb",
            "username": "postgres",
            "password": "your_password",
        },
        "duckdb": {
            "dialect": "duckdb",
            "duckdb_path": ":memory:",
        },
        "synapse": {
            "dialect": "synapse",
            "workspace": "<your-workspace-name>",
            "database": "<dedicated-pool-or-db-name>",
            "pool_type": "dedicated",
            "auth": {
                "method": "service_principal",
                "tenant_id": "",
                "client_id": "",
                "client_secret": "",
            },
        },
    }

    return {
        "scan_name": "my_first_dqs_scan",
        "mode": "live",
        "connector": connector_examples[dialect],
        "mvp_only": False,
        "dimensions": [],
        "checks": [
            {
                "check_id": 1,
                "enabled": True,
                "table": "schema.my_table",
                "column": "email",
            },
            {
                "check_id": 2,
                "enabled": True,
                "table": "schema.my_table",
                "columns": ["customer_id", "email"],
            },
            {
                "check_id": 5,
                "enabled": True,
                "table": "schema.my_table",
                "pk_column": "id",
            },
            {
                "check_id": 9,
                "enabled": True,
                "table": "schema.my_table",
                "column": "status",
                "allowed_values": ["active", "inactive", "pending"],
            },
            {
                "check_id": 10,
                "enabled": True,
                "table": "schema.my_table",
                "column": "email",
                "pattern": "^[A-Za-z0-9._%+\\-]+@[A-Za-z0-9.\\-]+\\.[A-Za-z]{2,}$",
            },
            {
                "check_id": 11,
                "enabled": True,
                "table": "schema.my_table",
                "column": "age",
                "min_value": 0,
                "max_value": 120,
            },
            {
                "check_id": 20,
                "enabled": True,
                "table": "schema.my_table",
                "timestamp_column": "updated_at",
                "freshness_sla_hours": 24,
            },
            {
                "check_id": 23,
                "enabled": True,
                "today_table": "schema.my_table",
                "prior_table": "schema.my_table_yesterday",
                "volume_change_band": 0.3,
            },
        ],
    }
