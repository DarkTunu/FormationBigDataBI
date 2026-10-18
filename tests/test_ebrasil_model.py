from pathlib import Path
import ast
import yaml

ROOT = Path(__file__).resolve().parents[1]

def test_ebrasil_files_and_startup_order():
    source_files = sorted((ROOT / "donnees" / "db_ebrasil").glob("*.parquet"))
    assert len(source_files) == 8
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text())
    services = compose["services"]
    silver = services["ebrasil-silver-init"]
    druid = services["ebrasil-druid-init"]
    check = services["ebrasil-check"]
    assert "objectstore-init" in silver["depends_on"]
    assert "spark-master" in silver["depends_on"]
    assert silver["depends_on"]["spark-master"]["condition"] == "service_healthy"
    assert druid["depends_on"]["ebrasil-silver-init"]["condition"] == "service_completed_successfully"
    assert druid["depends_on"]["druid-router"]["condition"] == "service_healthy"
    assert check["depends_on"]["ebrasil-druid-init"]["condition"] == "service_completed_successfully"
    assert "full" in silver["profiles"] and "full" in druid["profiles"]

def test_ebrasil_processing_and_control_scripts_parse():
    scripts = [
        ROOT / "spark/jobs/20_build_ebrasil_warehouse.py",
        ROOT / "docker/objectstore-init/init_s3.py",
        ROOT / "docker/ebrasil-druid/ingest.py",
        ROOT / "docker/ebrasil-druid/check.py",
    ]
    for path in scripts:
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    model = scripts[0].read_text(encoding="utf-8")
    for table in (
        "dim_date", "dim_time", "dim_customer", "dim_product", "dim_seller",
        "dim_geography", "dim_order_status", "dim_payment_type",
        "fact_order_item", "fact_order", "fact_payment", "fact_logistics",
        "fact_review", "mart_customer_rfm",
    ):
        assert f'"{table}"' in model
    assert "bronze/ebrasil" in model and "silver/ebrasil" in model
    checker = scripts[3].read_text(encoding="utf-8")
    assert "EBRASIL_ALL_CHECKS_OK" in checker
