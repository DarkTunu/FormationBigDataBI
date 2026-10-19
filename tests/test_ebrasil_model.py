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
    assert check["profiles"] == ["checks"]
    assert "checks" in silver["profiles"] and "checks" in druid["profiles"]
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
    star_tables = (
        "fact_order_item", "fact_order", "fact_payment", "fact_logistics",
        "fact_review", "mart_customer_rfm",
    )
    for table in star_tables:
        assert f'persist("{table}"' in model
    assert model.count('persist("fact_') == 5
    assert 'persist("mart_customer_rfm"' in model
    assert "join_dimension(" in model
    for dimension in (
        "dim_geography", "dim_customer", "dim_product", "dim_seller",
        "dim_date", "dim_time",
    ):
        assert f'persist("{dimension}"' not in model
    assert "bronze/ebrasil" in model and "silver/ebrasil" in model
    assert "getFileSystem(hadoop_conf)" in model
    assert '"spark.sql.legacy.parquet.nanosAsLong", "true"' in model
    assert "timestamp_micros" in model and "DIV 1000" in model
    assert "FileSystem.get(" not in model
    assert 'F.date_trunc("hour"' in model
    assert 'return (F.hour(ts) * 10000).cast("int")' in model
    assert 'hour_timestamp(purchase)' in model
    assert '.alias("minute")' not in model
    assert '.alias("second")' not in model
    assert "HH:mm:ss" not in model
    checker = scripts[3].read_text(encoding="utf-8")
    assert "EBRASIL_ALL_CHECKS_OK" in checker
