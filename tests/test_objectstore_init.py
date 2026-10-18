from pathlib import Path
import importlib.util

MODULE_PATH = Path(__file__).parents[1] / "docker" / "objectstore-init" / "init_s3.py"
spec = importlib.util.spec_from_file_location("init_s3", MODULE_PATH)
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(module)


class FakeS3:
    def __init__(self):
        self.buckets = {}

    def list_buckets(self):
        return {"Buckets": [{"Name": name} for name in sorted(self.buckets)]}

    def create_bucket(self, Bucket):
        self.buckets.setdefault(Bucket, {})
        return {}

    def head_bucket(self, Bucket):
        if Bucket not in self.buckets:
            raise AssertionError(f"bucket missing: {Bucket}")
        return {}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.buckets[Bucket][Key] = Body
        return {}

    def head_object(self, Bucket, Key):
        if Key not in self.buckets.get(Bucket, {}):
            raise AssertionError(f"object missing: {Bucket}/{Key}")
        return {"ContentLength": len(self.buckets[Bucket][Key])}


def test_required_bucket_defaults():
    assert module.required_buckets("warehouse,lakehouse,checkpoints") == [
        "warehouse", "lakehouse", "checkpoints"
    ]


def test_ensure_and_verify_buckets():
    client = FakeS3()
    buckets = ["warehouse", "lakehouse", "checkpoints"]
    module.ensure_buckets(client, buckets, attempts=1, delay_seconds=0)
    module.verify_only(client, buckets)
    assert set(client.buckets) == set(buckets)
    for bucket in buckets:
        assert module.MARKER_KEY in client.buckets[bucket]
    for key in module.LAKEHOUSE_PREFIX_MARKERS:
        assert key in client.buckets["lakehouse"]


def test_sync_nested_data_idempotent_and_detects_corruption(tmp_path):
    import io
    import json
    import pytest

    class DataS3(FakeS3):
        def upload_file(self, filename, bucket, key):
            self.buckets[bucket][key] = Path(filename).read_bytes()

        def get_object(self, Bucket, Key):
            return {'Body': io.BytesIO(self.buckets[Bucket][Key])}

    (tmp_path / 'input').mkdir()
    (tmp_path / 'input' / 'sales.csv').write_text('sale_id\n1\n')
    (tmp_path / 'binary.parquet').write_bytes(bytes(range(256)))
    client = DataS3()
    client.create_bucket(Bucket='lakehouse')
    first = module.sync_data(client, tmp_path)
    assert set(first) == {'donnees/input/sales.csv', 'donnees/binary.parquet'}
    assert module.sync_data(client, tmp_path) == first
    assert module.sync_data(client, tmp_path, verify_only=True) == first
    client.buckets['lakehouse']['donnees/binary.parquet'] = b'corrupt'
    with pytest.raises(RuntimeError, match='S3 content mismatch'):
        module.sync_data(client, tmp_path, verify_only=True)
    module.sync_data(client, tmp_path)
    client.buckets['lakehouse']['donnees-manifest.json'] = json.dumps({}).encode()
    with pytest.raises(RuntimeError, match='manifest'):
        module.sync_data(client, tmp_path, verify_only=True)


def test_sync_ebrasil_bronze_copies_and_verifies_nine_parquet_files(tmp_path):
    import io
    import pytest

    class BronzeS3(FakeS3):
        def upload_file(self, filename, bucket, key):
            self.buckets[bucket][key] = Path(filename).read_bytes()

        def get_object(self, Bucket, Key):
            return {"Body": io.BytesIO(self.buckets[Bucket][Key])}

    source = tmp_path / "donnees"
    ebrasil = source / "db_ebrasil"
    ebrasil.mkdir(parents=True)
    for index in range(9):
        (ebrasil / f"table_{index}.parquet").write_bytes(f"parquet-{index}".encode())
    client = BronzeS3()
    client.create_bucket(Bucket="lakehouse")

    manifest = module.sync_ebrasil_bronze(client, source)
    assert len(manifest) == 9
    assert all(key.startswith("bronze/ebrasil/") for key in client.buckets["lakehouse"])
    assert module.sync_ebrasil_bronze(client, source, verify_only=True) == manifest
    client.buckets["lakehouse"]["bronze/ebrasil/table_0.parquet"] = b"corrupt"
    with pytest.raises(RuntimeError, match="bronze content mismatch"):
        module.sync_ebrasil_bronze(client, source, verify_only=True)
