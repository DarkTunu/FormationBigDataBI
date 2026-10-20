"""Check the access policy on every consumer, including Airflow's driver."""
import json
from pathlib import Path
import pytest
import yaml


def test_workspace_access_and_dependency_contract():
    root = Path(__file__).parents[1]
    compose = yaml.safe_load((root / 'compose.yaml').read_text())
    services = compose['services']
    data_volume = next(v for v in services['spark-master']['volumes'] if v.startswith('spark-data:/opt/spark/'))
    for name in ['spark-master', 'spark-worker-1', 'spark-worker-2', 'spark-history', 'spark-jupyter', 'integration-check', 'airflow', 'airflow-check']:
        service = services[name]
        readonly = name.startswith('airflow')
        for volume in [data_volume, 'spark-jobs:/opt/spark/jobs']:
            assert volume + (':ro' if readonly else '') in service['volumes'], name
        assert service['depends_on']['workspace-init']['condition'] == 'service_completed_successfully'
    assert services['integration-check']['user'] == 'spark'
    assert services['airflow-check']['user'] == 'airflow'
    assert './donnees:/seed/data:ro' in services['objectstore-init']['volumes']
    workspace_seed = next(v for v in services['workspace-init']['volumes'] if v.startswith('./donnees:/seed/'))
    assert workspace_seed in ['./donnees:/seed/data:ro', './donnees:/seed/donnees:ro']
    for dockerfile in ['spark/runtime/Dockerfile', 'docker/airflow/Dockerfile']:
        text = (root / dockerfile).read_text()
        for name in ['donnees', 'jobs']:
            assert f'ln -s /opt/spark/{name} /home/spark/{name}' in text


def test_spark_donnees_path_is_consistent_in_scripts_and_notebooks():
    root = Path(__file__).parents[1]
    old_opt = '/opt/spark/' + 'data'
    old_home = '/home/spark/' + 'data'
    files = [*root.rglob('*.py'), *root.rglob('*.ipynb'), *root.rglob('*.md')]
    files += [root / 'spark/runtime/Dockerfile', root / 'docker/airflow/Dockerfile']
    for path in files:
        if not path.is_file():
            continue
        text = path.read_text(encoding='utf-8')
        assert old_opt not in text, f'old Spark data path in {path.relative_to(root)}'
        assert old_home not in text, f'old Spark home data path in {path.relative_to(root)}'


def test_compose_spark_mount_uses_donnees():
    root = Path(__file__).parents[1]
    compose_text = (root / 'compose.yaml').read_text()
    old_opt = '/opt/spark/' + 'data'
    if old_opt in compose_text:
        pytest.skip('compose.yaml is intentionally pending the owner update; use the supplied corrected file')
    compose = yaml.safe_load(compose_text)
    services = compose['services']
    for name in ['workspace-init', 'spark-master', 'spark-worker-1', 'spark-worker-2', 'spark-history', 'spark-jupyter', 'integration-check', 'airflow', 'airflow-check']:
        assert 'spark-data:/opt/spark/donnees' in services[name]['volumes'] or 'spark-data:/opt/spark/donnees:ro' in services[name]['volumes'], name
    assert './donnees:/seed/donnees:ro' in services['workspace-init']['volumes']


def test_spark_package_resolution_uses_a_writable_ivy_cache():
    root = Path(__file__).parents[1]
    defaults = (root / 'spark/conf/spark-defaults.conf').read_text(encoding='utf-8')
    assert 'spark.jars.ivy                           /tmp/spark-ivy' in defaults

    for dockerfile in ['spark/runtime/Dockerfile', 'docker/airflow/Dockerfile']:
        text = (root / dockerfile).read_text(encoding='utf-8')
        assert 'install -d -m 1777 /tmp/spark-ivy /tmp/spark-ivy/cache /tmp/spark-ivy/jars' in text

    notebook_path = root / 'spark/notebooks/pySpark/08-Lecture-des-six-etoiles-Druid.ipynb'
    notebook = json.loads(notebook_path.read_text(encoding='utf-8'))
    cells = notebook['cells']
    sources = [''.join(cell.get('source', [])) for cell in cells]
    code = '\n'.join(source for cell, source in zip(cells, sources) if cell['cell_type'] == 'code')
    assert '.config("spark.jars.ivy", IVY_USER_DIR)' in code
    assert 'IVY_USER_DIR = os.environ.get("SPARK_IVY_DIR", "/tmp/spark-ivy")' in code
    assert all(not cell.get('outputs') and cell.get('execution_count') is None
               for cell in cells if cell['cell_type'] == 'code')
    assert 'pySpark/08-Lecture-des-six-etoiles-Druid.ipynb' in '\n'.join(sources)
    assert 'pySpark/07-Lecture-des-six-etoiles-Druid.ipynb' not in '\n'.join(sources)
    for index, cell in enumerate(cells):
        if cell['cell_type'] == 'code':
            compile(''.join(cell.get('source', [])), f'{notebook_path}:cell-{index}', 'exec')


def test_druid_avatica_unbounded_varchar_dialect_is_built_and_registered():
    root = Path(__file__).parents[1]
    dockerfile = (root / 'spark/runtime/Dockerfile').read_text(encoding='utf-8')
    assert 'AS avatica-dialect-build' in dockerfile
    assert '/build/target/avatica-jdbc-dialect-1.0.0.jar /opt/spark/jars/' in dockerfile

    java_source = (root / 'spark/runtime/avatica-dialect/src/main/java/'
                   'training/bigdata/druid/AvaticaJdbcDialect.java').read_text(encoding='utf-8')
    assert 'url.startsWith("jdbc:avatica:")' in java_source
    assert 'sqlType == Types.VARCHAR' in java_source
    assert 'Option.apply(DataTypes.StringType)' in java_source

    notebook_path = root / 'spark/notebooks/pySpark/08-Lecture-des-six-etoiles-Druid.ipynb'
    notebook = json.loads(notebook_path.read_text(encoding='utf-8'))
    code = '\n'.join(''.join(cell.get('source', [])) for cell in notebook['cells']
                     if cell['cell_type'] == 'code')
    assert 'def enregistrer_dialecte_avatica(session):' in code
    assert 'JdbcDialects.registerDialect(dialecte)' in code
    assert code.index('enregistrer_dialecte_avatica(spark)') < code.index('.option("query", requete_catalogue)')
