from pathlib import Path
import ast
import importlib.util
import os
import yaml

ROOT = Path(__file__).parents[1]
REMOVED = {'timescaledb','timescale-load','timescale-check','timescale-reports-init','logstash','elasticsearch','kibana'}


def test_remaining_services_and_donnees_mounts():
    compose = yaml.safe_load((ROOT/'compose.yaml').read_text())
    services = compose['services']
    assert not REMOVED.intersection(services)
    assert not (ROOT/'data').exists() and (ROOT/'donnees/input/sales.csv').is_file()
    for name, definition in services.items():
        assert set(definition.get('depends_on', {})) <= services.keys(), name
        for volume in definition.get('volumes', []):
            source = volume.split(':')[0]
            assert not source.startswith('./data')
            if source.startswith('./'):
                assert (ROOT/source).exists(), (name, source)
    for name in ['workspace-init','objectstore-init','file-producer','web-log-producer','mock-opendata']:
        assert any(v.startswith('./donnees:') for v in services[name]['volumes']), name
    assert 'donnees' in (ROOT/'.dockerignore').read_text().splitlines()


def test_portal_pgadmin_and_checks_match_remaining_stack(monkeypatch):
    monkeypatch.setenv('POSTGRES_DB','formation')
    monkeypatch.setenv('POSTGRES_USER','formation')
    monkeypatch.setenv('POSTGRES_PASSWORD','formation')
    for filename in ['dashboard/server.py','docker/bootstrap/pgadmin.py']:
        spec=importlib.util.spec_from_file_location('contract',ROOT/filename)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        if hasattr(module,'servers'):
            assert [s['Host'] for s in module.servers()['Servers'].values()] == ['postgres-source']
        else:
            html=module.render().decode()
            assert 'Trino' in html and 'Superset' in html
            assert not any(term in html.lower() for term in REMOVED)
    source=(ROOT/'checks/integration.py').read_text()
    ast.parse(source)
    assert not any(term in source.lower() for term in REMOVED)
    for function in ['verify_trino','verify_druid','verify_superset','kafka_roundtrip','s3_roundtrip']:
        assert 'def '+function+'(' in source


def test_trino_http_200_during_startup_is_not_ready():
    from types import SimpleNamespace
    import pytest
    tree=ast.parse((ROOT/'checks/integration.py').read_text())
    function=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='http')
    state={'starting':True}
    namespace={'request':lambda *args:SimpleNamespace(status_code=200,json=lambda:state)}
    exec(compile(ast.Module(body=[function],type_ignores=[]),'<readiness>','exec'),namespace)
    with pytest.raises(RuntimeError,match='still starting'):
        namespace['http']('http://trino:8080/v1/info')
    state['starting']=False
    assert namespace['http']('http://trino:8080/v1/info')==200
    health=yaml.safe_load((ROOT/'compose.yaml').read_text())['services']['trino']['healthcheck']['test'][-1]
    assert 'starting' in health and 'false' in health


def test_full_wait_has_no_unconsumed_one_shot_producers():
    services=yaml.safe_load((ROOT/'compose.yaml').read_text())['services']
    active={name:definition for name,definition in services.items()
            if not definition.get('profiles') or 'full' in definition['profiles']}
    completed={dependency for definition in active.values()
               for dependency,condition in definition.get('depends_on',{}).items()
               if condition['condition']=='service_completed_successfully'}
    for name,definition in active.items():
        if definition.get('restart')=='no':
            assert name in completed, f'{name} would exit during up --wait without a completion dependency'
    for name in ['file-producer','postgres-producer','web-log-producer']:
        assert name not in active
        assert 'full' not in services[name]['profiles']
    source=(ROOT/'checks/integration.py').read_text()
    assert "['file-producer','postgres-producer','web-log-producer']" in source


def test_fresh_machine_setup_is_documented():
    requirements = (ROOT/'requirements-checks.txt').read_text().splitlines()
    assert requirements == ['pytest==9.1.1', 'PyYAML==6.0.3', 'boto3==1.43.114']
    readme = (ROOT/'README.md').read_text()
    assert 'git clone https://github.com/rbizoi/FormationBigDataBI.git' in readme
    assert 'docker context show' in readme and 'docker compose version' in readme
    assert 'profile full --profile checks run --rm ebrasil-check' in readme
    workflow = (ROOT/'.github/workflows/contracts.yml').read_text()
    assert 'pip install -r requirements-checks.txt' in workflow
