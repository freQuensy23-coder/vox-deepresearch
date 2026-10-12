import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import stat
import sys


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'duplicate JSON key: {key}')
        result[key] = value
    return result


def relative_path(value):
    if not isinstance(value, str) or not value or Path(value).is_absolute() or '..' in Path(value).parts:
        raise ValueError('expected a nonescaping relative path')
    return Path(value)


def open_scoped(root, relative, flags):
    parts = relative_path(str(relative)).parts
    if not parts:
        raise ValueError('file path required')
    parent = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            os.close(parent)
            parent = child
        return os.open(parts[-1], flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=parent)
    finally:
        os.close(parent)


def read_source(root, relative):
    path = relative_path(str(relative))
    with os.fdopen(open_scoped(root, path, os.O_RDONLY), 'rb') as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError('regular input file required')
        content = stream.read()
    digest = hashlib.sha256(content).hexdigest()
    source = {'path': path.as_posix(), 'sha256': digest, 'observed_at': timestamp(), 'source_id': f'{path.as_posix()}@sha256:{digest}'}
    return json.loads(content, object_pairs_hook=unique_object, parse_constant=lambda value: reject_constant(value)), source


def reject_constant(value):
    raise ValueError(f'invalid JSON constant: {value}')


def registry_state(data, source):
    rows = data.get('candidates') if isinstance(data, dict) else data
    if not isinstance(rows, list):
        raise ValueError('registry must be a candidate list or object with candidates list')
    ids, aliases, accepted_ids, accepted_aliases = set(), set(), set(), set()
    observations = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f'malformed candidate row {index}')
        identity, alias, decision = row.get('id'), row.get('alias'), row.get('decision')
        if identity is not None and (type(identity) is not int or identity == 0):
            raise ValueError(f'invalid numeric id at candidate row {index}')
        if alias is not None and (not isinstance(alias, str) or (identity is None and not alias.strip())):
            raise ValueError(f'invalid alias at candidate row {index}')
        if decision is not None and not isinstance(decision, str):
            raise ValueError(f'invalid decision at candidate row {index}')
        if identity is None and alias is None:
            raise ValueError(f'missing identity at candidate row {index}')
        if identity is not None:
            ids.add(identity)
            if decision == 'accepted':
                accepted_ids.add(identity)
        else:
            aliases.add(alias)
            if decision == 'accepted':
                accepted_aliases.add(alias)
        pointer = f'/candidates/{index}' if isinstance(data, dict) else f'/{index}'
        observations.append({'source_id': source['source_id'], 'pointer': pointer, 'id': identity, 'alias': alias, 'decision_claimed': decision})
    return {**source, 'candidate_rows': len(rows), 'numeric_ids': sorted(ids), 'numeric_id_count': len(ids), 'unresolved_aliases': sorted(aliases), 'unresolved_alias_count': len(aliases), 'accepted_claimed_numeric_ids': sorted(accepted_ids), 'accepted_claimed_numeric_id_count': len(accepted_ids), 'accepted_claimed_unresolved_aliases': sorted(accepted_aliases), 'evidence_verification': 'not_performed', 'rows': observations}


def snapshot(root, registry):
    root = Path(root).absolute()
    if '..' in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
        raise ValueError('root must not contain symlinks or parent traversal')
    if not root.is_dir():
        raise ValueError('root must be an existing directory')
    registry = relative_path(registry)
    data, source = read_source(root, registry)
    result = {'schema_version': 1, 'root': str(root), 'observed_at': timestamp(), 'consistency': 'per_file_observations_not_atomic', 'registry': registry_state(data, source), 'collectors': [], 'projections': [], 'skipped_symlinks': []}
    for directory, dirs, files in os.walk(root, followlinks=False, onerror=lambda error: fail_walk(error)):
        for name in sorted(dirs + files):
            path = Path(directory) / name
            if path.is_symlink():
                result['skipped_symlinks'].append(path.relative_to(root).as_posix())
        dirs[:] = sorted(name for name in dirs if not (Path(directory) / name).is_symlink())
        if 'manifest.json' not in files or (Path(directory) / 'manifest.json').is_symlink():
            continue
        manifest, receipt = read_source(root, (Path(directory) / 'manifest.json').relative_to(root))
        if not isinstance(manifest, dict) or not isinstance(manifest.get('status'), str) or not manifest['status'] or not isinstance(manifest.get('parameters'), dict) or 'remaining_window' not in manifest or (manifest['remaining_window'] is not None and not isinstance(manifest['remaining_window'], dict)):
            raise ValueError(f'malformed collector manifest: {receipt["path"]}')
        result['collectors'].append({**receipt, 'status': manifest['status'], 'parameters': manifest['parameters'], 'remaining_window': manifest['remaining_window'], 'reason': manifest.get('reason'), 'relationship': 'independent_observation'})
    projections = data.get('projections', []) if isinstance(data, dict) else []
    if not isinstance(projections, list):
        raise ValueError('projections must be a list of relative paths')
    for path in projections:
        projection, receipt = read_source(root, relative_path(path))
        if not isinstance(projection, dict):
            raise ValueError('projection must be an object')
        issues = []
        declared_path, declared_hash = projection.get('source_path'), projection.get('source_sha256')
        if declared_path is not None:
            relative_path(declared_path)
            if declared_path != source['path']:
                issues.append('source_path_mismatch')
        if declared_hash is not None:
            if not isinstance(declared_hash, str) or len(declared_hash) != 64 or any(c not in '0123456789abcdef' for c in declared_hash):
                raise ValueError('invalid projection source_sha256')
            if declared_hash != source['sha256']:
                issues.append('source_hash_mismatch')
        result['projections'].append({**receipt, 'source_path_claimed': declared_path, 'source_sha256_claimed': declared_hash, 'issues': issues, 'binding_status': 'mismatch' if issues else 'matches' if declared_path is not None and declared_hash is not None else 'unverified'})
    result['collectors'].sort(key=lambda item: item['path'])
    result['skipped_symlinks'].sort()
    return result


def fail_walk(error):
    raise error


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    parser.add_argument('--registry', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    try:
        root = Path(args.root).absolute()
        output = Path(args.output)
        relative = output.relative_to(root) if output.is_absolute() else output
        relative_path(str(relative))
        result = snapshot(root, args.registry)
        payload = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + '\n'
        with os.fdopen(open_scoped(root, relative, os.O_WRONLY | os.O_CREAT | os.O_EXCL), 'w', encoding='utf-8') as stream:
            stream.write(payload)
        return 0
    except (OSError, ValueError, TypeError) as error:
        print(f'snapshot error: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
