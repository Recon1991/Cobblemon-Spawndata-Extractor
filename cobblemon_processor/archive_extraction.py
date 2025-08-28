
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Tuple, Optional, Iterable
import os
import zipfile
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

FileTuple = Tuple[bytes, str, str]

@dataclass
class ExtractionResult:
    all_files: Dict[str, FileTuple]
    spawn_files: Dict[str, FileTuple]
    species_files: Dict[str, FileTuple]
    species_addition_files: Dict[str, FileTuple]

    def counts(self) -> dict:
        return {
            "all_files": len(self.all_files),
            "spawn_files": len(self.spawn_files),
            "species_files": len(self.species_files),
            "species_addition_files": len(self.species_addition_files),
        }

COBBLEMON_ROOT = "data/cobblemon"
INCLUDE_DIRS = (
    f"{COBBLEMON_ROOT}/spawn_pool_world",
    f"{COBBLEMON_ROOT}/species",
    f"{COBBLEMON_ROOT}/species_addition",
)

def _is_target_json(path: str) -> bool:
    return path.endswith(".json") and any(path.startswith(prefix) for prefix in INCLUDE_DIRS)

def _categorize_key(path: str) -> Optional[str]:
    if path.startswith(f"{COBBLEMON_ROOT}/spawn_pool_world"): return "spawn"
    if path.startswith(f"{COBBLEMON_ROOT}/species_addition"): return "species_addition"
    if path.startswith(f"{COBBLEMON_ROOT}/species"): return "species"
    return None

def _archive_iter(archives_dir: str):
    if not os.path.isdir(archives_dir):
        raise FileNotFoundError(f"Archives directory does not exist: {archives_dir}")
    for name in os.listdir(archives_dir):
        if name.lower().endswith((".zip", ".jar")):
            yield os.path.join(archives_dir, name)

def _extract_from_archive(archive_path: str, logger: Optional[logging.Logger] = None) -> Dict[str, FileTuple]:
    results: Dict[str, FileTuple] = {}
    archive_basename = os.path.basename(archive_path)
    try:
        with zipfile.ZipFile(archive_path, "r") as zf:
            for info in zf.infolist():
                path = info.filename
                if not _is_target_json(path):
                    continue
                try:
                    data = zf.read(info)
                    dir_leaf = os.path.dirname(path).split("/")[-1] if "/" in path else ""
                    results[path] = (data, archive_basename, dir_leaf)
                except Exception as e:
                    if logger: logger.error(f"Failed reading {path} from {archive_basename}: {e}")
    except zipfile.BadZipFile as e:
        if logger: logger.error(f"Bad archive {archive_basename}: {e}")
    except Exception as e:
        if logger: logger.error(f"Error opening {archive_basename}: {e}")
    if logger and results:
        logger.debug(f"Extracted {len(results)} target files from '{archive_basename}'.")
    return results

def extract_archives_in_memory(archives_dir: str, max_workers: int = 8, logger: Optional[logging.Logger] = None, fun_mode: bool = False) -> ExtractionResult:
    _logger = logger or logging.getLogger(__name__)
    if fun_mode and logger:
        _logger.info("~*~ FUN MODE: Spinning up archive scanners! whirr-click ~*~")
    all_files: Dict[str, FileTuple] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_extract_from_archive, ap, _logger): ap for ap in _archive_iter(archives_dir)}
        for fut in as_completed(futures):
            res = fut.result()
            all_files.update(res)
    spawn_files: Dict[str, FileTuple] = {}
    species_files: Dict[str, FileTuple] = {}
    species_addition_files: Dict[str, FileTuple] = {}
    for path, triple in all_files.items():
        cat = _categorize_key(path)
        if cat == "spawn": spawn_files[path] = triple
        elif cat == "species": species_files[path] = triple
        elif cat == "species_addition": species_addition_files[path] = triple
    if logger:
        counts = {"spawn": len(spawn_files), "species": len(species_files), "species_addition": len(species_addition_files)}
        _logger.info(f"Archive scan complete. Found {sum(counts.values())} target files ({counts['spawn']} spawn, {counts['species']} species, {counts['species_addition']} additions).")
        if fun_mode:
            _logger.info("~*~ FUN MODE: Archives dutifully raided! Tachikoma approves! ~*~")
    return ExtractionResult(all_files, spawn_files, species_files, species_addition_files)
