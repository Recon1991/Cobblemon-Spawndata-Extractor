
import json, logging, os, re
from typing import Dict, Tuple, List
from .data_processing import build_merged_entry

def _name_from_spawn_filename(spawn_path: str) -> str:
    base = os.path.splitext(os.path.basename(spawn_path))[0]
    # strip leading numeric dex prefixes like '0001_' or '0001-'
    base = re.sub(r"^\d+[_-]*", "", base)
    return base.lower().strip()

def parse_species_additions(species_addition_files: Dict[str, Tuple[bytes, str, str]]) -> Dict[str, dict]:
    out = {}
    for path, (blob, arch, leaf) in species_addition_files.items():
        try:
            data = json.loads(blob)
        except Exception as e:
            logging.error(f"Bad addition JSON {path} in {arch}: {e}")
            continue
        target_id = str(data.get("target_id", "") or data.get("targetId",""))
        name_from_id = target_id.split(":")[-1] if ":" in target_id else target_id
        normalized = (name_from_id or str(data.get("name",""))).strip().lower()
        if not normalized:
            logging.warning(f"Addition file {path} missing target_id/name; skipping.")
            continue
        data.setdefault("forms", [])
        data.setdefault("labels", [])
        out[normalized] = data
    logging.info(f"Parsed {len(out)} species_addition records.")
    return out

def merge_additions_against_spawns(spawn_files: Dict[str, Tuple[bytes,str,str]], additions_by_name: Dict[str, dict]) -> List[dict]:
    rows: List[dict] = []
    for spawn_path, (blob, spawn_arch, spawn_leaf) in spawn_files.items():
        try:
            spjson = json.loads(blob)
        except Exception as e:
            logging.error(f"Bad spawn JSON {spawn_path}: {e}")
            continue
        for entry in spjson.get("spawns", []):
            pname = str(entry.get("pokemon","")).strip().lower() or _name_from_spawn_filename(spawn_path)
            if not pname:
                continue
            spec = additions_by_name.get(pname)
            if not spec:
                continue
            row = build_merged_entry("0000", spec, entry, spawn_path, None, spawn_arch, None, generation="", species_directory=None)
            if row:
                row["Dex Number"] = "#----"
                rows.append(row)
    logging.info(f"Merged {len(rows)} species_addition rows by name (with filename fallback).")
    return rows
