
import json
import logging
import os
from typing import Dict, Tuple

FileTuple = Tuple[bytes, str, str]

def _dex_from_filename(path: str) -> str:
    base = os.path.basename(path)
    dex = base.split("_")[0]
    return dex.lstrip("0").zfill(4)

def build_spawn_dex_dict(spawn_files: Dict[str, FileTuple]) -> Dict[str, Tuple[str, bytes, str, str]]:
    out = {}
    for path, (blob, arch, leaf) in spawn_files.items():
        if path.endswith(".json"):
            out[_dex_from_filename(path)] = (path, blob, arch, leaf)
    logging.info(f"Built spawn Dex dict with {len(out)} entries.")
    return out

def build_species_dex_dict(species_files: Dict[str, FileTuple]) -> Dict[str, Tuple[str, dict, str, str]]:
    out = {}
    for path, (blob, arch, leaf) in species_files.items():
        if not path.endswith(".json"): continue
        try:
            data = json.loads(blob)
        except Exception as e:
            logging.error(f"Bad species JSON {path} in {arch}: {e}")
            continue
        dex = str(data.get("nationalPokedexNumber", "")).zfill(4)
        if dex and dex != "None":
            out[dex] = (path, data, leaf, arch)
    logging.info(f"Built species Dex dict with {len(out)} entries.")
    return out

def match_dex_numbers(spawn_dex: dict, species_dex: dict) -> Dict[str, tuple]:
    matched = {}
    keys = set(spawn_dex.keys()) | set(species_dex.keys())
    for dex in keys:
        s = spawn_dex.get(dex, (None, None, None, None))
        p = species_dex.get(dex, (None, None, None, None))
        matched[dex] = (
            s[0], s[1], s[2],       # spawn_path, spawn_bytes, spawn_archive
            p[0], p[1], p[2], p[3]  # species_path, species_data(json), species_dir_leaf, species_archive
        )
    return matched
