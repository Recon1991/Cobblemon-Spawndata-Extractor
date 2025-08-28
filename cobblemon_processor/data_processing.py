
import json
import logging
import os
from typing import Dict, Any, Optional
try:
    from utils import format_location_names, get_weather_condition, get_sky_condition, get_moon_phase_name
except Exception:
    # fallback if running as a module and utils is adjacent to package
    import sys, pathlib
    sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent))
    from .utils import format_location_names, get_weather_condition, get_sky_condition, get_moon_phase_name

def get_species_data(pokemon_name, species_data):
    return next(
        (form for form in species_data.get("forms", []) if form.get("name","").lower() in pokemon_name.lower()),
        species_data
    )

def extract_species_info(species_data: dict):
    name = species_data.get("name", "Unknown")
    primary_type = species_data.get("primaryType", "")
    secondary_type = species_data.get("secondaryType", "")
    egg_groups = ', '.join(species_data.get("eggGroups", []))
    labels_all = species_data.get("labels", [])
    gen_label = next((lbl for lbl in labels_all if str(lbl).lower().startswith("gen")), None)
    generation = gen_label.replace("gen","Gen ").capitalize() if gen_label else ""
    meaningful = [str(lbl).replace("_"," ").title() for lbl in labels_all if not str(lbl).lower().startswith("gen")]
    labels = ", ".join(meaningful) if meaningful else ""
    return name, primary_type, secondary_type, egg_groups, generation, labels

def build_merged_entry(dex_number: str, species_data: dict, entry: dict, spawn_file: str, species_file: Optional[str],
                       original_spawn_archive: Optional[str], original_species_archive: Optional[str],
                       generation: str, species_directory: Optional[str]) -> Optional[Dict[str, Any]]:
    pokemon_name = entry.get("pokemon","").strip()
    if not pokemon_name: return None
    pokemon_species = get_species_data(pokemon_name, species_data or {})
    primary_type = pokemon_species.get("primaryType","").title()
    secondary_type = pokemon_species.get("secondaryType","-----").title()
    egg_groups = ", ".join(pokemon_species.get("eggGroups", [])).title()
    meaningful_labels = [str(lbl).replace("_"," ").title() for lbl in (species_data or {}).get("labels", []) if not str(lbl).lower().startswith("gen")]
    labels = ", ".join(meaningful_labels) if meaningful_labels else ""
    time_range = entry.get("condition", {}).get("timeRange", "Any").title()
    sky_condition = get_sky_condition(entry)
    moon_phase = entry.get("condition", {}).get("moonPhase", [])
    anti_moon_phase = entry.get("anticondition", {}).get("moonPhase", [])
    species_archive = f"{species_directory}/{os.path.basename(species_file)}" if species_directory and species_file else "Unknown"
    original_spawn_archive = os.path.basename(original_spawn_archive) if original_spawn_archive else "Unknown"
    original_species_archive = os.path.basename(original_species_archive) if original_species_archive else "Unknown"
    return {
        "Dex Number": f"#{str(dex_number).zfill(4)}",
        "Pokemon Name": pokemon_name.title(),
        "Primary Type": primary_type,
        "Secondary Type": secondary_type,
        "Rarity": entry.get("bucket","").title(),
        "Egg Groups": egg_groups,
        "Generation": generation,
        "Labels": labels,
        "Time": time_range,
        "Weather": get_weather_condition(entry.get("condition", {})),
        "Sky": sky_condition,
        "Presets": ", ".join(entry.get("presets", [])).title() or "",
        "Biomes": ", ".join(format_location_names(entry.get("condition", {}).get("biomes", []))).strip(),
        "Anti-Biomes": ", ".join(format_location_names(entry.get("anticondition", {}).get("biomes", []))).strip(),
        "Structures": ", ".join(format_location_names(entry.get("condition", {}).get("structures", []))).strip(),
        "Anti-Structures": ", ".join(format_location_names(entry.get("anticondition", {}).get("structures", []))).strip(),
        "Moon Phase": get_moon_phase_name(moon_phase),
        "Anti-Moon Phase": get_moon_phase_name(anti_moon_phase),
        "Base Blocks": ", ".join(format_location_names(entry.get("condition", {}).get("neededBaseBlocks", []))),
        "Nearby Blocks": ", ".join(format_location_names(entry.get("condition", {}).get("neededNearbyBlocks", []))),
        "Weight": entry.get("weight",""),
        "Context": entry.get("context","").title(),
        "Spawn ID": entry.get("id","Unknown"),
        "Species Archive": species_archive,
        "Original Spawn Archive": original_spawn_archive,
        "Original Species Archive": original_species_archive,
    }

async def process_entry(dex_number: str, matched_dex_dict: dict):
    spawn_file, spawn_blob, original_spawn_archive, species_file, species_json, species_dir, original_species_archive = matched_dex_dict[dex_number]
    if not species_json:
        pokemon_name = primary_type = secondary_type = egg_groups = generation = labels = ""
    else:
        pokemon_name, primary_type, secondary_type, egg_groups, generation, labels = extract_species_info(species_json)

    if not spawn_file or not spawn_blob:
        skipped = {
            "Dex Number": dex_number,
            "Pokemon Name": pokemon_name,
            "Primary Type": primary_type,
            "Secondary Type": secondary_type,
            "Egg Groups": egg_groups,
            "Generation": generation,
            "Labels": labels,
            "Species Archive": original_species_archive
        }
        logging.info(f"Skipping Dex {dex_number} ({pokemon_name}) - No spawn data.")
        return None, skipped

    try:
        spawn_data = json.loads(spawn_blob) or {"spawns": []}
    except Exception as e:
        logging.error(f"Bad spawn JSON for {dex_number}: {e}")
        return None, None

    merged = []
    for entry in spawn_data.get("spawns", []):
        row = build_merged_entry(dex_number, species_json, entry, spawn_file, species_file, original_spawn_archive, original_species_archive, generation, species_dir)
        if row:
            merged.append(row)
    return merged, None
