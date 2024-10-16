import os
import json
import csv
import logging
import zipfile
import time
import cProfile
import pstats
import io
from functools import lru_cache
from queue import Queue
from logging.handlers import QueueHandler, QueueListener
from concurrent.futures import ThreadPoolExecutor, as_completed
from column_names import column_names  # Import column names

# Constants
ZIP_ARCHIVES_DIR = 'zip_archives'
CSV_FILENAME = 'cobblemon_dexkey_merged_data_v1.csv'
MAX_WORKERS = 8  # Number of parallel workers

# Configure async logging
log_queue = Queue()
queue_handler = QueueHandler(log_queue)
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(queue_handler)
file_handler = logging.FileHandler("process_log.txt")
console_handler = logging.StreamHandler()

listener = QueueListener(log_queue, file_handler, console_handler)
listener.start()

def stop_listener():
    """Stop the logging listener."""
    listener.stop()

def extract_dex_number_from_filename(filename):
    """Extract and format the Dex number from the filename."""
    base_name = os.path.basename(filename)
    dex_number = base_name.split('_')[0]
    return dex_number.lstrip('0')

def format_location_names(locations):
    """Format biome/structure names."""
    return [location.split(':')[-1].replace("is", "").replace('_', ' ').strip().title() for location in locations]

def get_weather_condition(condition):
    """Determine the weather condition."""
    if condition.get("isThundering"):
        return "Thunder"
    if condition.get("isRaining"):
        return "Rain"
    return "Clear" if condition.get("isRaining") is False else "Any"

def get_sky_condition(condition):
    """Determine sky visibility."""
    see_sky = condition.get("canSeeSky")
    return "MUST SEE" if see_sky else "CANNOT SEE" if see_sky is False else "Any"

@lru_cache(maxsize=None)
def extract_json_data_cached(archive_name, target_path):
    """Extract JSON data with caching."""
    archive_path = os.path.join(ZIP_ARCHIVES_DIR, archive_name)
    with zipfile.ZipFile(archive_path, 'r') as zip_file:
        with zip_file.open(target_path) as f:
            try:
                return json.load(f)
            except json.JSONDecodeError as e:
                logging.error(f"Error reading {target_path} in {archive_name}: {e}")
                return None

def build_spawn_dex_dict():
    """Build spawn Dex dictionary."""
    spawn_dex_dict = {}
    logging.info("Building spawn Dex dictionary...")

    for archive_name in os.listdir(ZIP_ARCHIVES_DIR):
        if archive_name.endswith(('.zip', '.jar')):
            with zipfile.ZipFile(os.path.join(ZIP_ARCHIVES_DIR, archive_name), 'r') as zip_file:
                for file_info in zip_file.namelist():
                    if 'data/cobblemon/spawn_pool_world/' in file_info and file_info.endswith('.json'):
                        dex_number = extract_dex_number_from_filename(file_info)
                        spawn_dex_dict[dex_number] = (archive_name, file_info)

    logging.info(f"Built spawn Dex dict with {len(spawn_dex_dict)} entries.")
    return spawn_dex_dict

def build_species_dex_dict():
    """Build species Dex dictionary."""
    species_dex_dict = {}
    logging.info("Building species Dex dictionary...")

    for archive_name in os.listdir(ZIP_ARCHIVES_DIR):
        if archive_name.endswith(('.zip', '.jar')):
            with zipfile.ZipFile(os.path.join(ZIP_ARCHIVES_DIR, archive_name), 'r') as zip_file:
                for file_info in zip_file.infolist():
                    if 'data/cobblemon/species/' in file_info.filename and file_info.filename.endswith('.json'):
                        data = extract_json_data_cached(archive_name, file_info.filename)
                        if data:
                            dex_number = str(data.get("nationalPokedexNumber"))
                            species_dex_dict[dex_number] = (archive_name, file_info.filename)

    logging.info(f"Built species Dex dict with {len(species_dex_dict)} entries.")
    return species_dex_dict

def get_species_data(pokemon_name, species_data):
    """Extract form-specific or base species data."""
    if "forms" in species_data:
        for form in species_data["forms"]:
            if form["name"].lower() in pokemon_name.lower():
                return form  # Match form-specific data

    return species_data  # Default to base data

def process_entry(dex_number, matched_dex_dict):
    """Process and merge data for a single Dex entry."""
    spawn_archive, spawn_file, species_archive, species_file = matched_dex_dict[dex_number]

    try:
        species_data = extract_json_data_cached(species_archive, species_file)
        if not species_data:
            logging.warning(f"No species data for Dex {dex_number}.")
            return None

        spawn_data = extract_json_data_cached(spawn_archive, spawn_file) or {"spawns": []}
        merged_entries = []

        for entry in spawn_data["spawns"]:
            pokemon_name = entry.get("pokemon", "").strip()
            if not pokemon_name:
                continue

            pokemon_species_data = get_species_data(pokemon_name, species_data)
            primary_type = pokemon_species_data.get("primaryType", "")
            secondary_type = pokemon_species_data.get("secondaryType", "")
            egg_groups = ', '.join(pokemon_species_data.get("eggGroups", []))
            source_files = f"{spawn_archive}, {species_archive}"

            merged_entries.append({
                "Dex Number": dex_number,
                "Pokemon Name": pokemon_name.title(),
                "Primary Type": primary_type,
                "Secondary Type": secondary_type,
                "Egg Groups": egg_groups,
                "Spawn ID": entry.get("id", "Unknown"),
                "Biomes": ', '.join(format_location_names(entry.get("condition", {}).get("biomes", []))).strip(),
                "Anti-Biomes": ', '.join(format_location_names(entry.get("anticondition", {}).get("biomes", []))).strip(),
                "Structures": ', '.join(format_location_names(entry.get("condition", {}).get("structures", []))).strip(),
                "Time": entry.get("time", "Any"),
                "Weather": get_weather_condition(entry.get("condition", {})),
                "Sky": get_sky_condition(entry.get("condition", {})),
                "Source File": source_files
            })

        return merged_entries

    except Exception as e:
        logging.error(f"Error processing Dex {dex_number}: {e}")
        return None

def main():
    """Main function to extract and merge Pokémon data."""
    spawn_dex = build_spawn_dex_dict()
    species_dex = build_species_dex_dict()
    matched_dex_dict = match_dex_numbers(spawn_dex, species_dex)

    with open(CSV_FILENAME, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=column_names)
        writer.writeheader()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_entry, dex, matched_dex_dict): dex for dex in matched_dex_dict}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    for entry in result:
                        writer.writerow(sort_row(entry))

    stop_listener()

if __name__ == "__main__":
    main()
