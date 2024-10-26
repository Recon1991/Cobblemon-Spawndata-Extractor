import os
import shutil
import zipfile
import json
import csv
import logging
import time
import cProfile
import pstats
import io
from functools import lru_cache
from queue import Queue
from logging.handlers import QueueHandler, QueueListener
from concurrent.futures import ThreadPoolExecutor, as_completed
from column_names import column_names, skipped_entries_column_names

# Load configuration from config.json
try:
    with open('config.json', 'r') as f:
        config = json.load(f)
except (FileNotFoundError, json.JSONDecodeError) as e:
    print(f"Error loading config: {e}")
    logging.error(f"Error loading config: {e}")
    exit(1) # Exit program if the config can't be loaded

# Get the output filename from the config
output_filename = config.get("output_filename", "default_output")
if not output_filename.endswith('.csv'):
    output_filename += '.csv'

# Get the skipped entries filename from the config
skipped_entries_filename = config.get("skipped_entries_filename", "skipped_entries")
if not skipped_entries_filename.endswith('.csv'):
    skipped_entries_filename += '.csv'  

# Constants
ARCHIVES_DIR = config["ARCHIVES_DIR"]
EXTRACTED_DIR = "./data/extracted"  # Directory for extracted files
CSV_FILENAME = output_filename
SKIPPED_ENTRIES_FILENAME = skipped_entries_filename
MAX_WORKERS = config["MAX_WORKERS"]

# Configure async logging
log_queue = Queue()
queue_handler = QueueHandler(log_queue)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Load log filename and level from config
log_filename = config.get("LOG_FILENAME", "process_log.txt")
log_level = getattr(logging, config.get("LOG_LEVEL", "INFO").upper(), logging.INFO)

# Add a timestamp to log messages
log_format = config.get("LOG_FORMAT", "%(asctime)s - %(levelname)s - %(message)s")
formatter = logging.Formatter(log_format)

# Initialize file and console handlers
file_handler = logging.FileHandler(log_filename)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Avoid adding duplicate handlers
if not any(isinstance(h, QueueHandler) for h in root_logger.handlers):
    root_logger.addHandler(queue_handler)

if not any(isinstance(h, logging.FileHandler) for h in root_logger.handlers):
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

if not any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers):
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

# Start the logging listener
listener = QueueListener(log_queue, file_handler, console_handler)
listener.start()

def stop_listener():
    """Stop the logging listener."""
    listener.stop()

def extract_archives(archives_dir, extracted_dir, overwrite=True):
    """
    Extracts specific directories from zip/jar files in the archives directory to the extracted directory.
    Only extracts 'data/cobblemon/spawn_pool_world' and 'data/cobblemon/species'.
    """
    # Remove existing extracted directory if needed
    if overwrite and os.path.exists(extracted_dir):
        shutil.rmtree(extracted_dir)
    
    os.makedirs(extracted_dir, exist_ok=True)
    
    # Iterate over the archives in the directory and extract specific directories
    for archive_name in os.listdir(archives_dir):
        if archive_name.endswith(('.zip', '.jar')):
            archive_path = os.path.join(archives_dir, archive_name)
            with zipfile.ZipFile(archive_path, 'r') as zip_file:
                for file_info in zip_file.infolist():
                    if file_info.filename.startswith(('data/cobblemon/spawn_pool_world', 'data/cobblemon/species')):
                        zip_file.extract(file_info, extracted_dir)
                print(f"Extracted relevant directories from '{archive_name}' to '{extracted_dir}'")

def extract_dex_number_from_filename(filename):
    """Extract and format the Dex number from the filename."""
    base_name = os.path.basename(filename)
    dex_number = base_name.split('_')[0]
    return dex_number.lstrip('0')

def format_location_names(locations):
    """Format biome/structure names."""
    formatted_locations = []
    for location in locations:
        name = location.split(':')[-1]
        # Only remove the prefix 'is_' if it is at the beginning of the name
        if name.startswith("is_"):
            name = name[3:]  # Remove the 'is_' prefix
        formatted_name = name.replace('_', ' ').strip().title()
        formatted_locations.append(formatted_name)
    return formatted_locations


def get_weather_condition(condition):
    """Determine the weather condition."""
    if condition.get("isThundering"):
        return "Thunder"
    if condition.get("isRaining"):
        return "Rain"
    return "Clear" if condition.get("isRaining") is False else "Any"

def get_sky_condition(spawn):
    """Determine the sky visibility condition."""
    can_see_sky = spawn.get('canSeeSky', spawn.get('condition', {}).get('canSeeSky', None))

    if can_see_sky is True:
        return "MUST SEE"
    elif can_see_sky is False:
        return "CANNOT SEE"
    
    min_sky_light = spawn.get('condition', {}).get('minSkyLight', 'N/A')
    max_sky_light = spawn.get('condition', {}).get('maxSkyLight', 'N/A')

    if 'minSkyLight' in spawn.get('condition', {}) or 'maxSkyLight' in spawn.get('condition', {}):
        return f"{min_sky_light} - {max_sky_light}"

    return "Any"

def build_spawn_dex_dict(extracted_dir):
    """Build spawn Dex dictionary from extracted files."""
    spawn_dex_dict = {}
    logging.info("Building spawn Dex dictionary...")

    for root, _, files in os.walk(os.path.join(extracted_dir, 'data/cobblemon/spawn_pool_world')):
        for file_name in files:
            if file_name.endswith('.json'):
                dex_number = extract_dex_number_from_filename(file_name)
                spawn_dex_dict[dex_number] = (root, file_name)

    logging.info(f"Built spawn Dex dict with {len(spawn_dex_dict)} entries.")
    return spawn_dex_dict

def build_species_dex_dict(extracted_dir):
    """Build species Dex dictionary from extracted files."""
    species_dex_dict = {}
    logging.info("Building species Dex dictionary...")

    for root, _, files in os.walk(os.path.join(extracted_dir, 'data/cobblemon/species')):
        for file_name in files:
            if file_name.endswith('.json'):
                file_path = os.path.join(root, file_name)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        dex_number = str(data.get("nationalPokedexNumber"))
                        species_dex_dict[dex_number] = (root, file_name)
                except json.JSONDecodeError as e:
                    logging.error(f"Error reading {file_name} in {root}: {e}")

    logging.info(f"Built species Dex dict with {len(species_dex_dict)} entries.")
    return species_dex_dict

def match_dex_numbers(spawn_dex, species_dex):
    """
    Match Dex numbers from spawn and species dictionaries and prepare them for processing.
    Returns a dictionary with Dex numbers as keys and tuples containing (spawn archive, 
    spawn file, species archive, species file).
    """
    matched_dex = {}

    all_dex_numbers = set(spawn_dex.keys()).union(set(species_dex.keys()))

    for dex_number in all_dex_numbers:
        spawn_info = spawn_dex.get(dex_number, (None, None))
        species_info = species_dex.get(dex_number, (None, None))

        matched_dex[dex_number] = (
            spawn_info[0],  # Spawn archive name
            spawn_info[1],  # Spawn file name
            species_info[0],  # Species archive name
            species_info[1]  # Species file name
        )

    return matched_dex

def get_species_data(pokemon_name, species_data):
    return next(
        (form for form in species_data.get("forms", []) if form["name"].lower() in pokemon_name.lower()),
        species_data  # Default to base data
    )

def process_entry(dex_number, matched_dex_dict):
    """Process and merge data for a single Dex entry."""
    spawn_archive, spawn_file, species_archive, species_file = matched_dex_dict[dex_number]
    
    pokemon_name = ""
    primaryType = ""
    secondaryType = ""
    egg_groups = ""
    generation = ""
    labels = ""

    # Extract species data if available
    if species_archive and species_file:
        species_data = extract_json_data_cached(os.path.join(species_archive, species_file))
        if species_data:
            # Basic species information
            pokemon_name = species_data.get("name", "Unknown")
            primaryType = species_data.get("primaryType", "")
            secondaryType = species_data.get("secondaryType", "")
            egg_groups = ', '.join(species_data.get("eggGroups", []))

            # Extract labels and generation
            all_labels = species_data.get("labels", [])
            generation_label = next((label for label in all_labels if label.startswith("gen")), None)
            generation = generation_label[:3].capitalize() + " " + generation_label[3:] if generation_label else ""

            meaningful_labels = [
                label.strip().replace('_', ' ').title() 
                for label in all_labels
                if not label.lower().startswith("gen")
            ]
            labels = ', '.join(meaningful_labels) if meaningful_labels else ""

    if not spawn_archive or not spawn_file:
        skipped_entry = {
            "Dex Number": dex_number,
            "Pokemon Name": pokemon_name,
            "Primary Type": primaryType,
            "Secondary Type": secondaryType,
            "Egg Groups": egg_groups,
            "Generation": generation,
            "Labels": labels,
            "Species Archive": species_archive 
        }
        logging.info(f"Skipping Dex {dex_number} ({pokemon_name}) - No spawn data.")
        return None, skipped_entry

    try:
        spawn_data = extract_json_data_cached(os.path.join(spawn_archive, spawn_file)) or {"spawns": []}
        merged_entries = []

        for entry in spawn_data["spawns"]:
            pokemon_name = entry.get("pokemon", "").strip()
            if not pokemon_name:
                continue

            pokemon_species_data = get_species_data(pokemon_name, species_data)

            primary_type = pokemon_species_data.get("primaryType", "").title()
            secondary_type = pokemon_species_data.get("secondaryType", "-----").title()
            egg_groups = ', '.join(pokemon_species_data.get("eggGroups", [])).title()
            meaningful_labels = [
                label.strip().replace('_', ' ').title() 
                for label in all_labels
                if not label.lower().startswith("gen")
            ]
            labels = ', '.join(meaningful_labels) if meaningful_labels else ""
            time_range = entry.get("condition", {}).get("timeRange", "Any").title()
            sky_condition = get_sky_condition(entry)

            merged_entries.append({
                "Dex Number": dex_number,
                "Pokemon Name": pokemon_name.title(),
                "Primary Type": primary_type,
                "Secondary Type": secondary_type,
                "Rarity": entry.get("bucket", "").title(),
                "Egg Groups": egg_groups,
                "Generation": generation, 
                "Labels": labels,  
                "Time": time_range,
                "Weather": get_weather_condition(entry.get("condition", {})),
                "Sky": sky_condition,
                "Presets": ', '.join(entry.get("presets", [])).title() or "",
                "Biomes": ', '.join(format_location_names(entry.get("condition", {}).get("biomes", []))).strip(),
                "Anti-Biomes": ', '.join(format_location_names(entry.get("anticondition", {}).get("biomes", []))).strip(),
                "Structures": ', '.join(format_location_names(entry.get("condition", {}).get("structures", []))).strip(),
                "Anti-Structures": ', '.join(format_location_names(entry.get("anticondition", {}).get("structures", []))).strip(),
                "Moon Phase": entry.get("condition", {}).get("moonPhase", ""),
                "Anti-Moon Phase": entry.get("anticondition", {}).get("moonPhase", ""),
                "Base Blocks": ', '.join(format_location_names(entry.get("condition", {}).get("neededBaseBlocks", []))),
                "Nearby Blocks": ', '.join(format_location_names(entry.get("condition", {}).get("neededNearbyBlocks", []))),
                "Weight": entry.get("weight", ""),
                "Context": entry.get("context", "").title(),
                "Spawn ID": entry.get("id", "Unknown"),
                "Spawn Archive": spawn_archive,
                "Species Archive": species_archive
            })

        return merged_entries, None

    except Exception as e:
        logging.error(f"Error processing Dex {dex_number}: {e}")
        return None, None

@lru_cache(maxsize=None)
def extract_json_data_cached(file_path):
    """Extract JSON data with caching."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logging.error(f"Error reading {file_path}: {e}")
        return None

def main():
    """Main function to extract and merge Pokémon data."""
    # Step 1: Extract archives if needed
    extract_archives(ARCHIVES_DIR, EXTRACTED_DIR)

    # Step 2: Build Dex dictionaries
    spawn_dex = build_spawn_dex_dict(EXTRACTED_DIR)
    species_dex = build_species_dex_dict(EXTRACTED_DIR)
    matched_dex_dict = match_dex_numbers(spawn_dex, species_dex)

    all_rows = []  # Store valid entries
    skipped_entries = []  # Store skipped entries

    # Process entries in parallel and collect rows
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_entry, dex, matched_dex_dict): dex for dex in matched_dex_dict}
        for future in as_completed(futures):
            result, skipped = future.result()
            if result:
                all_rows.extend(result)  # Collect valid rows
            else:
                skipped_entries.append(skipped)  # Collect skipped rows

    # Sort the valid rows using primary and secondary keys
    sort_key = config.get('primary_sorting_key', "Pokemon Name")  # Fallback to default
    secondary_sort_key = config.get('secondary_sorting_key', None)  # Optional

    sorted_rows = sorted(
        all_rows,
        key=lambda entry: (
            entry.get(sort_key, "").lower(),
            entry.get(secondary_sort_key, "").lower() if secondary_sort_key else ""
        )
    )

    # Write sorted valid rows to the main CSV
    with open(CSV_FILENAME, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=column_names)
        writer.writeheader()
        writer.writerows(sorted_rows)

    # Sort the skipped entries the same way
    sorted_skipped_entries = sorted(
        skipped_entries,
        key=lambda entry: (
            entry.get(sort_key, "").lower(),
            entry.get(secondary_sort_key, "").lower() if secondary_sort_key else ""
        )
    )

    # Write sorted skipped entries to the skipped entries CSV
    with open(SKIPPED_ENTRIES_FILENAME, mode='w', newline='', encoding='utf-8') as skipped_file:
        skipped_writer = csv.DictWriter(skipped_file, fieldnames=skipped_entries_column_names)
        skipped_writer.writeheader()
        skipped_writer.writerows(sorted_skipped_entries)

    stop_listener()

if __name__ == "__main__":
    main()
