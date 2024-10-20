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
    archive_path = os.path.join(ARCHIVES_DIR, archive_name)
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

    for archive_name in os.listdir(ARCHIVES_DIR):
        if archive_name.endswith(('.zip', '.jar')):
            with zipfile.ZipFile(os.path.join(ARCHIVES_DIR, archive_name), 'r') as zip_file:
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

    for archive_name in os.listdir(ARCHIVES_DIR):
        if archive_name.endswith(('.zip', '.jar')):
            with zipfile.ZipFile(os.path.join(ARCHIVES_DIR, archive_name), 'r') as zip_file:
                for file_info in zip_file.infolist():
                    if 'data/cobblemon/species/' in file_info.filename and file_info.filename.endswith('.json'):
                        data = extract_json_data_cached(archive_name, file_info.filename)
                        if data:
                            dex_number = str(data.get("nationalPokedexNumber"))
                            species_dex_dict[dex_number] = (archive_name, file_info.filename)

    logging.info(f"Built species Dex dict with {len(species_dex_dict)} entries.")
    return species_dex_dict

def get_species_data(pokemon_name, species_data):
    return next(
        (form for form in species_data.get("forms", []) if form["name"].lower() in pokemon_name.lower()),
        species_data  # Default to base data
    )

def match_dex_numbers(spawn_dex, species_dex):
    """
    Match Dex numbers from spawn and species dictionaries and prepare them for processing.
    Returns a dictionary with Dex numbers as keys and tuples containing (spawn archive, 
    spawn file, species archive, species file).
    """
    matched_dex = {}

    # Combine spawn and species data using Dex numbers
    all_dex_numbers = set(spawn_dex.keys()).union(set(species_dex.keys()))

    for dex_number in all_dex_numbers:
        spawn_info = spawn_dex.get(dex_number, (None, None))
        species_info = species_dex.get(dex_number, (None, None))

        # Store matched entries with relevant information
        matched_dex[dex_number] = (
            spawn_info[0],  # Spawn archive name
            spawn_info[1],  # Spawn file name
            species_info[0],  # Species archive name
            species_info[1]  # Species file name
        )

    return matched_dex
    
def sort_rows(rows, primary_key, secondary_key=None):
    """Sorts rows by the given primary key and an optional secondary key."""
    try:
        return sorted(
            rows,
            key=lambda x: (
                x.get(primary_key, "").lower(),  # Primary sort key
                x.get(secondary_key, "").lower() if secondary_key else ""  # Secondary sort key (optional)
            )
        )
    except KeyError:
        logging.warning(f"Invalid primary key '{primary_key}', defaulting to 'Pokemon Name'.")
        return sorted(
            rows,
            key=lambda x: (
                x.get("Pokemon Name", "").lower(),
                x.get(secondary_key, "").lower() if secondary_key else ""
            )
        )

# Collect Pokemon entries that skipped processing
skipped_entries = []

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
        species_data = extract_json_data_cached(species_archive, species_file)
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

            # Filter out non-generation labels
            meaningful_labels = [
                label.strip().replace('_', ' ').title() 
                for label in all_labels
                if not label.lower().startswith("gen")
            ]
            labels = ', '.join(meaningful_labels) if meaningful_labels else "N/A"

    # Handle skipped entries (no spawn data)
    if not spawn_archive or not spawn_file:
        skipped_entries.append({
            "Dex Number": dex_number,
            "Pokemon Name": pokemon_name,
            "Primary Type": primaryType,
            "Secondary Type": secondaryType,
            "Egg Groups": egg_groups,
            "Generation": generation,
            "Labels": labels
        })
        logging.info(f"Skipping Dex {dex_number} ({pokemon_name}) - No spawn data.")
        return None

    try:
        # Extract spawn data or use an empty structure if not found
        spawn_data = extract_json_data_cached(spawn_archive, spawn_file) or {"spawns": []}
        merged_entries = []

        # Loop through each spawn entry
        for entry in spawn_data["spawns"]:
            pokemon_name = entry.get("pokemon", "").strip()
            if not pokemon_name:
                continue  # Skip invalid entries

            # Get the appropriate species data (handle forms if necessary)
            pokemon_species_data = get_species_data(pokemon_name, species_data)

            # Extract relevant data for the merged entry
            primary_type = pokemon_species_data.get("primaryType", "")
            secondary_type = pokemon_species_data.get("secondaryType", "")
            egg_groups = ', '.join(pokemon_species_data.get("eggGroups", []))
            meaningful_labels = [
                label.strip().replace('_', ' ').title() 
                for label in all_labels
                if not label.lower().startswith("gen")
            ]
            labels = ', '.join(meaningful_labels) if meaningful_labels else "N/A"

            # Append the merged entry
            merged_entries.append({
                "Dex Number": dex_number,
                "Pokemon Name": pokemon_name.title(),
                "Primary Type": primary_type,
                "Secondary Type": secondary_type,
                "Rarity": entry.get("bucket", ""),
                "Egg Groups": egg_groups,
                "Generation": generation,  # Use the pre-extracted generation
                "Labels": labels,  # Use the pre-extracted labels
                "Time": entry.get("time", "Any"),
                "Weather": get_weather_condition(entry.get("condition", {})),
                "Sky": get_sky_condition(entry.get("condition", {})),
                "Presets": ', '.join(entry.get("presets", [])) or "",
                "Biomes": ', '.join(format_location_names(entry.get("condition", {}).get("biomes", []))).strip(),
                "Anti-Biomes": ', '.join(format_location_names(entry.get("anticondition", {}).get("biomes", []))).strip(),
                "Structures": ', '.join(format_location_names(entry.get("condition", {}).get("structures", []))).strip(),
                "Anti-Structures": ', '.join(format_location_names(entry.get("anticondition", {}).get("structures", []))).strip(),
                "Moon Phase": entry.get("condition", {}).get("moonPhase", ""),
                "Anti-Moon Phase": entry.get("anticondition", {}).get("moonPhase", ""),
                "Base Blocks": ', '.join(format_location_names(entry.get("condition", {}).get("neededBaseBlocks", []))),
                "Nearby Blocks": ', '.join(format_location_names(entry.get("condition", {}).get("neededNearbyBlocks", []))),
                "Weight": entry.get("weight", ""),
                "Context": entry.get("context", ""),
                "Spawn ID": entry.get("id", "Unknown"),
                "Spawn Archive": spawn_archive,
                "Species Archive": species_archive
            })

        return merged_entries  # Closing the function properly

    except Exception as e:
        logging.error(f"Error processing Dex {dex_number}: {e}")
        return None


def main():
    """Main function to extract and merge Pokémon data."""
    sort_key = config.get('primary_sorting_key', "Pokemon Name")  # Fallback to default
    secondary_sort_key = config.get('secondary_sorting_key', None)  # Optional
    max_workers = config.get("MAX_WORKERS", 8)  # Default to 8 workers if not in config

    # Build Dex dictionaries
    spawn_dex = build_spawn_dex_dict()
    species_dex = build_species_dex_dict()
    matched_dex_dict = match_dex_numbers(spawn_dex, species_dex)

    all_rows = []  # Store valid entries
    skipped_entries = []  # Store skipped entries

    # Process entries in parallel and collect rows
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_entry, dex, matched_dex_dict): dex for dex in matched_dex_dict}
        for future in as_completed(futures):
            result = future.result()
            if result:
                all_rows.extend(result)  # Collect valid rows
            else:
                skipped_entries.append(result)  # Collect skipped rows

    # Sort the valid rows using primary and secondary keys
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

    filtered_skipped_entries = [entry for entry in skipped_entries if entry is not None]

    # Sort the skipped entries the same way
    sorted_skipped_entries = sorted(
        filtered_skipped_entries,
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
