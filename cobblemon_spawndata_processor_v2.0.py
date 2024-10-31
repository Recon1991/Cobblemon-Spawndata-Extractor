import asyncio
import aiofiles
import cProfile
import csv
import io
import json
import logging
import os
import pstats
import shutil
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from logging.handlers import QueueHandler, QueueListener
from queue import Queue

from column_names import column_names, skipped_entries_column_names
from utils import format_location_names, get_weather_condition, get_sky_condition, get_moon_phase_name


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

def extract_archives_in_memory(archives_dir):
    """
    Extracts specific JSON files from zip/jar files in memory without writing to disk.
    Only extracts 'data/cobblemon/spawn_pool_world' and 'data/cobblemon/species'.
    Tracks the original archive for each extracted file.
    """
    extracted_files_mapping = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for archive_name in os.listdir(archives_dir):
            if archive_name.endswith(('.zip', '.jar')):
                archive_path = os.path.join(archives_dir, archive_name)
                futures.append(executor.submit(extract_specific_files_in_memory, archive_path, extracted_files_mapping))

        for future in as_completed(futures):
            future.result()

    return extracted_files_mapping

def extract_specific_files_in_memory(archive_path, extracted_files_mapping):
    """Extract specific JSON files from a zip/jar file in memory."""
    with zipfile.ZipFile(archive_path, 'r') as zip_file:
        for file_info in zip_file.infolist():
            if file_info.filename.startswith(('data/cobblemon/spawn_pool_world', 'data/cobblemon/species')):
                data = zip_file.read(file_info)
                extracted_files_mapping[file_info.filename] = (data, os.path.basename(archive_path), os.path.dirname(file_info.filename).split('/')[-1])
        print(f"Extracted relevant files from '{os.path.basename(archive_path)}' into memory.")

def extract_dex_number_from_filename(filename):
    """Extract and format the Dex number from the filename."""
    base_name = os.path.basename(filename)
    dex_number = base_name.split('_')[0]
    return dex_number.lstrip('0').zfill(4)

def build_spawn_dex_dict(extracted_files_mapping):
    """Build spawn Dex dictionary from in-memory extracted files."""
    spawn_dex_dict = {}
    logging.info("Building spawn Dex dictionary...")

    for file_name, (data, archive_name, directory_name) in extracted_files_mapping.items():
        if 'spawn_pool_world' in file_name and file_name.endswith('.json'):
            dex_number = extract_dex_number_from_filename(file_name)
            spawn_dex_dict[dex_number] = (file_name, data, archive_name, directory_name)

    logging.info(f"Built spawn Dex dict with {len(spawn_dex_dict)} entries.")
    return spawn_dex_dict

async def build_species_dex_dict(extracted_files_mapping):
    """Build species Dex dictionary from in-memory extracted files."""
    species_dex_dict = {}
    logging.info("Building species Dex dictionary...")

    for file_name, (data, archive_name, directory_name) in extracted_files_mapping.items():
        if 'species' in file_name and file_name.endswith('.json'):
            try:
                data = json.loads(data)
                dex_number = str(data.get("nationalPokedexNumber")).zfill(4)
                species_dex_dict[dex_number] = (file_name, data, directory_name, archive_name)
            except json.JSONDecodeError as e:
                logging.error(f"Error reading {file_name}: {e}")

    logging.info(f"Built species Dex dict with {len(species_dex_dict)} entries.")
    return species_dex_dict

def match_dex_numbers(spawn_dex, species_dex):
    """
    Match Dex numbers from spawn and species dictionaries and prepare them for processing.
    Returns a dictionary with Dex numbers as keys and tuples containing (spawn archive, 
    spawn file, species archive, species file, species data).
    """
    matched_dex = {}

    all_dex_numbers = set(spawn_dex.keys()).union(set(species_dex.keys()))

    for dex_number in all_dex_numbers:
        spawn_info = spawn_dex.get(dex_number, (None, None, None))
        species_info = species_dex.get(dex_number, (None, None, None, None))

        matched_dex[dex_number] = (
            spawn_info[0],  # Spawn file name
            spawn_info[1],  # Spawn data
            spawn_info[2],  # Original spawn archive
            species_info[0],  # Species file name
            species_info[1],  # Species data
            species_info[2],  # Original species archive
            species_info[3]   # Original species data
        )

    return matched_dex

async def process_entry_with_limit(dex_number, matched_dex_dict, semaphore):
    """Process and merge data for a single Dex entry with a concurrency limit."""
    async with semaphore:
        return await process_entry(dex_number, matched_dex_dict)

async def process_entry(dex_number, matched_dex_dict):
    """Process and merge data for a single Dex entry."""
    spawn_file, spawn_data, original_spawn_archive, species_file, species_data, species_directory, original_species_archive = matched_dex_dict[dex_number]

    # Extract species data if available
    if not species_data:
        pokemon_name, primary_type, secondary_type, egg_groups, generation, labels = "", "", "", "", "", ""
    else:
        pokemon_name, primary_type, secondary_type, egg_groups, generation, labels = extract_species_info(species_data)

    if not spawn_file or not spawn_data:
        skipped_entry = {
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
        return None, skipped_entry

    try:
        spawn_data = json.loads(spawn_data) or {"spawns": []}
        merged_entries = []

        for entry in spawn_data["spawns"]:
            merged_entry = build_merged_entry(
                dex_number, species_data, entry, spawn_file, 
                species_file, original_spawn_archive, original_species_archive, 
                generation, species_directory
            )
            if merged_entry:
                merged_entries.append(merged_entry)

        return merged_entries, None

    except Exception as e:
        logging.error(f"Error processing Dex {dex_number}: {e}")
        return None, None

def build_merged_entry(dex_number, species_data, entry, spawn_file, species_file, original_spawn_archive, original_species_archive, generation, species_directory):
    """Build a merged entry dictionary for a given Pokémon."""
    pokemon_name = entry.get("pokemon", "").strip()
    if not pokemon_name:
        return None

    pokemon_species_data = get_species_data(pokemon_name, species_data)

    primary_type = pokemon_species_data.get("primaryType", "").title()
    secondary_type = pokemon_species_data.get("secondaryType", "-----").title()
    egg_groups = ', '.join(pokemon_species_data.get("eggGroups", [])).title()
    meaningful_labels = [
        label.strip().replace('_', ' ').title() 
        for label in species_data.get("labels", [])
        if not label.lower().startswith("gen")
    ]
    labels = ', '.join(meaningful_labels) if meaningful_labels else ""
    time_range = entry.get("condition", {}).get("timeRange", "Any").title()
    sky_condition = get_sky_condition(entry)

    # Condense the archive paths for better readability
    spawn_archive = os.path.basename(spawn_file) if spawn_file else "Unknown"
    species_archive = f"{species_directory}/{os.path.basename(species_file)}" if species_directory and species_file else "Unknown"
    original_spawn_archive = os.path.basename(original_spawn_archive) if original_spawn_archive else "Unknown"
    original_species_archive = os.path.basename(original_species_archive) if original_species_archive else "Unknown"

    return {
        "Dex Number": f"#{str(dex_number).zfill(4)}",
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
        "Moon Phase": get_moon_phase_name(entry.get("condition", {}).get("moonPhase", [])),
        "Anti-Moon Phase": get_moon_phase_name(entry.get("anticondition", {}).get("moonPhase", [])),
        "Base Blocks": ', '.join(format_location_names(entry.get("condition", {}).get("neededBaseBlocks", []))),
        "Nearby Blocks": ', '.join(format_location_names(entry.get("condition", {}).get("neededNearbyBlocks", []))),
        "Weight": entry.get("weight", ""),
        "Context": entry.get("context", "").title(),
        "Spawn ID": entry.get("id", "Unknown"),
        #"Spawn Archive": spawn_archive,
        "Species Archive": species_archive,
        "Original Spawn Archive": original_spawn_archive,
        "Original Species Archive": original_species_archive
    }

def get_species_data(pokemon_name, species_data):
    """Retrieve species data for a given Pokémon name."""
    return next(
        (form for form in species_data.get("forms", []) if form["name"].lower() in pokemon_name.lower()),
        species_data  # Default to base data
    )

def extract_species_info(species_data):
    """Extract species information such as name, types, egg groups, labels, and generation."""
    pokemon_name = species_data.get("name", "Unknown")
    primary_type = species_data.get("primaryType", "")
    secondary_type = species_data.get("secondaryType", "")
    egg_groups = ', '.join(species_data.get("eggGroups", []))

    # Extract labels and generation
    all_labels = species_data.get("labels", [])
    generation_label = next((label for label in all_labels if label.startswith("gen")), None)
    generation = generation_label.replace('gen', 'Gen ').capitalize() if generation_label else ""

    meaningful_labels = [
        label.strip().replace('_', ' ').title() 
        for label in all_labels
        if not label.lower().startswith("gen")
    ]
    labels = ', '.join(meaningful_labels) if meaningful_labels else ""

    return pokemon_name, primary_type, secondary_type, egg_groups, generation, labels

async def main():
    """Main function to extract and merge Pokémon data."""
    # Step 1: Extract archives to memory
    extracted_files_mapping = extract_archives_in_memory(ARCHIVES_DIR)

    # Step 2: Build Dex dictionaries
    spawn_dex = build_spawn_dex_dict(extracted_files_mapping)
    species_dex = await build_species_dex_dict(extracted_files_mapping)
    matched_dex_dict = match_dex_numbers(spawn_dex, species_dex)

    all_rows = []  # Store valid entries
    skipped_entries = []  # Store skipped entries

    # Set up a semaphore to limit concurrency
    semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent tasks

    # Process entries in parallel and collect rows
    tasks = [process_entry_with_limit(dex, matched_dex_dict, semaphore) for dex in matched_dex_dict]
    results = await asyncio.gather(*tasks)

    for result, skipped in results:
        if result:
            all_rows.extend(result)  # Collect valid rows
        if skipped:
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

    # Write sorted valid rows to the main CSV in batches
    BATCH_SIZE = 1000
    with open(CSV_FILENAME, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=column_names)
        writer.writeheader()
        batch = []

        for row in sorted_rows:
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                writer.writerows(batch)
                batch = []

        # Write any remaining rows
        if batch:
            writer.writerows(batch)

    # Sort the skipped entries the same way
    sorted_skipped_entries = sorted(
        skipped_entries,
        key=lambda entry: (
            entry.get(sort_key, "").lower(),
            entry.get(secondary_sort_key, "").lower() if secondary_sort_key else ""
        )
    )

    # Write sorted skipped entries to the skipped entries CSV in batches
    with open(SKIPPED_ENTRIES_FILENAME, mode='w', newline='', encoding='utf-8') as skipped_file:
        skipped_writer = csv.DictWriter(skipped_file, fieldnames=skipped_entries_column_names)
        skipped_writer.writeheader()
        batch = []

        for row in sorted_skipped_entries:
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                skipped_writer.writerows(batch)
                batch = []

        # Write any remaining rows
        if batch:
            skipped_writer.writerows(batch)

    print(f"Processing complete, written to {CSV_FILENAME}")
    stop_listener()

if __name__ == "__main__":
    asyncio.run(main())
