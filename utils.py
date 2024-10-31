# utils.py

import asyncio
import aiofiles
import json
import os

def format_location_names(locations):
    """Format biome, structure, or other location names for better readability."""
    formatted_locations = []
    for location in locations:
        # Handle namespaced locations with optional descriptors
        if ':' in location:
            parts = location.split(':')
            if '/' in parts[1]:
                namespace, descriptor = parts[1].split('/')
                # Format namespace and descriptor
                formatted_namespace = namespace.replace('_', ' ').title()
                formatted_descriptor = descriptor.replace('is_', '').replace('_', ' ').title()
                formatted_location = f"{formatted_namespace}: {formatted_descriptor}"
            else:
                # Simple namespace case, such as 'is_overworld'
                name = parts[1]
                if name.startswith("is_"):
                    name = name[3:]  # Remove the 'is_' prefix
                formatted_location = name.replace('_', ' ').strip().title()
        else:
            # No namespace, just format the name normally
            formatted_location = location.replace('_', ' ').title()
        
        formatted_locations.append(formatted_location)
    
    return formatted_locations

def get_moon_phase_name(moon_phases):
    """Convert moon phase numbers (0-7) to readable moon phase names."""
    moon_phase_map = {
        0: "Full Moon",
        1: "Waning Gibbous",
        2: "Last Quarter",
        3: "Waning Crescent",
        4: "New Moon",
        5: "Waxing Crescent",
        6: "First Quarter",
        7: "Waxing Gibbous"
    }
    
    # if moon_phases is None or Empty, return an empty string
    if not moon_phases:
        return ""
        
    # if moon_phases is a list, map each number to the corresponding phase name
    if isinstance(moon_phase, list):
        phase_names =   [moon_phase_map.get(phase, "Unknown Phase") for phase in moon_phases]
        return ', '.join(phase_names)
        
    # if moon_phases is a single value, map it directly
    return moon_phase_map.get(moon_phases, "Unknown Phase")

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

def extract_dex_number_from_filename(filename):
    """Extract and format the Dex number from the filename."""
    base_name = os.path.basename(filename)
    dex_number = base_name.split('_')[0]
    return dex_number.lstrip('0')
	
async def extract_json_data_cached(file_path):
    """Extract JSON data with caching."""
    try:
        async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
            data = await f.read()
            return json.loads(data)
    except json.JSONDecodeError as e:
        logging.error(f"Error reading {file_path}: {e}")
        return None
		
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