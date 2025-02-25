# column_names.py

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

column_names = [
    "Dex Number",              # Pokémon's Dex number
    "Pokemon Name",            # Pokémon's name
    "Primary Type",            # Primary type (e.g., Fire)
    "Secondary Type",          # Secondary type (optional)
    "Rarity",                  # Rarity bucket
    "Sky",                     # Sky visibility conditions (like 'Must See Sky')
    "Light",                   # Light value conditions (0-7 = non-torch light, 8-15 = torch light)
    "Time",                    # Time of day for spawn
    "Weather",                 # Weather conditions needed
    "Biomes",                  # Biomes where the Pokémon spawns
    "Anti-Biomes",             # Biomes where it won't spawn
    "Structures",              # Structures required for spawn
    "Anti-Structures",         # Structures that block spawn
    "Base Blocks",             # Blocks needed to spawn on
    "Nearby Blocks",           # Nearby blocks required for spawn
    "Moon Phase",              # Moon phase needed for spawn
    "Anti-Moon Phase",         # Moon phases that block spawn
    "Presets",                 # Preset configurations (comma-separated)
    "Generation",              # Generation (like Gen 1, Gen 9)
    "Labels",                  # Additional labels (like Legendary, Mythical)
    "Egg Groups",              # Egg groups (comma-separated)
    "Weight",                  # Weight factor for spawn chance
    "Context",                 # Spawn context (e.g., Surface, Underground)
    "Spawn ID",                # ID of the spawn entry      
    #"Spawn Archive",           # Spawn data directory 
    "Species Archive",         # Species data directory
    "Original Spawn Archive",  # Spawn archive filename
    "Original Species Archive" # Species archive filename
]

skipped_entries_column_names = [
    "Dex Number",              # Pokémon's Dex number
    "Pokemon Name",            # Pokémon's name
    "Primary Type",            # Primary type (e.g., Fire)
    "Secondary Type",          # Secondary type (optional)
    "Egg Groups",              # Egg groups (comma-separated)
    "Generation",              # Generation (like Gen 1, Gen 9)
    "Labels",                  # Additional labels (like Legendary, Mythical)
    "Species Archive"          # Which archive the species data was from
]
