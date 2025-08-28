
import asyncio, json, logging, os, sys, argparse
from logging.handlers import QueueHandler, QueueListener
from queue import Queue

PKG_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.dirname(PKG_DIR)
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from .archive_extraction import extract_archives_in_memory
from .dex_matching import build_spawn_dex_dict, build_species_dex_dict, match_dex_numbers
from .data_processing import process_entry
from .species_addition_handler import parse_species_additions, merge_additions_against_spawns
from .output_writer import write_main_csv, write_skipped_entries_csv, write_additions_csv

try:
    from colorama import init as colorama_init, Fore, Style
    colorama_init()
    COLOR_OK = Fore.CYAN + Style.BRIGHT
    COLOR_END = Style.RESET_ALL
except Exception:
    COLOR_OK = ""
    COLOR_END = ""

# --- Colorized console formatter (FUN_MODE) -----------------------------------
class ColorFormatter(logging.Formatter):
    def __init__(self, fmt, datefmt=None, fun=False):
        super().__init__(fmt, datefmt)
        self.fun = fun
        try:
            from colorama import Fore, Style
            self.Fore, self.Style = Fore, Style
        except Exception:
            # No colorama? No problem — stay plain.
            self.Fore = type("F", (), {k: "" for k in ["BLUE","CYAN","YELLOW","RED","GREEN","MAGENTA","WHITE"]})()
            self.Style = type("S", (), {"BRIGHT":"", "RESET_ALL":"", "DIM":""})()

    def format(self, record):
        msg = super().format(record)
        if not self.fun:
            return msg
        # Color by level
        if record.levelno >= logging.CRITICAL:
            color = self.Style.BRIGHT + self.Fore.RED
        elif record.levelno >= logging.ERROR:
            color = self.Style.BRIGHT + self.Fore.RED
        elif record.levelno >= logging.WARNING:
            color = self.Style.BRIGHT + self.Fore.YELLOW
        elif record.levelno >= logging.INFO:
            color = self.Fore.CYAN
        else:  # DEBUG
            color = self.Fore.BLUE
        return f"{color}{msg}{self.Style.RESET_ALL}"
# ------------------------------------------------------------------------------

# Extra accents for FUN_MODE messaging inside strings
try:
    from colorama import Fore as F, Style as S
except Exception:
    class _N: ...
    F = _N(); S = _N()
    for k in ("CYAN","GREEN","YELLOW","RED","MAGENTA","WHITE","BLUE"): setattr(F, k, "")
    for k in ("BRIGHT","RESET_ALL","DIM"): setattr(S, k, "")

# --- config helpers ---
def resolve_config_path():
    local = os.path.join(PKG_DIR, "config.json")
    fallback = os.path.join(os.getcwd(), "config.json")
    return local if os.path.exists(local) else fallback

def load_config_from(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# --- logging setup ---
def setup_logging(cfg):
    fun = bool(cfg.get("FUN_MODE", False))
    log_queue = Queue()
    qh = QueueHandler(log_queue)

    root = logging.getLogger()
    root.setLevel(getattr(logging, cfg.get("LOG_LEVEL","INFO").upper(), logging.INFO))

    fmt_str = cfg.get("LOG_FORMAT","%(asctime)s - %(levelname)s - %(message)s")
    plain_fmt = logging.Formatter(fmt_str)
    color_fmt = ColorFormatter(fmt_str, fun=fun)

    # File stays plain (no escape codes in logs)
    fh = logging.FileHandler(cfg.get("LOG_FILENAME","process_log.txt"), encoding="utf-8")
    fh.setFormatter(plain_fmt)

    # Console gets colors in FUN_MODE
    ch = logging.StreamHandler()
    ch.setFormatter(color_fmt)

    # Avoid duplicates if re-run in same interpreter
    if not any(isinstance(h, QueueHandler) for h in root.handlers):
        root.addHandler(qh)

    listener = QueueListener(log_queue, fh, ch)
    listener.start()
    return listener

# --- args & modes ---
def build_arg_parser():
    p = argparse.ArgumentParser(
        prog="cobblemon_processor",
        description="Cobblemon Spawndata Processor (modular)"
    )
    # Simple modes
    p.add_argument("--mode",
                   choices=["default","additions","skipped","full"],
                   default="default",
                   help="default: Dex-matched only; additions: species_addition only; skipped: unmatched-only; full: all outputs")
    # Optional overrides
    p.add_argument("--archives-dir", help="Override ARCHIVES_DIR from config.json")
    p.add_argument("--max-workers", type=int, help="Override MAX_WORKERS from config.json")
    p.add_argument("--output-main", help="Override main CSV filename")
    p.add_argument("--output-skipped", help="Override skipped entries CSV filename")
    p.add_argument("--output-additions", help="Override species additions CSV filename")
    p.add_argument("--sort-primary", help="Primary sort key (default from config)")
    p.add_argument("--sort-secondary", help="Secondary sort key (default from config)")
    return p

def apply_mode(args):
    m = args.mode
    if m == "default":
        include_normal = True
        write_main = True
        write_skipped = False
        include_additions = False
        include_spawn_without_species = False
    elif m == "additions":
        include_normal = False
        write_main = False
        write_skipped = False
        include_additions = True
        include_spawn_without_species = False
    elif m == "skipped":
        include_normal = True           # we still run normal to compute skipped
        write_main = False              # but we won't write the main CSV
        write_skipped = True
        include_additions = False
        include_spawn_without_species = False
    elif m == "full":
        include_normal = True
        write_main = True
        write_skipped = True
        include_additions = True
        include_spawn_without_species = False
    else:
        # Fallback to default
        include_normal = True
        write_main = True
        write_skipped = False
        include_additions = False
        include_spawn_without_species = False
    return include_normal, write_main, write_skipped, include_additions, include_spawn_without_species

# --- main orchestrator ---
async def main(argv=None):
    # Load config and set up logging
    cfg_path = resolve_config_path()
    cfg = load_config_from(cfg_path)
    listener = setup_logging(cfg)
    logging.info(f"{S.DIM}Using config:{S.RESET_ALL} {cfg_path}")
    try:
        parser = build_arg_parser()
        args = parser.parse_args(argv)
        include_normal, write_main, write_skipped_csv, include_additions, include_spawn_wo_species = apply_mode(args)

        # Resolve runtime options
        raw_archives = args.archives_dir or cfg["ARCHIVES_DIR"]
        cfg_dir = os.path.dirname(cfg_path)
        ARCHIVES_DIR = raw_archives if os.path.isabs(raw_archives) else os.path.normpath(os.path.join(cfg_dir, raw_archives))
        logging.info(f"{S.DIM}ARCHIVES_DIR (resolved):{S.RESET_ALL} {ARCHIVES_DIR}")

        MAX_WORKERS = args.max_workers or cfg.get("MAX_WORKERS", 8)
        FUN_MODE = cfg.get("FUN_MODE", False)

        out_main = (args.output_main or cfg.get("output_filename", "output.csv"))
        if not out_main.endswith(".csv"): out_main += ".csv"
        out_skipped = (args.output_skipped or cfg.get("skipped_entries_filename", "skipped_entries.csv"))
        if not out_skipped.endswith(".csv"): out_skipped += ".csv"

        # Additions filename: CLI > config.species_additions_filename > derived from output_filename > fallback
        if args.output_additions:
            out_additions = args.output_additions
        elif "species_additions_filename" in cfg:
            out_additions = cfg["species_additions_filename"]
        elif "output_filename" in cfg:
            base = cfg["output_filename"]
            base_no_ext = base[:-4] if base.lower().endswith(".csv") else base
            out_additions = f"{base_no_ext}_species_addition_entries.csv"
        else:
            out_additions = "species_addition_entries.csv"
        if not out_additions.endswith(".csv"): out_additions += ".csv"

        primary = args.sort_primary or cfg.get("primary_sorting_key", "Pokemon Name")
        secondary = args.sort_secondary or cfg.get("secondary_sorting_key", "Spawn ID")

        logging.info(COLOR_OK + f"Mode={args.mode} | normal={include_normal} additions={include_additions} skipped_csv={write_skipped_csv} write_main={write_main}" + COLOR_END)

        # === Extraction ===
        result = extract_archives_in_memory(
            ARCHIVES_DIR,
            max_workers=MAX_WORKERS,
            logger=logging.getLogger(__name__),
            fun_mode=FUN_MODE
        )
        counts = result.counts()
        logging.info(
          f"{S.BRIGHT}{F.MAGENTA}Counts{S.RESET_ALL} → "
          f"spawn: {S.BRIGHT}{F.GREEN}{counts['spawn_files']}{S.RESET_ALL}, "
          f"species: {S.BRIGHT}{F.GREEN}{counts['species_files']}{S.RESET_ALL}, "
          f"additions: {S.BRIGHT}{F.GREEN}{counts['species_addition_files']}{S.RESET_ALL}"
        )

        rows, skipped = [], []

        # === Normal species path (Dex-based) ===
        if include_normal:
            spawn_dex = build_spawn_dex_dict(result.spawn_files)
            species_dex = build_species_dex_dict(result.species_files)
            matched = match_dex_numbers(spawn_dex, species_dex)

            sem = asyncio.Semaphore(10)
            async def process_with_limit(dex):
                async with sem:
                    return await process_entry(dex, matched)

            # Only process entries that have species_json present at tuple index 4
            iter_keys = [d for d, tup in matched.items() if tup[4] is not None] if not include_spawn_wo_species else list(matched.keys())

            normal_results = await asyncio.gather(*[process_with_limit(d) for d in iter_keys])
            for r, s in normal_results:
                if r: rows.extend(r)
                if s and write_skipped_csv: skipped.append(s)

        # === Species additions path (name-based) ===
        addition_rows = []
        if include_additions:
            additions_by_name = parse_species_additions(result.species_addition_files)
            addition_rows = merge_additions_against_spawns(result.spawn_files, additions_by_name)

        # === Sorting ===
        rows_sorted = sorted(rows, key=lambda e: (str(e.get(primary,"")).lower(), str(e.get(secondary,"")).lower()))
        skipped_sorted = sorted(skipped, key=lambda e: (str(e.get(primary,"")).lower(), str(e.get(secondary,"")).lower()))
        additions_sorted = sorted(addition_rows, key=lambda e: (str(e.get(primary,"")).lower(), str(e.get(secondary,"")).lower()))

        # === Output ===
        if include_normal and write_main:
            write_main_csv(rows_sorted, out_main)
        if include_normal and write_skipped_csv:
            write_skipped_entries_csv(skipped_sorted, out_skipped)
        if include_additions:
            write_additions_csv(additions_sorted, out_additions)

        logging.info(
          f"{S.BRIGHT}{F.GREEN}Done.{S.RESET_ALL} Wrote: "
          + (out_main if (include_normal and write_main) else "(no main)") + ", "
          + (out_skipped if (include_normal and write_skipped_csv) else "(no skipped)") + ", "
          + (out_additions if include_additions else "(no additions)")
        )
    finally:
        listener.stop()

if __name__ == "__main__":
    asyncio.run(main())
