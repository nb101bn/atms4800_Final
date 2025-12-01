<!-- Copilot / AI agent instructions for the atms4800_Final repo -->
# Quick agent guide — atms4800_Final

These notes are focused, actionable, and specific to this repository so an AI coding agent can be productive immediately.

1) Big picture
- **Purpose:** A small static frontend (HTML/CSS/JS) plus Python tools to fetch, process, regrid, and visualize meteorological station (ASOS/METAR) data for Missouri. Key outputs are gridded xarray Datasets (netCDF) and map images.
- **Major components:**
  - Frontend: `index.html`, `pages/` (static content), `css/custom.css`, `js/listener.js`, `js/updater.js` — static UI and simple listeners.
  - Data pipeline (Python): `py/generator.py` (primary end-to-end example), `py/processer.py` (generalized processing helpers), `py/map_generation.py` (legacy/expanded station lists and plotting helpers), `py/interpolater.py` (placeholder/empty).
  - Data assets: `Surface_Data.text` and `images/` (icons and map tiles).

2) What to edit and why (service boundaries)
- Short-lived UI work (HTML/CSS/JS) stays in root/`pages` and `js/`. Changing Python code should not require frontend edits unless altering output file locations or image names.
- Python modules are the core: `generator.py` implements the high-level workflow: fetch -> filter -> unit conversion -> interpolate -> xarray dataset -> plot. Prefer extending/using `processer.py` for generalized ingestion and `map_generation.py` for complex station selections.

3) Patterns and conventions discovered in code
- Functions return xarray `Dataset` objects for downstream use (see `process_and_map_asos` in `py/generator.py`).
- Scripts commonly include an `if __name__ == '__main__'` example block — safe to run directly.
- Network calls are made directly (requests to Iowa State Mesonet). Many scripts include print/log-style progress rather than structured logging.
- Several modules contain partially-implemented or placeholder functions (e.g., `py/interpolater.py`, some parts of `py/processer.py`), so assume changes may require adding missing implementations or tests.

4) Developer workflows & commands (how humans run things)
- Run the example end-to-end (Windows PowerShell):
  ```powershell
  python py\generator.py
  ```
  Or call the main function from another script or REPL:
  ```python
  from py.generator import process_and_map_asos
  ds = process_and_map_asos('2025-11-29', 18)
  ```
- Debugging in-place (pdb):
  ```powershell
  python -m pdb py\generator.py
  ```
- Recommended development environment setup (not enforced): create a venv and install dependencies discovered in the code:
  ```powershell
  python -m venv .venv; .\.venv\Scripts\Activate.ps1
  pip install numpy pandas xarray metpy cartopy matplotlib requests netCDF4 siphon geopandas
  ```
  Note: `cartopy` and `geopandas` may require system packages; install them carefully for your platform.

5) Important files to reference when making changes
- `py/generator.py` — primary end-to-end workflow and best example of how functions are used together. Look here first for behavior expectations.
- `py/processer.py` — generalized ingestion helpers; preferred place for new reusable fetch/transform logic.
- `py/map_generation.py` — contains long lists of station codes and plotting utilities; useful when extending station coverage.
- `index.html`, `js/listener.js`, `js/updater.js` — frontend integration points for displaying maps or assets.

6) Rules for the AI agent (do this / avoid this)
- Do:
  - Preserve public APIs: functions that return xarray Datasets should keep returning Dataset objects.
  - Keep network calls configurable. If you change an external endpoint, update the top-level constants (e.g., `ASOS_BASE_URL` in `py/generator.py`) and keep a fallback/timeout.
  - Use the `if __name__ == '__main__'` blocks for runnable examples; preserve them.
  - Add unit tests or small smoke scripts when adding/altering data processing logic.
- Avoid:
  - Hardcoding credentials or writing secrets into the repo.
  - Breaking the on-disk output conventions (expected output is a netCDF-like xarray Dataset and optional generated plot). If you change file names/paths, update frontend references.
  - Running live network fetches in CI without mocking — network calls assume availability of external services.

7) Quick examples to show patterns
- To add a new derived field to the gridded dataset, extend `regrid_and_save` in `py/generator.py` and add corresponding interpolation keys in `data` before creating the xarray `Dataset`.
- To add a new station group, append the `station=` list in `py/map_generation.py` (beware of URL length and the ASOS API limits). Prefer grouping large lists into multiple requests.

8) Gaps / known hazards for the agent
- Many modules have TODOs and placeholders. Validate behavior by running `py/generator.py` locally with a small recent date/time.
- Cartopy/geopandas/cartographic dependencies are environment-sensitive; plotting code may fail on minimal CI images.

9) If you make changes, please ask
- Which target workflows should be preserved (script runs, saved file names, frontend image hooks)?

---
If any section is unclear or you'd like more examples (unit tests, a requirements file, or a small CI-friendly smoke test), tell me which part to expand and I'll iterate.
