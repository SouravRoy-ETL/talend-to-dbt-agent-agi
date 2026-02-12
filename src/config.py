import os

# --- AGENT CONFIGURATION ---
AGENT_NAME = "TalendToDbtSouravAgent"
OLLAMA_MODEL = "qwen3-coder:30b" 
OLLAMA_BASE_URL = "http://localhost:11434"

# --- TARGET DIALECT ---
TARGET_DB = "DuckDB" 

# --- PATHS ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(BASE_DIR, "input_data", "TALEND_PROJECT")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
DBT_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "dbt_project")
TEMPORAL_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "temporal_workflows")

# --- DYNAMIC MEMORY PATHS ---
# The Agent writes these itself during Phase 0
PROJECT_CONTEXT_FILE = os.path.join(OUTPUT_DIR, "inferred_context.md")
STYLE_GUIDE_FILE = os.path.join(OUTPUT_DIR, "inferred_style_guide.md")