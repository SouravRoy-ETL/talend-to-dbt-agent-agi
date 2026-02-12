import os
import sys
import time
import subprocess
import webbrowser
from src.main_engine import MigrationEngine

def main():
    # --- STEP 0: PRE-FLIGHT CHECKS ---
    if not os.path.exists("./input_data/TALEND_PROJECT"):
        print("[ERROR] Target folder './input_data/TALEND_PROJECT' not found.")
        sys.exit(1)

    # --- STEP 1: RUN MIGRATION ENGINE ---
    # This triggers Phase 0 (Discovery) and Phase 1 (Intelligent Generation)
    print("\n" + "="*60)
    print("[INFO] PHASE 1: AUTONOMOUS DISCOVERY & CODE GENERATION")
    print("="*60)
    
    try:
        engine = MigrationEngine()
        engine.run()
    except Exception as e:
        print(f"[FATAL] Migration failed during code generation: {e}")
        sys.exit(1)

    # --- STEP 2: START TEMPORAL WORKER ---
    # We launch this as a background process so the script can continue
    print("\n" + "="*60)
    print("[INFO] PHASE 2: INITIALIZING TEMPORAL WORKER")
    print("="*60)
    
    worker_script = os.path.join("src", "run_temporal_worker.py")
    try:
        worker_process = subprocess.Popen([sys.executable, worker_script])
        print(f"[SYSTEM] Worker started with PID: {worker_process.pid}")
        time.sleep(5) # Give the worker a moment to connect to Temporal
    except Exception as e:
        print(f"[ERROR] Failed to start Temporal worker: {e}")
        sys.exit(1)

    try:
        # --- STEP 3: TRIGGER WORKFLOW ---
        print("\n" + "="*60)
        print("[INFO] PHASE 3: TRIGGERING MIGRATION EXECUTION")
        print("="*60)
        
        trigger_script = os.path.join("src", "trigger_migration.py")
        result = subprocess.run([sys.executable, trigger_script])
        
        if result.returncode != 0:
            print("[ERROR] Failed to trigger workflow.")

        # --- STEP 4: OPEN BROWSER ---
        print("\n" + "="*60)
        print("[INFO] PHASE 4: OPENING OBSERVABILITY DASHBOARD")
        print("="*60)
        
        url = "http://localhost:8233"
        print(f"[UI] Opening {url}...")
        webbrowser.open(url)

        # --- STEP 5: KEEP ALIVE ---
        print("\n" + "="*60)
        print("[SYSTEM] STATUS: ACTIVE")
        print("="*60)
        print("   - Code Generated: ./output")
        print("   - Context Inferred: ./output/inferred_context.md")
        print("   - Worker:         RUNNING")
        print("   - Dashboard:      OPEN")
        print("\n   [PRESS CTRL+C TO STOP THE WORKER AND EXIT]")
        
        worker_process.wait()

    except KeyboardInterrupt:
        print("\n\n[INFO] Shutting down...")
        worker_process.terminate()
        sys.exit(0)

if __name__ == "__main__":
    main()