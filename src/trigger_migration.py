import asyncio
import uuid
import os
import json
from temporalio.client import Client

PLAN_FILE = os.path.join("output", "temporal_workflows", "migration_plan.json")

def load_migration_plan():
    if not os.path.exists(PLAN_FILE):
        print(f"[ERROR] Plan not found at {PLAN_FILE}")
        return {"tasks": []}
    with open(PLAN_FILE, 'r') as f:
        return json.load(f)

async def main():
    try:
        client = await Client.connect("localhost:7233")
    except Exception as e:
        print(f"[FATAL] Could not connect to Temporal Server: {e}")
        return

    run_id = f"migration-run-{uuid.uuid4().hex[:6]}"
    
    # LOAD PLAN HERE (Outside Sandbox)
    plan_data = load_migration_plan()
    
    if not plan_data['tasks']:
        print("[ERROR] Empty migration plan. Aborting.")
        return

    print(f"[INFO] Triggering Workflow ID: {run_id}")
    print(f"[INFO] Payload: {len(plan_data['tasks'])} Master Chains loaded from disk.")
    
    try:
        handle = await client.start_workflow(
            "GenericMigrationWorkflow",
            args=[plan_data], # Pass the JSON object, not the filepath
            id=run_id,
            task_queue="talend-migration-queue",
        )

        print(f"[SUCCESS] Workflow Started! Dashboard: http://localhost:8233/namespaces/default/workflows/{run_id}")

    except Exception as e:
        print(f"[ERROR] Trigger failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())