import asyncio
import json
import os
from datetime import timedelta
from temporalio import workflow, activity
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.common import RetryPolicy

# --- ACTIVITIES ---

@activity.defn
async def dbt_run_activity(model: str) -> str:
    print(f"   [ACTIVITY] Running dbt model: {model}")
    return f"SUCCESS: {model}"

@activity.defn
async def file_watcher_activity(path: str) -> str:
    print(f"   [ACTIVITY] Checking for file: {path}")
    return "FOUND"

@activity.defn
async def loop_activity(job_id: str) -> str:
    print(f"   [ACTIVITY] Processing Iterator: {job_id}")
    return "LOOP_DONE"

@activity.defn
async def api_activity(job_id: str) -> str:
    print(f"   [ACTIVITY] Executing API/Socket Call: {job_id}")
    return "API_SUCCESS"

@activity.defn
async def system_activity(job_id: str) -> str:
    print(f"   [ACTIVITY] Executing System Command: {job_id}")
    return "SYS_SUCCESS"

@activity.defn
async def suppress_db_warning() -> str:
    """Helper to silently handle missing DB connections/warnings."""
    return "DB_WARNINGS_SUPPRESSED"

# --- WORKFLOW (DAG Visible in Dashboard) ---

@workflow.defn
class GenericMigrationWorkflow:
    @workflow.run
    async def run(self, plan_data: dict) -> str:
        # FIX: Validate payload to prevent KeyError: 'tasks'
        if not plan_data or 'tasks' not in plan_data:
            raise ValueError("Input data must contain a 'tasks' key.")

        print(f"[WORKFLOW] Received {len(plan_data['tasks'])} Master Chains.")
        results = []
        
        for task in plan_data['tasks']:
            print(f"\n[MASTER] Starting Chain: {task['id']}")
            
            if task['type'] == 'workflow_chain':
                steps = task.get('steps', [])
                print(f"         Chain contains {len(steps)} sequential steps.")
                
                # Execute DAG steps in derived order
                for step in steps:
                    rp = RetryPolicy(maximum_attempts=3)
                    
                    if step['type'] == 'dbt_run':
                        await workflow.execute_activity(
                            dbt_run_activity, args=[step['id']], 
                            start_to_close_timeout=timedelta(minutes=10), retry_policy=rp
                        )
                    elif step['type'] == 'temporal_loop':
                        await workflow.execute_activity(
                            loop_activity, args=[step['id']], 
                            start_to_close_timeout=timedelta(minutes=10), retry_policy=rp
                        )
                    elif step['type'] == 'file_watcher':
                        await workflow.execute_activity(
                            file_watcher_activity, args=[step['id']], 
                            start_to_close_timeout=timedelta(minutes=5), retry_policy=rp
                        )
                    elif step['type'] == 'api_request':
                        await workflow.execute_activity(
                            api_activity, args=[step['id']], 
                            start_to_close_timeout=timedelta(minutes=5), retry_policy=rp
                        )
                    elif step['type'] == 'system_command':
                        await workflow.execute_activity(
                            system_activity, args=[step['id']], 
                            start_to_close_timeout=timedelta(minutes=5), retry_policy=rp
                        )
            
            results.append(f"Chain {task['id']} Complete")

        return f"Migration Complete. {len(results)} Chains Executed."

async def main():
    print("[INFO] Starting CHAIN-AWARE Temporal Worker...")
    try:
        client = await Client.connect("localhost:7233")
    except Exception as e:
        print(f"[FATAL] Connection failed: {e}")
        return

    worker = Worker(
        client,
        task_queue="talend-migration-queue",
        workflows=[GenericMigrationWorkflow],
        activities=[dbt_run_activity, file_watcher_activity, loop_activity, api_activity, system_activity, suppress_db_warning],
    )

    print("[INFO] Worker listening. DAG execution will be visible in Temporal UI.")
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())