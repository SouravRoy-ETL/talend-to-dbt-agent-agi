import os
import json
import networkx as nx

class TemporalGenerator:
    def __init__(self, graph, output_dir):
        self.graph = graph
        self.output_dir = output_dir

    def generate(self):
        print("[STRATEGY] Consolidating Nested Jobs into Execution Chains...")
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 1. Identify Roots (Master Entry Points)
        roots = [n for n, d in self.graph.in_degree() if d == 0]
        
        # PATCH: Smart Filter for Project Debris
        # If we find explicit "Master" or "Main" jobs, automatically ignore unconnected orphans
        master_roots = [n for n in roots if any(x in n for x in ["Master", "Main", "Parallel"])]
        if master_roots:
            print(f"[INFO] Primary Master Roots detected: {master_roots}")
            print(f"[INFO] Filtering out {len(roots) - len(master_roots)} disconnected utility/backup jobs.")
            roots = master_roots

        tasks = []
        for root in roots:
            # 2. Build the Execution Chain for this Master hierarchy
            descendants = nx.descendants(self.graph, root)
            subgraph = self.graph.subgraph(list(descendants) + [root])
            
            try:
                # Order: Leaves (Inner-most subjobs/dependencies) -> Root (Master) last
                chain = list(reversed(list(nx.topological_sort(subgraph))))
            except nx.NetworkXUnfeasible:
                chain = list(subgraph.nodes) # Fallback if cycles exist

            chain_steps = []
            for node in chain:
                node_data = self.graph.nodes[node]
                ntype = node_data.get("type", "STANDARD")
                caps = node_data.get("capabilities", [])
                
                # RESTORED: Advanced Task Classification Logic
                task_type = "dbt_run"
                strategy = "Execute SQL Model"
                
                if "ITERATOR" in ntype:
                    task_type = "temporal_loop"
                    strategy = "Handle Iteration Logic (Python)"
                elif "TRIGGER_FILE" in ntype:
                    task_type = "file_watcher"
                    strategy = "Wait for File Event"
                elif "API" in ntype or "API" in caps:
                    task_type = "api_request"
                    strategy = "Execute REST/Socket Call"
                elif "SYSTEM" in caps:
                    task_type = "system_command"
                    strategy = "Execute OS Command"
                elif "HAS_LIFECYCLE" in caps:
                    strategy = "Execute SQL Model (With Pre/Post Hooks)"
                elif ntype == "ORCHESTRATION":
                    task_type = "workflow_group"
                    strategy = "Trigger Sub-Workflows"

                chain_steps.append({
                    "id": node,
                    "type": task_type,
                    "strategy": strategy,
                    "capabilities": caps,
                    "file": node_data.get("filepath", "")
                })

            # Consolidated Master Chain task
            tasks.append({
                "id": root,
                "type": "workflow_chain",
                "strategy": f"Master Orchestration (Consolidated {len(chain)} steps)",
                "steps": chain_steps
            })

        plan = {"tasks": tasks}
        
        with open(os.path.join(self.output_dir, "migration_plan.json"), "w", encoding='utf-8') as f:
            json.dump(plan, f, indent=2)
            
        print(f"[SUCCESS] Plan Generated. Consolidated {len(self.graph.nodes)} jobs into {len(tasks)} Master Chains.")