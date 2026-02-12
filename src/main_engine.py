import os
import json
import re
import networkx as nx
from concurrent.futures import ThreadPoolExecutor, as_completed
from lxml import etree
from src.config import INPUT_DIR, DBT_OUTPUT_DIR, TEMPORAL_OUTPUT_DIR
from src.graph_builder import ProjectGraph
from src.context_parser import ContextParser
from src.context_discoverer import ContextDiscoverer
from src.agent_llm import SouravAgent
from src.temporal_generator import TemporalGenerator
from src.knowledge_base import KnowledgeRetriever
from src.memory import SemanticMemory

MAX_WORKERS = 8  

class MigrationEngine:
    def __init__(self):
        self.graph_builder = ProjectGraph(INPUT_DIR)
        self.context_parser = ContextParser()
        self.kb = KnowledgeRetriever()
        self.memory = SemanticMemory()
        self.agent = None 

    def run(self):
        print("="*60)
        print("[INFO] STARTING TALEND-TO-DBT-SOURAV-AGENT (AGI-CYBERNET-MODE)")
        print("="*60)
        
        discoverer = ContextDiscoverer()
        discoverer.run()
        
        self.agent = SouravAgent()
        self.context_parser.parse()
        graph = self.graph_builder.build()
        TemporalGenerator(graph, TEMPORAL_OUTPUT_DIR).generate()
        self._generate_dbt_assets_parallel(graph)

    def _generate_dbt_assets_parallel(self, graph):
        os.makedirs(os.path.join(DBT_OUTPUT_DIR, "models"), exist_ok=True)
        os.makedirs(os.path.join(DBT_OUTPUT_DIR, "macros"), exist_ok=True)
        
        tasks = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for job in graph.nodes:
                node_data = graph.nodes[job]
                # Skip orchestration jobs (handled by Temporal) or jobs without files
                if node_data.get('type') == 'ORCHESTRATION' or 'filepath' not in node_data:
                    continue
                
                # Also skip Iterators/Triggers from SQL gen as they are temporal tasks
                if node_data.get('type') in ['ITERATOR', 'FILE_TRIGGER']:
                     continue

                # PATCH: Reconstruct Absolute Path for File Reading
                # The graph now stores relative paths, so we must join with INPUT_DIR
                full_path = os.path.join(INPUT_DIR, node_data['filepath'])

                if node_data.get('type') == 'JOBLET':
                    future = executor.submit(self._convert_joblet, job, full_path)
                else:
                    future = executor.submit(self._convert_job_chain, job, full_path)
                tasks.append(future)

            for future in as_completed(tasks):
                try:
                    future.result()
                except Exception as e:
                    print(f"   [ERROR] Task failed: {e}")

    def _convert_joblet(self, name, filepath):
        sql = self._process_xml_chain(name, filepath)
        if sql:
            path = os.path.join(DBT_OUTPUT_DIR, "macros", f"{name}.sql")
            with open(path, "w", encoding='utf-8') as f:
                f.write(f"{{% macro {name}() %}}\n/* Joblet: {name} */\n{sql}\n{{% endmacro %}}")

    def _convert_job_chain(self, name, filepath):
        raw_sql = self._process_xml_chain(name, filepath)
        if raw_sql:
            path = os.path.join(DBT_OUTPUT_DIR, "models", f"{name}.sql")
            with open(path, "w", encoding='utf-8') as f:
                f.write(f"-- Migrated Job: {name}\nWITH \n{raw_sql}\n\nSELECT * FROM final_cte")

    def _process_xml_chain(self, job_name, filepath):
        try:
            # FIX: Robust Parser
            parser = etree.XMLParser(recover=True)
            tree = etree.parse(filepath, parser=parser)
            root = tree.getroot()
            
            # 1. Identify Nodes using Python Iteration (No XPath Predicates)
            comp_map = {}
            for elem in root.iter():
                if "node" in elem.tag:
                    u_name = None
                    for child in elem:
                        if "elementParameter" in child.tag and child.get("name") == "UNIQUE_NAME":
                            u_name = child.get("value")
                            break
                    if u_name:
                        comp_map[u_name] = elem

            # 2. Build Internal Graph
            internal_graph = nx.DiGraph()
            conn_map = {}
            
            for elem in root.iter():
                if "connection" in elem.tag:
                    src = elem.get("source")
                    tgt = elem.get("target")
                    name = elem.get("name") or elem.get("label") or "row"
                    if src and tgt:
                        internal_graph.add_edge(src, tgt)
                        conn_map[name] = src
                        conn_map[f"{src}->{tgt}"] = name

            try:
                execution_order = list(nx.topological_sort(internal_graph))
            except:
                execution_order = list(comp_map.keys())

            cte_list = []
            cte_registry = {} 
            
            # EXPANDED EXCLUSION LIST FOR ORCHESTRATORS
            ORCHESTRATION_COMPONENTS = [
                "tPrejob", "tPostjob", "tRunJob", "tLoop", "tForeach", "tInfiniteLoop", 
                "tFlowToIterate", "tIterateToFlow", "tFileList", "tWaitForFile", 
                "tWaitForSocket", "tWaitForSqlData", "tSleep", "tDie", "tWarn", 
                "tLogCatcher", "tStatCatcher", "tAssert", "tAssertCatcher", 
                "tParallelize", "tPartitioner", "tCollector", "tDepartitioner", 
                "tRecollector", "tReplicate", "tUnite", "tBarrier", "tSystem", 
                "tChronometerStart", "tChronometerStop", "tFlowMeter", "tFlowMeterCatcher", 
                "tMoment", "tMsgBox", "tSocketInput", "tSocketOutput", "tRest", 
                "tRestClient", "tRouteInput", "tRouteOutput"
            ]

            for comp_id in execution_order:
                if comp_id not in comp_map: continue
                node = comp_map[comp_id]
                comp_type = node.get("componentName")
                
                # *** GUARDRAIL: Skip Logic-Only Components ***
                if comp_type in ORCHESTRATION_COMPONENTS:
                    # Special logging for debugging
                    if comp_type in ["tPrejob", "tPostjob"]:
                        print(f"   [INFO] Lifecycle Hook {comp_type} detected. Handled by Temporal.")
                    continue
                
                # Manual serialization to string for Agent
                xml_str = etree.tostring(node).decode('utf-8')

                predecessors = list(internal_graph.predecessors(comp_id))
                input_ctes = [cte_registry[p] for p in predecessors if p in cte_registry]
                
                prev_cte_context = []
                for p in predecessors:
                    if p in cte_registry:
                        flow_name = conn_map.get(f"{p}->{comp_id}", "unknown")
                        prev_cte_context.append(f"Flow '{flow_name}' is CTE '{cte_registry[p]}'")
                
                prev_cte_str = " | ".join(prev_cte_context) if prev_cte_context else "dual"

                if "Input" in comp_type:
                    cte_sql = self._handle_input_component(node, comp_id)
                    cte_list.append(cte_sql)
                    cte_registry[comp_id] = comp_id
                    continue
                
                if "Output" in comp_type: continue

                # Deep Scan
                if "tMap" in comp_type:
                    dna_summary = self._scan_tmap_deep(node)
                else:
                    dna_summary = self._universal_recursive_introspector(node, comp_type)
                
                input_map = { "input_ctes": input_ctes, "structure_hint": dna_summary }
                
                result = self.agent.convert_component(job_name, comp_type, xml_str, prev_cte_str, json.dumps(input_map))
                
                if result.get('status') == 'SUCCESS':
                    clean_sql = result['sql_logic'].strip().rstrip(';')
                    cte_list.append(f"{comp_id} AS (\n {clean_sql} \n)")
                    cte_registry[comp_id] = comp_id
            
            if not cte_list: return None
            final_cte = execution_order[-1] if execution_order else "dual"
            if final_cte not in cte_registry: final_cte = cte_list[-1].split(" AS")[0]
            return ",\n".join(cte_list) + f",\nfinal_cte AS ( SELECT * FROM {final_cte} )"
            
        except Exception as e:
            print(f"[WARN] Error processing {job_name}: {e}")
            return None

    def _handle_input_component(self, node, comp_id):
        params = {}
        for child in node:
            if "elementParameter" in child.tag:
                params[child.get("name")] = child.get("value")
        
        query = params.get("QUERY", "").replace('"', '').replace('\\', '')
        if len(query) > 10:
            query = re.sub(r'context\.(\w+)', r"{{ var('\1') }}", query).replace(' + ', '').strip()
            return f"{comp_id} AS ( \n    {query} \n)"
        table = params.get("TABLE", f"src_{comp_id}").replace('"', '')
        return f"{comp_id} AS ( SELECT * FROM {{{{ source('raw', '{table}') }}}} )"

    def _scan_tmap_deep(self, node):
        dna = ["COMPONENT: tMap"]
        # Python Iteration for tMap structure
        node_data = None
        for child in node:
            if "nodeData" in child.tag:
                node_data = child
                break
        
        if node_data is not None:
            for tbl in node_data:
                if "inputTables" in tbl.tag:
                    name = tbl.get("name")
                    join = tbl.get("joinType") or "CROSS"
                    keys = []
                    for entry in tbl:
                        if "mapperTableEntries" in entry.tag and entry.get("expression"):
                            keys.append(f"{name}.{entry.get('name')} = {entry.get('expression')}")
                    dna.append(f"INPUT [{name}]: Join={join} ON {' AND '.join(keys)}")
                
                if "outputTables" in tbl.tag:
                    name = tbl.get("name")
                    dna.append(f"\nOUTPUT_TARGET [{name}]:")
                    for entry in tbl:
                        if "mapperTableEntries" in entry.tag:
                            col = entry.get("name")
                            expr = entry.get("expression")
                            if expr:
                                expr = expr.replace("Relational.ISNULL", "IS NULL")
                                dna.append(f"   - {col} := {expr}")
                            else:
                                dna.append(f"   - {col} := NULL")
        return "\n".join(dna)

    def _universal_recursive_introspector(self, node, comp_type):
        dna = [f"COMPONENT: {comp_type}"]
        for child in node:
            if "elementParameter" in child.tag:
                n = child.get("name")
                v = child.get("value")
                if n and v and "COLOR" not in n: dna.append(f"{n}={v}")
        return "\n".join(dna[:50])