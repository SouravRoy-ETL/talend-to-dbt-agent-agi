import os
import json
import re
import duckdb
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from src.config import OLLAMA_MODEL, OLLAMA_BASE_URL, PROJECT_CONTEXT_FILE
from src.knowledge_base import KnowledgeRetriever
from src.memory import SemanticMemory

# --- PERSONA: THE DECISIVE DATA ENGINEER ---
ENGINEER_PROMPT = """
YOU ARE A SENIOR DATA ENGINEER MIGRATING TALEND TO DUCKDB.

### YOUR GOAL
Generate a single, valid DuckDB SQL `SELECT` statement (inside a CTE) that implements the logic described in the DNA.

### CRITICAL RULES
1. **RESOLVE MAPPINGS**:
   - The DNA uses FLOW names (e.g., `row1`, `row2`).
   - The INPUTS list maps FLOW names to CTE names (e.g., `Flow 'row1' is CTE 'tDBInput_1'`).
   - **ACTION**: You MUST replace Flow Names with CTE Names in your SQL.
   - *Example*: If DNA says `row1.id = row2.id`, you write `tDBInput_1.id = tDBInput_2.id`.

2. **STRUCTURE**:
   - The first input in the DNA is usually the `FROM` table.
   - Subsequent inputs are `LEFT JOIN` (lookups).
   - Use the specific JOIN KEYS defined in the DNA `TRANSFORMATION_LOGIC`.

3. **SELECT COLUMNS**:
   - Do NOT use `SELECT *`. 
   - Explicitly select and alias columns as defined in the `OUTPUT_TARGET` section of the DNA.
   - Use `{{arg}}` style replacements for Java functions as defined in the RULES.

### OUTPUT FORMAT
Return ONLY the JSON object. No markdown, no conversational text.
{{
  "status": "SUCCESS",
  "reasoning": "Mapped tDBInput_1 to tDBInput_2 on ID.",
  "sql_logic": "SELECT t1.col AS output_col FROM tDBInput_1 AS t1 LEFT JOIN tDBInput_2 AS t2 ON t1.id = t2.id",
  "issues": []
}}
"""

class SouravAgent:
    def __init__(self):
        self.llm = ChatOllama(
            model=OLLAMA_MODEL,
            base_url=OLLAMA_BASE_URL,
            temperature=0.1, 
            num_ctx=16384, # High context for complex joins
            keep_alive=-1,
            num_thread=16,
            num_gpu=-1
        )
        self.kb = KnowledgeRetriever()
        self.memory = SemanticMemory()
        self.project_context = self._load_discovered_context()

    def _load_discovered_context(self):
        if os.path.exists(PROJECT_CONTEXT_FILE):
            try:
                with open(PROJECT_CONTEXT_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                    return f.read()
            except:
                pass
        return "NO CONTEXT DISCOVERED."

    def convert_component(self, job_name, comp_type, xml_snippet, prev_cte_context, input_map_json):
        input_data = json.loads(input_map_json)
        dna_summary = input_data.get("structure_hint", "")
        context = self.kb.get_context(comp_type, xml_snippet)
        
        past_solution = self.memory.retrieve(comp_type, dna_summary)
        memory_txt = f"REUSED PATTERN: {past_solution['sql']}" if past_solution else ""

        prompt = ChatPromptTemplate.from_messages([
            ("system", ENGINEER_PROMPT),
            ("human", """
            *** PROJECT CONTEXT ***
            {project_context}
            
            JOB: {job_name} | COMPONENT: {comp_type}
            
            === 1. INPUTS (Flow Mapping) ===
            {inputs}
            
            === 2. DNA (Logic Mapping) ===
            {dna}
            
            === 3. RULES (Knowledge Base) ===
            {kb_rules}
            
            {memory}
            """) 
        ])
        
        try:
            print(f"   ... [QWEN] Migrating {comp_type} ...")
            chain = prompt | self.llm
            response = chain.invoke({
                "project_context": self.project_context,
                "job_name": job_name,
                "comp_type": comp_type,
                "inputs": prev_cte_context,
                "dna": dna_summary,
                "kb_rules": context.get('rag', ''),
                "memory": memory_txt
            })
            
            raw = response.content.strip()
            
            # Direct JSON Extraction (No thinking block parsing)
            # Find the first { and last }
            idx_start = raw.find('{')
            idx_end = raw.rfind('}')
            
            if idx_start != -1 and idx_end != -1:
                try:
                    result = json.loads(raw[idx_start:idx_end+1])
                except:
                     # Fallback if Qwen produced broken JSON
                    result = {"status": "SUCCESS", "sql_logic": f"/* JSON PARSE FAIL - CHECK LOGS */\n{raw}", "issues": ["JSON_ERR"]}
            else:
                result = {"status": "SUCCESS", "sql_logic": f"/* NO JSON FOUND */\n{raw}", "issues": ["NO_JSON"]}

            final_result = self._self_correct_with_simulation(result, prev_cte_context)
            
            if not final_result.get('issues'):
                self.memory.store(comp_type, dna_summary, final_result['sql_logic'], "Direct Translation")
                
            return final_result

        except Exception as e:
            print(f"   [ERROR] Agent failed: {e}")
            return {"status": "ERROR", "sql_logic": f"SELECT * FROM {prev_cte_context.split('|')[0].strip()}", "issues": [str(e)]}

    def _self_correct_with_simulation(self, result, inputs):
        """
        Runs a 'Loose' Simulation.
        It checks for Syntax Errors but ignores 'Column Not Found' errors 
        because our Mock Tables don't have the full schema from Talend.
        """
        sql = result.get("sql_logic", "")
        if not sql or "SELECT" not in str(sql).upper(): return result
        
        con = duckdb.connect(":memory:")
        try:
            # 1. Parse Inputs to create Dummy Tables
            # Input format: "Flow 'row1' is CTE 'tDBInput_1' | ..."
            
            # Regex to find CTE names inside single quotes after CTE
            cte_names = re.findall(r"CTE '(\w+)'", str(inputs))
            
            # Fallback for simple single inputs or "dual"
            if not cte_names and "CTE" not in str(inputs) and str(inputs) != "dual":
                 cte_names = [str(inputs).strip()]
            
            for table_name in cte_names:
                if table_name and table_name != "dual":
                    # Create a dummy table with generic columns + commonly used keys
                    con.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} AS 
                        SELECT 
                            1 as BusinessEntityID, 
                            1 as id, 
                            1 as key,
                            'test' as name, 
                            'test' as label,
                            '2023-01-01'::DATE as date_col
                    """)

            # 2. Clean SQL for Simulation (Handle Jinja)
            sim_sql = re.sub(r"\{\{.*?\}\}", "mock_src", sql)
            sim_sql = sim_sql.replace("source", "ref").replace("var", "ref")
            
            # 3. Syntax Check Only
            con.execute(f"EXPLAIN SELECT * FROM ({sim_sql})")
            
            return result

        except Exception as e:
            err_msg = str(e).lower()
            # If it's just a column missing (which is expected with mocks), we ignore it.
            if "binder error" in err_msg or "not found" in err_msg:
                return result 
            
            # Real Syntax Errors (e.g. missing comma, bad keyword)
            result['issues'] = result.get('issues', []) + [f"Syntax Error: {str(e)}"]
            return result