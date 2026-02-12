import re

class KnowledgeRetriever:
    """
    THE TALEND ENCYCLOPEDIA (v8.0.1 COMPATIBLE)
    
    Acts as the 'Rosetta Stone' for the agent, providing:
    1. Deterministic mapping of Talend Components to SQL/dbt Logic.
    2. Translation patterns for Java Routines (TalendDate, StringHandling) to DuckDB SQL.
    """
    def __init__(self):
        # ==============================================================================
        #  1. JAVA ROUTINE ROSETTA STONE (Dynamic SQL Mapping)
        # ==============================================================================
        # PATCH: All patterns use double braces {{arg}} to prevent LangChain variable collision.
        self.function_map = {
            # --- TalendDate ---
            "TalendDate.getCurrentDate": "current_date",
            "TalendDate.getDate": "current_timestamp",
            "TalendDate.formatDate": "strftime({{arg2}}, {{arg1}})", 
            "TalendDate.parseDate": "strptime({{arg2}}, {{arg1}})",
            "TalendDate.addDate": "{{arg1}} + INTERVAL {{arg2}} {{arg3}}",
            "TalendDate.diffDate": "date_diff({{arg3}}, {{arg2}}, {{arg1}})",
            "TalendDate.isDate": "try_cast({{arg1}} as DATE) IS NOT NULL",
            "TalendDate.compareDate": "CASE WHEN {{arg1}} < {{arg2}} THEN -1 WHEN {{arg1}} > {{arg2}} THEN 1 ELSE 0 END",
            "TalendDate.getFirstDayOfMonth": "date_trunc('month', {{arg1}})",
            "TalendDate.getLastDayOfMonth": "last_day({{arg1}})",
            "TalendDate.getPartOfDate": "extract({{arg1}} from {{arg2}})",
            
            # --- StringHandling ---
            "StringHandling.UPCASE": "upper({{arg1}})",
            "StringHandling.DOWNCASE": "lower({{arg1}})",
            "StringHandling.ALPHA": "regexp_matches({{arg1}}, '^[a-zA-Z]+$')",
            "StringHandling.CHANGE": "replace({{arg1}}, {{arg2}}, {{arg3}})",
            "StringHandling.COUNT": "(length({{arg1}}) - length(replace({{arg1}}, {{arg2}}, '')))",
            "StringHandling.EREPLACE": "regexp_replace({{arg1}}, {{arg2}}, {{arg3}})",
            "StringHandling.INDEX": "strpos({{arg1}}, {{arg2}}) - 1",
            "StringHandling.LEFT": "left({{arg1}}, {{arg2}})",
            "StringHandling.RIGHT": "right({{arg1}}, {{arg2}})",
            "StringHandling.LEN": "length({{arg1}})",
            "StringHandling.TRIM": "trim({{arg1}})",
            
            # --- Mathematical ---
            "Mathematical.ABS": "abs({{arg1}})",
            "Mathematical.INT": "cast({{arg1}} as INTEGER)",
            "Mathematical.SQRT": "sqrt({{arg1}})",
            "Mathematical.POW": "power({{arg1}}, {{arg2}})",
            "Mathematical.SMUL": "({{arg1}} * {{arg2}})",
            "Mathematical.SDIV": "({{arg1}} / nullif({{arg2}}, 0))",
            
            # --- Relational & Numeric ---
            "Relational.ISNULL": "({{arg1}} IS NULL)",
            "Relational.NOTISNULL": "({{arg1}} IS NOT NULL)",
            "Numeric.sequence": "row_number() OVER (ORDER BY (SELECT NULL))",
            "Numeric.random": "random()",
            
            # --- DataOperation ---
            "DataOperation.CHAR": "chr({{arg1}})",
            "DataOperation.DT": "cast({{arg1}} as timestamp)",
            
            # --- BigDecimal ---
            "BigDecimal.ROUND": "round({{arg1}}, {{arg2}})",
        }

        # ==============================================================================
        #  2. COMPONENT LOGIC MAP (Behavioral Categories)
        # ==============================================================================
        self.special_rules = {
            # --- TRANSFORMATIONS ---
            "tMap": "RULE: [CTE] Multi-input Mapper. Use LEFT/INNER JOIN for lookups. Map expressions.",
            "tXMLMap": "RULE: [CTE] Hierarchical Mapper. Use xpath() functions in SELECT mappings.",
            "tFilterRow": "RULE: [CTE] Filter. Convert Java conditions to SQL WHERE.",
            "tFilterColumns": "RULE: [CTE] Projection. Select only specified columns.",
            "tAggregateRow": "RULE: [CTE] Aggregator. GROUP BY logic with SUM/MIN/MAX/AVG.",
            "tSortRow": "RULE: [CTE] Sorter. Generate ORDER BY clause.",
            "tUniqRow": "RULE: [CTE] Deduplicator. Use QUALIFY ROW_NUMBER() OVER(PARTITION BY [keys] ORDER BY [order]) = 1.",
            "tJoin": "RULE: [CTE] Joiner. Generate standard INNER/LEFT JOIN syntax.",
            "tUnite": "RULE: [CTE] Union. Use UNION ALL to combine datasets.",
            "tNormalize": "RULE: [CTE] Unpivoter. Use UNNEST(string_split(column, delimiter)).",
            "tDenormalize": "RULE: [CTE] Pivoter. Use string_agg(column, delimiter).",
            "tReplace": "RULE: [CTE] Cleaner. Use replace() or regexp_replace().",
            "tConvertType": "RULE: [CTE] Caster. Use CAST(column AS type).",
            
            # --- JSON/XML ---
            "tExtractJSONFields": "RULE: [CTE] JSON Parse. Use json_extract(column, path).",
            "tExtractXMLField": "RULE: [CTE] XML Parse. Use unnest(xpath(column, path)).",
            
            # --- INPUTS (PATCHED for Query Extraction) ---
            "tMysqlInput": "RULE: [SOURCE] Check for 'QUERY' parameter first. If found, preserve SQL. Else use {{ source() }}.",
            "tPostgresqlInput": "RULE: [SOURCE] Check for 'QUERY' parameter. Preserve SQL logic.",
            "tMSSqlInput": "RULE: [SOURCE] Check for 'QUERY' parameter. Preserve SQL logic.",
            "tDBInput": "RULE: [SOURCE] Database Input. MUST EXTRACT 'QUERY' element and preserve SQL.",
            "tFileInputDelimited": "RULE: [SOURCE] CSV. Use {{ source('files', 'csv_name') }}.",
            
            # --- ORCHESTRATION (Temporal Handlers) ---
            "tLoop": "RULE: [ORCH] Loop. Handled by Temporal Workflow logic, NOT SQL.",
            "tRunJob": "RULE: [ORCH] Subjob. Handled by Temporal ChildWorkflow, NOT SQL.",
            "tParallelize": "RULE: [ORCH] Parallelism. Handled by Temporal asyncio.gather.",
            "tDie": "RULE: [ORCH] Error. Handled by Temporal RetryPolicy/Exceptions."
        }

    def get_context(self, comp_type: str, xml_snippet: str) -> dict:
        """
        Generates a RAG context for the LLM based on component type and XML content.
        Detects specific Java functions used in the XML to provide targeted SQL translations.
        """
        # A. Behavioral Rule Retrieval
        rag_hint = self.special_rules.get(comp_type)
        if not rag_hint:
            # Generic Category Fallbacks
            if comp_type.endswith("Input"):
                rag_hint = "RULE: [SOURCE] Database/SaaS. Check XML for 'QUERY'. Use it if present. Else map to {{ source() }}."
            elif comp_type.endswith("Output") or comp_type.endswith("Put"):
                rag_hint = "RULE: [TARGET] Data Sink. This CTE defines the final materialization."
            elif any(x in comp_type for x in ["Loop", "FileList", "RunJob", "Wait", "Sleep", "Die", "Warn", "Exist"]):
                rag_hint = "RULE: [ORCH] Control Flow. Handled by Temporal Workflow, ignore in dbt SQL logic."
            elif any(x in comp_type for x in ["Connection", "Commit", "Rollback", "Close"]):
                rag_hint = "RULE: [IGNORE] dbt handles transactions automatically via profiles.yml."
            else:
                rag_hint = "RULE: [GENERIC] Analyze XML logic and wrap in a dbt CTE."

        # B. Dynamic Function Extraction (The "Rosetta Stone" Logic)
        detected_funcs = []
        for func, sql in self.function_map.items():
            # Check for "TalendDate.formatDate" or just "formatDate" usage
            if func in xml_snippet or ('.' in func and func.split(".")[1] in xml_snippet):
                detected_funcs.append(f"- JAVA: {func}(...) -> SQL: {sql}")
        
        function_context = ""
        if detected_funcs:
            function_context = "\n\nDETECTED JAVA FUNCTIONS (Use these SQL mappings):\n" + "\n".join(detected_funcs)
        
        return {
            "rag": f"{rag_hint}{function_context}"
        }

    def get_macro_conversion(self, java_snippet):
        """
        Helper to explicitly translate a raw Java snippet if needed outside the LLM.
        """
        for java_func, sql_pattern in self.function_map.items():
            if java_func in java_snippet:
                return f"Detected {java_func} -> Use SQL: {sql_pattern}"
        return None