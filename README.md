# Talend to dbt Cybernet Agent 🧠

> **Beyond Translation: A Cognitive Engine for ETL Modernization**

![Evolution](https://img.shields.io/badge/Evolution-Gen%203%20(AGI)-violet) ![Intelligence](https://img.shields.io/badge/Logic-Semantic%20Reasoning-blue) ![Target](https://img.shields.io/badge/Target-dbt%20%2F%20Snowflake-FF694B) ![Python](https://img.shields.io/badge/Python-3.10%2B-green)

## 📖 The Paradigm Shift (Gen 3 vs. The Past)

Traditional migration scripts use static analysis (Regex/XML parsing) to map components 1:1. This fails because Talend is *procedural* (row-by-row Java loops) while dbt is *declarative* (set-based SQL).

**This repository represents the shift to "Generation 3" AGI automation.** It does not just translate code; it **re-architects** it.

| Edition | Approach | Behavior | Verdict |
| :--- | :--- | :--- | :--- |
| **Gen 1 (Scripting)** | Rigid 1:1 Mapping | Saw a `tMap`, wrote a `SELECT`. Saw a `tRunJob`, wrote a `CALL`. | **Failure:** Produced massive, unreadable SQL. Failed on custom Java. |
| **Gen 2 (LLM Assist)** | Context-Free AI | Copied snippets into LLMs. Good at explaining, bad at architecture. | **Limited:** Created circular dependencies and duplicate logic. |
| **Gen 3 (Cybernet)** | **Semantic Reasoning** | Reads XML to understand *business intent*. Refactors loops into Window Functions. | **AGI:** Autonomous, self-correcting, and architecturally aware. |

---

## ✨ Feature List
*Derived from "Talend To Dbt Agent Development" Core Architecture*

### 1. Automated Discovery & Metadata Ingestion
* **XML Parsing Engine:** Natively parses Talend `.item` files to extract component logic, context variables, and connection strings without the Studio GUI.
* **Dependency Graphing:** Automatically builds a DAG (Directed Acyclic Graph) of all jobs, identifying `tRunJob` hierarchies and circular dependencies.
* **Source-to-Target Mapping:** Scans `tMap` components to generate lineage maps showing exactly which source columns feed which targets.
* **Context Variable Resolver:** Maps Talend `Context` groups to dbt `profiles.yml` or `vars`, handling Dev/Test/Prod configs.

### 2. Intelligent Code Conversion (The AGI Core)
* **Visual-to-SQL Transpiler:** Converts visual logic (lookups, joins, filters) into clean, CTE-based SQL.
* **Java-to-Jinja Translation:** Detects embedded Java (e.g., `row1.date.equals(...)`) and rewrites it into Jinja2 or SQL functions.
* **Loop Unrolling:** Identifies procedural loops (`tFlowToIterate`) and refactors them into set-based SQL operations (e.g., `QUALIFY ROW_NUMBER()`).

### 3. Optimization & Validation
* **Episodic Refactoring:** Breaks massive Talend jobs into smaller, reusable dbt intermediate models.
* **Anti-Pattern Detection:** Flags "row-by-row" logic and suggests bulk-processing alternatives.
* **Automated Test Generation:** Creates `.yml` schema tests (unique, not_null) based on Talend schema constraints.

---

## 🚀 Why This Is "Truly AGI"
A standard script maps keywords. An AGI agent demonstrates **Semantic Reasoning**:

1.  **Intent Understanding:** The agent doesn't just look at *what* the code says ("Loop 100 times"); it understands *why* ("This is an API pagination strategy") and rewrites it as a SQL Window Function.
2.  **Self-Correction:** If the generated SQL fails compilation, the agent analyzes the error, adjusts its logic (e.g., "Snowflake doesn't support this Regex syntax"), and retries.
3.  **Handling Ambiguity:** It creates "Control Tables" to manage state (replacing Talend `GlobalMap`), bridging the gap between stateful Java and stateless SQL—a creative task usually reserved for senior engineers.

---

## ⚠️ Key Hurdles Overcome
1.  **The "Black Box" of Java:** Interpreting arbitrary Java code inside `tJavaRow` and converting it to SQL logic without human intervention.
2.  **Procedural vs. Declarative Mismatch:** Converting "Step A -> Step B" logic into a single "Select" statement without losing data integrity.
3.  **Proprietary Binary Logic:** Handling components like `tFuzzyMatch` by approximating them with SQL Levenshtein distance functions or flagging them for review.

---

## 🛠️ User Manual: Installation & Usage

### Prerequisites
* Python 3.10+
* Git (Authorized via Browser)
* A target dbt project initialized

### 1. Installation
Clone the repo and install the semantic reasoning engine.
# Clone repository
git clone [https://github.com/SouravRoy-ETL/talend-to-dbt-agent-cybernet.git](https://github.com/SouravRoy-ETL/talend-to-dbt-agent-cybernet.git)
cd talend-to-dbt-agent-cybernet

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

---

## 📚 2. Running the AGI (Cybernet - Not Humanoid)
Mode A: Discovery Scans the Talend workspace and generates a migration_plan.json.

1) Install ollama
2) ollama pull qwen2.5-coder
3) ollam serve
4) Install Temporal.io CLI & temporal server start-dev
5) python run_migration.py

---

## 💡 Examples: Before & After
Case 1: The Lookup
Before (Talend XML):

Component: tMap

Logic: row1.id matches row2.id (Inner Join)

Expression: row2.desc.toUpperCase()

After (Generated dbt SQL):

SQL - models/int_orders_enriched.sql

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
products AS (
    SELECT * FROM {{ ref('stg_products') }}
)
SELECT
    o.id,
    UPPER(p.desc) as product_desc
FROM orders o
INNER JOIN products p ON o.id = p.id

Case 2: The Loop
Before (Talend):

tFlowToIterate -> Iterate 100 times -> tMysqlInput (Select * where id = globalVar)

After (Generated dbt SQL):

SQL- The agent refactors the loop into a single bulk JOIN

SELECT
    t1.*
FROM {{ ref('source_table') }} t1
INNER JOIN {{ ref('iterator_list') }} t2
    ON t1.id = t2.id
