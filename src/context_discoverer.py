import os
import glob
import json
from lxml import etree
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from src.config import INPUT_DIR, PROJECT_CONTEXT_FILE, OLLAMA_MODEL, OLLAMA_BASE_URL

DISCOVERY_PROMPT = """
YOU ARE A LEAD ARCHITECT AUDITING A LEGACY TALEND PROJECT.
Identify recurring patterns, deprecations, and business rules.

INPUT DATA:
{snippets}

OUTPUT FORMAT (Markdown):
# INFERRED PROJECT CONTEXT
...
"""

class ContextDiscoverer:
    def __init__(self):
        self.llm = ChatOllama(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL, temperature=0.1)

    def run(self):
        print(f"[DISCOVERY] Agent is auditing the legacy codebase...")
        try:
            evidence = self._harvest_evidence()
            snippets_str = json.dumps(evidence[:50], indent=2) if evidence else "No patterns found."

            prompt = ChatPromptTemplate.from_template(DISCOVERY_PROMPT)
            chain = prompt | self.llm
            response = chain.invoke({"snippets": snippets_str})
            
            self._save_context(response.content)
            print(f"[SUCCESS] Context inferred and saved.")
            
        except Exception as e:
            print(f"[ERROR] Discovery failed: {e}")
            self._save_context("- Discovery failed. Proceeding with defaults.")

    def _harvest_evidence(self):
        evidence = []
        # Recursive scan
        files = glob.glob(os.path.join(INPUT_DIR, "**", "*.item"), recursive=True)
        
        for f in files:
            try:
                # Force parser to recover from encoding errors
                parser = etree.XMLParser(recover=True, encoding='utf-8')
                tree = etree.parse(f, parser=parser)
                
                # Scrape values
                exprs = tree.xpath("//elementParameter/@value")
                for e in exprs:
                    if len(str(e)) > 10 and ("SELECT" in str(e).upper() or "row" in str(e)):
                        evidence.append(str(e)[:100])
            except Exception: 
                continue # Skip unreadable files silently
                
        return list(set(evidence))

    def _save_context(self, content):
        os.makedirs(os.path.dirname(PROJECT_CONTEXT_FILE), exist_ok=True)
        # FIX: Force UTF-8 encoding to prevent Windows charmap errors
        with open(PROJECT_CONTEXT_FILE, "w", encoding="utf-8", errors="ignore") as f:
            f.write(content)