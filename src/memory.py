import os
import json
import hashlib

class SemanticMemory:
    """
    Long-Term Memory for the Agent.
    Stores 'Problem Context' -> 'Solved SQL' pairs.
    Allows the agent to learn from previous successes in the migration.
    """
    def __init__(self, memory_file="agent_memory.json"):
        self.memory_file = os.path.join("output", memory_file)
        self.memory = self._load_memory()

    def _load_memory(self):
        if os.path.exists(self.memory_file):
            try:
                with open(self.memory_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def _save_memory(self):
        os.makedirs(os.path.dirname(self.memory_file), exist_ok=True)
        with open(self.memory_file, 'w') as f:
            json.dump(self.memory, f, indent=2)

    def _generate_key(self, comp_type, structure_hint):
        """Creates a signature based on component type and its internal structure."""
        # We hash the structure hint to create a unique fingerprint for this logic pattern
        raw_key = f"{comp_type}::{structure_hint}"
        return hashlib.md5(raw_key.encode()).hexdigest()

    def retrieve(self, comp_type, structure_hint):
        """Recall a solution for a similar problem."""
        key = self._generate_key(comp_type, structure_hint)
        if key in self.memory:
            return self.memory[key]
        return None

    def store(self, comp_type, structure_hint, successful_sql, reasoning):
        """Memorize a successful solution."""
        key = self._generate_key(comp_type, structure_hint)
        self.memory[key] = {
            "sql": successful_sql,
            "reasoning": reasoning,
            "type": comp_type
        }
        self._save_memory()