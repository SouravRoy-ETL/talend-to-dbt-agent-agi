import os
import glob
import re
import networkx as nx
from lxml import etree

class ProjectGraph:
    def __init__(self, input_dir):
        self.input_dir = input_dir
        self.graph = nx.DiGraph()
        self.job_map = {} # Normalized Name -> Original Label
        
        # DEFINITIVE ORCHESTRATION MAP (40+ TYPES)
        self.component_map = {
            # Lifecycle
            "tPrejob": "LIFECYCLE_PRE", "tPostjob": "LIFECYCLE_POST",
            # Subjobs
            "tRunJob": "SUBJOB",
            # Iterators
            "tLoop": "ITERATOR", "tForeach": "ITERATOR", "tInfiniteLoop": "ITERATOR",
            "tFlowToIterate": "ITERATOR", "tIterateToFlow": "ITERATOR",
            # Triggers / Watchers
            "tFileList": "TRIGGER_FILE", "tWaitForFile": "TRIGGER_FILE", 
            "tWaitForSocket": "TRIGGER_NET", "tWaitForSqlData": "TRIGGER_DB",
            # Control / Status
            "tSleep": "CONTROL", "tDie": "CONTROL", "tWarn": "CONTROL", "tMsgBox": "CONTROL",
            "tAssert": "TEST", "tAssertCatcher": "OBSERVABILITY",
            "tLogCatcher": "OBSERVABILITY", "tStatCatcher": "OBSERVABILITY", 
            "tFlowMeter": "OBSERVABILITY", "tFlowMeterCatcher": "OBSERVABILITY",
            "tChronometerStart": "OBSERVABILITY", "tChronometerStop": "OBSERVABILITY",
            # Parallelism / Synchronization
            "tParallelize": "PARALLEL", "tBarrier": "BARRIER", 
            "tPartitioner": "PARALLEL", "tCollector": "PARALLEL", "tDepartitioner": "PARALLEL",
            "tRecollector": "PARALLEL", "tReplicate": "PARALLEL", "tUnite": "PARALLEL",
            # Connectivity / API
            "tSystem": "SYSTEM", "tRest": "API", "tRestClient": "API", 
            "tSocketInput": "API", "tSocketOutput": "API",
            "tRouteInput": "ROUTE", "tRouteOutput": "ROUTE"
        }

    def _normalize(self, name):
        """
        STRICT NORMALIZATION: Bridges 'Product_SubJobs_0.1' and 'productsubjobs'.
        Ensures internal calls find physical files.
        """
        if not name: return ""
        # PATCH: Strip Project Prefix (e.g., 'LOCAL_PROJECT:Job' -> 'Job')
        if ":" in name:
            name = name.split(":")[-1]
        # Remove versioning (_0.1, _1.0) and all non-alphanumeric chars
        name = re.sub(r'_\d+\.\d+$', '', name)
        return re.sub(r'[^a-z0-9]', '', name.lower())

    def build(self):
        print(f"[GRAPH] Scanning {self.input_dir} for Job Hierarchy...")
        files = glob.glob(os.path.join(self.input_dir, "**", "*.item"), recursive=True)
        
        # 1. Map Files & Detect Capabilities
        for f in files:
            try:
                parser = etree.XMLParser(recover=True)
                tree = etree.parse(f, parser=parser)
                
                # Extract the real label from properties
                real_label = os.path.basename(f).replace(".item", "")
                for elem in tree.iter():
                    if "Property" in elem.tag and elem.get("label"):
                        real_label = elem.get("label")
                        break
                
                norm_name = self._normalize(real_label)
                self.job_map[norm_name] = real_label
                
                capabilities = set()
                primary_type = "STANDARD"
                
                for node in tree.iter():
                    if "node" in node.tag:
                        ctype = node.get("componentName")
                        if ctype in self.component_map:
                            cat = self.component_map[ctype]
                            capabilities.add(cat)
                            if cat in ["ITERATOR", "TRIGGER_FILE", "API"]:
                                primary_type = cat
                            elif cat in ["SUBJOB", "PARALLEL"]:
                                primary_type = "ORCHESTRATION"
                
                # PATCH: Use Relative Paths to fix "C:\\Users..." verbosity
                clean_path = os.path.relpath(f, self.input_dir)

                self.graph.add_node(real_label, filepath=clean_path, type=primary_type, 
                                   capabilities=list(capabilities), norm_id=norm_name, is_subjob=False)
            except Exception: continue

        # 2. Link Dependencies (Universal Hierarchy Linker)
        for node in list(self.graph.nodes):
            self._link_dependencies(node)

        # 3. Mark Subjobs (Nodes with incoming edges are NOT roots)
        for node in self.graph.nodes:
            if self.graph.in_degree(node) > 0:
                self.graph.nodes[node]['is_subjob'] = True

        roots = [n for n, d in self.graph.nodes(data=True) if not d.get('is_subjob')]
        print(f"[GRAPH] Hierarchy Built. Roots: {len(roots)} Master Chains. Total Nodes: {len(self.graph.nodes)}")
        return self.graph

    def _link_dependencies(self, parent_label):
        # PATCH: Reconstruct full path from relative path
        filepath = os.path.join(self.input_dir, self.graph.nodes[parent_label]['filepath'])
        try:
            tree = etree.parse(filepath, parser=etree.XMLParser(recover=True))
            root = tree.getroot()

            def link_child(child_raw):
                child_norm = self._normalize(child_raw)
                child_label = next((n for n, d in self.graph.nodes(data=True) if d['norm_id'] == child_norm), None)
                if child_label and child_label != parent_label:
                    if not self.graph.has_edge(parent_label, child_label):
                        self.graph.add_edge(parent_label, child_label)
                        print(f"   [LINK] {parent_label} -> {child_label}")

            for node in root.iter():
                if "node" in node.tag:
                    ctype = node.get("componentName")
                    
                    # A. Detect standard sub-jobs
                    if ctype == "tRunJob":
                        child_name = next((p.get("value") for p in node if p.get("name") in ['PROCESS', 'PROCESS:PROCESS_TYPE_PROCESS']), None)
                        link_child(child_name)
                    
                    # PATCH B: Universal Linker - Detect Joblets or Components named after other Jobs
                    elif self._normalize(ctype) in self.job_map:
                        link_child(ctype)

                    elif ctype == "tParallelize":
                        # Trace parallel branches
                        p_uname = next((p.get("value") for p in node if p.get("name") == "UNIQUE_NAME"), None)
                        if p_uname:
                            for conn in root.iter():
                                if "connection" in conn.tag and conn.get("source") == p_uname:
                                    tgt_name = conn.get("target")
                                    for tnode in root.iter():
                                        if "node" in tnode.tag:
                                            t_uname = next((p.get("value") for p in tnode if p.get("name") == "UNIQUE_NAME"), None)
                                            if t_uname == tgt_name:
                                                # If it connects to a tRunJob or a Joblet-named component
                                                inner_ctype = tnode.get("componentName")
                                                if inner_ctype == "tRunJob":
                                                    link_child(next((p.get("value") for p in tnode if p.get("name") in ['PROCESS', 'PROCESS:PROCESS_TYPE_PROCESS']), None))
                                                elif self._normalize(inner_ctype) in self.job_map:
                                                    link_child(inner_ctype)
        except: pass