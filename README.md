# SemViz for COVID-19 Literature
This is the in progress code base for EACL 2021 System Demonstration submission: 
_Exploration and Discovery of the COVID-19 Literature through Semantic Visualization_. 
This code base provides modules and scripts for data processing and hierarchical structure building. Dashboards are built directly using 
Kibana and graphs are built with JavaScript. Scripts for building visualizations will be provided soon in the future. Check [here](https://www.semviz.org/) for more information and the application.

# Datasets
we have applied semantic visualization techniques to the analysis of the recently released  COVID-related datasets:

- [Harvard  INDRA  Covid-19  KnowledgeNetwork (INDRA CKN) dataset](https://emmaa.indra.bio/all_statements/covid19)
- [the Blender labCovid knowledge graph dataset (Blender KG)](http://blender.cs.illinois.edu/covid19/)
- [COVID-19 Open Research Dataset (CORD-19)](https://www.semanticscholar.org/cord19/download)

# Directory Structure
Here we show the directory structure and main functionality of each module and script.
```
CORD_relation_visualization
├── data                    # module for data processing
│   ├── __init__.py
│   ├── chem_dis_rel.py     # process genes_diseases_relation.csv from Blender KG
│   ├── chem_gene_rel.py    # process chem_gene_ixns_relation.csv from Blender KG
│   ├── data_io.py
│   ├── dise_gene_rel.py    # process genes_diseases_relation.csv
│   ├── doc.py              # process each json file from CORD-19
│   ├── meta.py             # process metadata file from CORD-19
│   └── parse_pmc_stats.py  # process PPCA dataset
├── elastic_index.py
├── load_doc_ppi_index.py   
├── load_index.py
├── load_meta_index.py      # build data structure for CORD-19 metadata
├── load_ppi_index.py       # build data structure for PPCA dataset
├── load_rel_index.py       # build data structure for Blender KG
└──  script
│   ├── get_ppi_chains.py   # add protein functional types to the index
│   ├── get_sub_meta_csv.py
│   ├── ingest_mappings.py  # picklize genes, diseases and proteins from Blender KG  
│   └── json_inspect.py
└──
```