#!/usr/bin/env bash
set -euo pipefail

#raw_data
#├── comm_use_subset (download from https://pages.semanticscholar.org/coronavirus-research)
#├── KG (download from http://blender.cs.illinois.edu/covid19/)
#└── metadata.csv (download from https://pages.semanticscholar.org/coronavirus-research)

python ingest_mappings.py raw_data/KG raw_data
python load_rel_index.py sample_cord_rel_index