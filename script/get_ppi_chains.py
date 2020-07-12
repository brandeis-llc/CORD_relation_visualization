from collections import defaultdict
from data import load_pickled_obj, pickle_obj_mapping


def get_enz_subj_proteins(pkl_file: str, out_file: str):
    ppi_docs = load_pickled_obj(pkl_file)
    enz_name = "enz"
    subj_name = "subj"
    other_name = "other"
    protein_names = defaultdict(set)
    for doc in ppi_docs:
        if doc["PPIs"]["meta_rel"] == "Modification":
            if doc["PPIs"]["enz"]:
                protein_names[enz_name].add(doc["PPIs"]["enz"])
            else:
                protein_names[enz_name].add("Enzyme")
        elif doc["PPIs"]["meta_rel"] == "RegulateActivity":
            protein_names[subj_name].add(doc["PPIs"]["subj"])
        else:
            protein_names[other_name].add(doc["PPIs"]["ent1"])
    pickle_obj_mapping(protein_names, out_file)


def get_protein_container_rel(pkl_file: str, out_file: str):
    ppi_docs = load_pickled_obj(pkl_file)
    enz_name = "enz"
    subj_name = "subj"
    other_name = "ent1"
    protein_container_map = defaultdict(set)
    for doc in ppi_docs:
        if doc["PPIs"]["meta_rel"] == "Modification":
            try:
                protein_container_map[doc["PPIs"][enz_name]].add(doc["PPIs"]["container"])
            except KeyError:
                continue
        elif doc["PPIs"]["meta_rel"] == "RegulateActivity":
            protein_container_map[doc["PPIs"][subj_name]].add(doc["PPIs"]["container"])
        else:
            try:
                protein_container_map[doc["PPIs"][other_name]].add(
                    doc["PPIs"]["container"]
                )
            except KeyError:
                continue
    pickle_obj_mapping(protein_container_map, out_file)


def add_fst_level_inference(pkl_file: str, mapping_pkl_file: str, out_file: str) -> None:
    ppi_docs = load_pickled_obj(pkl_file)
    protein_mapping = load_pickled_obj(mapping_pkl_file)
    inference_mapping = dict()

    for p_type in protein_mapping:
        for p_name in protein_mapping[p_type]:
            inference_mapping[p_name] = defaultdict(set)

    for doc in ppi_docs:
        if doc["PPIs"]["meta_rel"] == "Modification":
            target_protein = doc["PPIs"]["sub"]
            act_protein = doc["PPIs"]["enz"]
            if not act_protein:
                act_protein = "Enzyme"
        elif doc["PPIs"]["meta_rel"] == "RegulateActivity":
            target_protein = doc["PPIs"]["obj"]
            act_protein = doc["PPIs"]["subj"]
        else:
            target_protein = doc["PPIs"]["ent2"]
            act_protein = doc["PPIs"]["ent1"]
        rel_by = doc["PPIs"]["rel"] + "_by"
        try:
            inference_mapping[target_protein][rel_by].add(act_protein)
        except KeyError:
            continue
    pickle_obj_mapping(inference_mapping, out_file)


def add_fst_infer_to_doc(
    pkl_file: str,
    inference_mapping_pkl: str,
    protein_container_mapping_pkl: str,
    out_file: str,
):
    oppo_rel_mappings = {
        "Activation": "Inhibition",
        "Inhibition": "Activation",
        "IncreaseAmount": "DecreaseAmount",
        "DecreaseAmount": "IncreaseAmount",
        "Acetylation": "Deacetylation",
        "Deacetylation": "Acetylation",
        "Glycosylation": "Deglycosylation",
        "Deglycosylation": "Glycosylation",
        "Methylation": "Demethylation",
        "Demethylation": "Methylation",
        "Palmitoylation": "Depalmitoylation",
        "Depalmitoylation": "Palmitoylation",
        "Phosphorylation": "Dephosphorylation",
        "Dephosphorylation": "Phosphorylation",
        "Ribosylation": "Deribosylation",
        "Deribosylation": "Ribosylation",
        "Sumoylation": "Desumoylation",
        "Desumoylation": "Sumoylation",
        "Ubiquitination": "Deubiquitination",
        "Deubiquitination": "Ubiquitination",
        "Complex": "Complex",
        "Translocation": "Translocation",
        "Autophosphorylation": "Autophosphorylation",
        "Farnesylation": "Farnesylation",
        "Hydroxylation": "Dehydroxylation",
        "Dehydroxylation": "Hydroxylation",
        "Myristoylation": "Myristoylation",
    }
    ppi_docs = load_pickled_obj(pkl_file)
    inference_mapping = load_pickled_obj(inference_mapping_pkl)
    protein_container_mapping = load_pickled_obj(protein_container_mapping_pkl)

    new_docs = []
    for doc in ppi_docs:
        if doc["PPIs"]["meta_rel"] == "Modification":
            act_protein = doc["PPIs"]["enz"]
            target_protein = doc["PPIs"]["sub"]
            if not act_protein:
                act_protein = "Enzyme"
        elif doc["PPIs"]["meta_rel"] == "RegulateActivity":
            act_protein = doc["PPIs"]["subj"]
            target_protein = doc["PPIs"]["obj"]
        else:
            act_protein = doc["PPIs"]["ent1"]
            target_protein = doc["PPIs"]["ent2"]
        rel = doc["PPIs"]["rel"] + "_by"
        oppo_rel = oppo_rel_mappings[doc["PPIs"]["rel"]] + "_by"
        try:
            rel_by_proteins = list(inference_mapping[act_protein][rel])
        except KeyError:
            rel_by_proteins = None
        try:
            oppo_rel_by_proteins = list(inference_mapping[act_protein][oppo_rel])
        except KeyError:
            oppo_rel_by_proteins = None
        try:
            target_protein_container = list(protein_container_mapping[target_protein])
        except KeyError:
            target_protein_container = None
        doc["PPIs"]["rel_by_proteins"] = rel_by_proteins
        doc["PPIs"]["oppo_rel_by_proteins"] = oppo_rel_by_proteins
        doc["PPIs"]["target_container"] = target_protein_container
        new_docs.append(doc)
    pickle_obj_mapping(new_docs, out_file)


def main():
    pkl_file = "../raw_data/ppi_docs_0705.pkl"
    name_mapping_file = "../raw_data/enz-subj-proteins.pkl"
    protein_container_mapping_file = "../raw_data/protein-container.pkl"
    inference_mapping_file = "../raw_data/fst-level-inference.pkl"
    get_enz_subj_proteins(pkl_file, name_mapping_file)
    get_protein_container_rel(pkl_file, protein_container_mapping_file)
    add_fst_level_inference(pkl_file, name_mapping_file, inference_mapping_file)

    add_fst_infer_to_doc(
        pkl_file,
        inference_mapping_file,
        protein_container_mapping_file,
        "../raw_data/ppi_docs_with_infer_0707.pkl",
    )


if __name__ == "__main__":
    main()
    pkl_file = "../raw_data/ppi_docs_with_infer_0707.pkl"
    ppi_doc = load_pickled_obj(pkl_file)
    for i in ppi_doc:
        print(i["PPIs"])
        break
