import json
import pprint
from os import path
from collections import Counter
import argparse


class JsonInspector(object):
    def __init__(self, in_file):
        """
        inspect individual json doc
        :param in_file:
        """
        with open(in_file, 'r') as f:
            self.doc = json.load(f)
        self.info = {}
        self._inspect_meta()
        self._inspect_body_text()
        self._inspect_ref()
        self._inspect_bib()

    def _inspect_meta(self):
        meta = self.doc['metadata']
        title = meta['title']
        authors = meta['authors']
        authors_count = len(authors)
        institutions = []
        for au in authors:
            try:
                inst = au['affiliation']['institution']
                if inst:
                    institutions.append(inst)
            except KeyError:
                continue
        institutions = dict.fromkeys(institutions).keys()

        self.info['title'] = title
        self.info['authors_count'] = authors_count
        self.info['institutions'] = tuple(institutions)

    def _inspect_body_text(self):
        body_t = self.doc['body_text']
        sections = dict.fromkeys([b['section'] for b in body_t])
        sections = tuple(sections.keys())
        self.info['sections_count'] = len(sections)

    def _inspect_bib(self):
        bibs = self.doc['bib_entries']
        bibs_count = len(bibs)
        self.info['bibs_count'] = bibs_count

    def _inspect_ref(self):
        ref_counter = Counter()
        refs = self.doc['ref_entries']
        ref_counter.update(refs[k]['type'] for k in refs)
        for k in ref_counter:
            self.info[f'{k}s_count'] = ref_counter[k]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('json_file')
    args = parser.parse_args()
    pprint.pprint(JsonInspector(path.join(args.json_file)).info, width=120, indent=4)





