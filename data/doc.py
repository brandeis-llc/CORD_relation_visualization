import json
from os import listdir, path


class ParseJsonDoc(object):
    def __init__(self, file_dir, json_name):
        """
        parse individual json doc
        :param file_dir:
        :param json_name:
        """
        self.fields = {}
        try:
            if json_name.endswith(".json"):
                with open(path.join(file_dir, json_name), "r") as f:
                    self.doc = json.load(f)
            else:
                with open(path.join(file_dir, json_name) + ".json", "r") as f:
                    self.doc = json.load(f)
        except FileNotFoundError:
            pass
        else:
            self._parse_meta()

    def _parse_meta(self):
        """
        get institutions and countries from json doc
        :return:
        """
        meta = self.doc["metadata"]
        authors = meta["authors"]
        institutions = []
        countries = []
        for au in authors:
            try:
                inst = au["affiliation"]["institution"]
                if inst:
                    institutions.append(inst)
            except KeyError:
                pass
            try:
                country = au["affiliation"]["location"]["country"]
                if country:
                    countries.append(country)
            except KeyError:
                pass

        self.fields["institutions"] = list(dict.fromkeys(institutions).keys())
        self.fields["countries"] = list(dict.fromkeys(countries).keys())

    # TODO: add more parsable fields

    def _inspect_body_text(self):
        # TODO:
        body_t = self.doc["body_text"]

    def _inspect_bib(self):
        # TODO:
        bibs = self.doc["bib_entries"]

    def _inspect_ref(self):
        # TODO:
        refs = self.doc["ref_entries"]

    def _inspect_abstract(self):
        # TODO:
        abstract = self.doc["abstract"]


if __name__ == "__main__":
    # for testing
    for p in listdir("../raw_data/comm_use_subset"):
        ParseJsonDoc("../raw_data/comm_use_subset/", p)
