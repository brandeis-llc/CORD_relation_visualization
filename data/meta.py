from typing import Sequence, Dict
from string import digits, ascii_lowercase, ascii_uppercase


class ParseMetaData(object):
    def __init__(self):
        """
        parse/clean/modify fields of each line from meta csv
        create new additional fields before generate ES Index
        """
        self.meta_dict = None
        self._get_word_shape_mapping()
        self.DATE_ABBR = dict(
            zip(
                [
                    "Jan",
                    "Feb",
                    "Mar",
                    "Apr",
                    "May",
                    "Jun",
                    "Jul",
                    "Aug",
                    "Sep",
                    "Oct",
                    "Nov",
                    "Dec",
                ],
                [str(n) if n > 9 else "0" + str(n) for n in range(1, 13)],
            )
        )

    def _parse_authors(self) -> None:
        """
        get authors-related fields:
        authors: [{'last_name': 'Doe', 'first_name': 'Jane'}, {...}]
        authors_full: ['Jane Doe', ...]
        :return:
        """
        authors = self.meta_dict.get("authors")
        if not authors:
            self.meta_dict["authors"] = None
            self.meta_dict["authors_full"] = None
            return
        authors = authors.split(";")
        authors_split = [self._split_name(author.split(", ")) for author in authors]
        authors_full = [" ".join(author.split(", ")[::-1]) for author in authors]
        self.meta_dict["authors"] = authors_split
        self.meta_dict["authors_full"] = authors_full

    @staticmethod
    def _split_name(name: Sequence) -> Dict[str, str]:
        if len(name) > 1:
            return {"last_name": name[0], "first_name": name[1]}
        else:
            return {"last_name": name[0], "first_name": None}

    def _parse_date(self) -> None:
        """
        get date-related fields:
        publish_time = {'year': '2020', 'month': '01'}
        es_date = '2020-01' (match es original date format)
        :return:
        """
        # TODO: get full date (year, month, day)
        date = self.meta_dict.get("publish_time")
        if not date:
            self.meta_dict["publish_time"] = None
            self.meta_dict["es_date"] = None
            return
        date_shape = "".join(self.WORD_SHAPE_MAPPING[d] for d in date)
        if date_shape == "dddd":
            date = {"year": date, "month": None}
        elif date_shape == "dddd-dd-dd":
            date = {"year": date[:4], "month": date[5:7]}
        elif date_shape.startswith("dddd Ccc"):
            date = {"year": date[:4], "month": self.DATE_ABBR.get(date[5:8], None)}
        self.meta_dict["publish_time"] = date
        year = self.meta_dict["publish_time"]["year"]
        month = self.meta_dict["publish_time"]["month"]
        # set default value (2020-01-01) for es_date field
        if not year:
            year = "2020"
        if not month:
            month = "01"
        self.meta_dict["es_date"] = "-".join([year, month, "01"])

    def _get_word_shape_mapping(self) -> None:
        """
        utility for processing date
        :return:
        """
        self.WORD_SHAPE_MAPPING = {}
        self.WORD_SHAPE_MAPPING.update(zip(digits, len(digits) * "d"))
        self.WORD_SHAPE_MAPPING.update(zip(ascii_lowercase, len(ascii_lowercase) * "c"))
        self.WORD_SHAPE_MAPPING.update(zip(ascii_uppercase, len(ascii_uppercase) * "C"))
        self.WORD_SHAPE_MAPPING.update({" ": " ", "-": "-"})

    def __call__(self, meta_dict: Dict):
        self.meta_dict = meta_dict
        self._parse_authors()
        self._parse_date()


if __name__ == "__main__":
    pass
