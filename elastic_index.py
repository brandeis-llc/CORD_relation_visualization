from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Index, Document, Text, Keyword, InnerDoc, Date
from elasticsearch_dsl.connections import connections


class CovidMeta(Document):
    """
    create Document mapping schema
    """

    sha = Text()
    pubmed_id = Keyword()
    title = Text()
    abstract = Text()
    authors = InnerDoc()  # data type for an array of objects
    authors_full = InnerDoc()
    institutions = InnerDoc()
    countries = InnerDoc()
    journal = Keyword()
    publish_time = InnerDoc()
    es_date = Date()  # Date data type
    action_interactions = InnerDoc()
    PPIs = InnerDoc()

    def save(self, *args, **kwargs):
        return super(CovidMeta, self).save(*args, **kwargs)


class ESIndex(object):
    def __init__(self, index_name, docs):
        # connect to your host (for elasticsearch)
        connections.create_connection(
            hosts=["yourhost"],
            timeout=100,
        )
        self.index = index_name
        # connect to your host (for elasticsearch-dsl)
        self.es = Elasticsearch(
            [{"host": "your-host", "port": 0000}],
            timeout=200,
        )
        es_index = Index(self.index)
        # delete existing index that has the same name
        if es_index.exists():
            es_index.delete()
        es_index.document(CovidMeta)
        es_index.create()
        if docs is not None:
            self.load(docs)

    def to_bulk_iterable(self, docs):
        # bulk insertion
        for i, doc in enumerate(docs):
            doc_id = doc.get("doc_id")
            identifier = i if doc_id is None else doc_id
            # doc to be inserted should be consistent with Document mapping we defined above
            yield {
                "_type": "_doc",
                "_id": identifier,
                "_index": self.index,
                "sha": doc.get("sha", None),
                "pubmed_id": doc[
                    "pubmed_id"
                ],  # technically pubmed_id will never be empty
                "title": doc.get("title", None),
                "abstract": doc.get("abstract", None),
                "authors": doc.get("authors", None),
                "authors_full": doc.get("authors_full", None),
                "institutions": doc.get("institutions", None),
                "countries": doc.get("countries", None),
                "journal": doc.get("journal", None),
                "publish_time": doc.get("publish_time", None),
                "es_date": doc.get("es_date", None),
                "action_interactions": doc.get(
                    "action_interactions", None
                ),  # for Heng Ji's data
                "PPIs": doc.get("PPIs", None),  # for John's data,
            }

    def load(self, docs):
        helpers.bulk(self.es, self.to_bulk_iterable(docs))
