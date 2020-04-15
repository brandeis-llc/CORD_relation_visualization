from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

client = Elasticsearch([{"host": "localhost", "port": 9200}])


def free_search_in_title(word):
    s = Search(using=client, index="covid_meta_index")
    # Q is a shortcut for constructing a query object
    q = Q("match", title=word)
    # At some point, q has to be added to the search object.
    s = s.query(q)
    s = s.highlight_options(pre_tags="<mark>", post_tags="</mark>")  # for html
    s = s.highlight("title", word, fragment_size=999999999, number_of_fragments=1)
    response = s.execute()
    print("Num hits for", word, len(response.to_dict()["hits"]["hits"]))
    for hit in response:
        print(hit.meta.score)  # doc score
        print(hit.meta.highlight)  # highlighted snippet


free_search_in_title("immunologic")
