import spacy
from elasticsearch import Elasticsearch
es = Elasticsearch('http://127.0.0.1:9200')
res = es.search(index="CrawlerDz", body={"query": {"match_all": {}}, "size": 1000})
for hit in res['hits']['hits']:
    text = "%(DATA)s" % hit["_source"]
    nlp = spacy.load("ru_core_news_sm")
    doc = nlp(text)
    for named_entity in doc.ents:
        print(named_entity, named_entity.label_)
