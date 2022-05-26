# Эластик
GET crawler/_search
{
    "size": 1000,
    "query": {
        "match_all": {}
    }
}
