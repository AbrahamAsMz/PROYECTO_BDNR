import pymongo
import cassandra.cluster
import pydgraph

def connect_mongo(uri="mongodb://127.0.0.1:27017"):
    return pymongo.MongoClient(uri).learnlink

def connect_cassandra(hosts=["127.0.0.1"]):
    cluster = cassandra.cluster.Cluster(hosts)
    session = cluster.connect("learnlink")
    return session

def connect_dgraph(host="127.0.0.1:9080"):
    client_stub = pydgraph.DgraphClientStub(host)
    return pydgraph.DgraphClient(client_stub)