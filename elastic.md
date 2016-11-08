#Elasticsearch Commands
The [Documentation](https://www.elastic.co/guide/index.html) can befound online.
Use [2.3.X Documentation](https://www.elastic.co/guide/en/elasticsearch/reference/2.3/index.html) for this project.

###Create an Indexedit
```
curl -XPUT 'localhost:9200/customer?pretty'
curl 'localhost:9200/_cat/indices?v'
```

###Let’s index a simple customer document into the customer index, "external" type, with an ID of 1 as follows:
```
curl -XPUT 'localhost:9200/customer/external/1?pretty' -d '
{
  "name": "John Doe"
}'
```
###Let’s now retrieve that document that we just indexed:
```
curl -XGET 'localhost:9200/customer/external/1?pretty'

```
###Delete an Indexedit
```
curl -XDELETE 'localhost:9200/customer?pretty'
```
###This example shows how to index a document without an explicit ID:
```
curl -XPOST 'localhost:9200/customer/external?pretty' -d '
{
  "name": "Jane Doe"
}'
```
### Update
This example shows how to update our previous document (ID of 1) by changing the name field to "Jane Doe" and at the same time add an age field to it:
```
curl -XPOST 'localhost:9200/customer/external/1/_update?pretty' -d '
{
  "doc": { "name": "Jane Doe", "age": 20 }
}'
```
###Deleting Documentsedit
```
curl -XDELETE 'localhost:9200/customer/external/2?pretty'
```

##The Search APIedit
The REST API for search is accessible from the _search endpoint. This example returns all documents in the bank index:
```
curl -XPOST 'localhost:9200/reactiontest/_search?pretty' -d ' { "query": { "match_all": {} } }'
```
###
```


```
###
```


```
###
```


```
###
```


```
###
```


```
###
```


```
###
```


```
