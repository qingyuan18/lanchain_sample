import boto3
import json
import requests
import time
from collections import defaultdict
from requests_aws4auth import AWS4Auth
import os
from opensearchpy import OpenSearch, RequestsHttpConnection

source_includes = ["question","answer"]
runtime= boto3.client('runtime.sagemaker')
headers = { "Content-Type": "application/json" }


########get k-NN search resule###############
# input:
#  r: AOS returned json
# return:
#  result : array of topN text  
#############################################
def parse_results(r):
    res = []
    result = []
    for i in range(len(r['hits']['hits'])):
        h = r['hits']['hits'][i]
        if h['_source']['question'] not in clean:
          result.append(h['_source']['question'])
          res.append('<第'+str(i+1)+'条信息>'+h['_source']['question'] + '。</第'+str(i+1)+'条信息>\n')
    print(res)
    return result

########get embedding vector by SM llm########
# input:
#  q:question text
#  sm_endpoint:Sagemaker KNN_ENDPOINT_NAME
# return:
#  result : vector of embeded text  
#############################################
def get_vector_by_sm_endpoint(q):
  payload = json.dumps({"inputs":[q]})
  response = runtime.invoke_endpoint(EndpointName=KNN_ENDPOINT_NAME,
                                     ContentType="application/json",
                                     Body=payload)
  result = json.loads(response['Body'].read().decode())[0][0][0]
  # print(result)
  return result


########get embedding vector by lanchain vector search########
# input:
#  q:question text
#  vectorSearch:lanchain vectorSearch instance
# return:
#  result : vector of embeded text  
#############################################################
def get_vector_by_lanchain(q , vectorSearch):
    docs = vectorSearch.similarity_search(query)
    print(docs[0].page_content)
    return docs


########k-nn search by lanchain########
# input:
#  q:question text
#  vectorSearch: lanchain VectorSearch instance
# return:
#  result : k-NN search result  
#############################################################
def search_using_lanchain(question, vectorSearch):
  docs = vectorSearch.similarity_search(query)
  return docs



########k-nn by native AOS########
# input:
#  q:question text
#  index:AOS k-NN index name
#  hostname: aos endpoint
#  username: aos username
#  passwd: aos password
#  source_includes: fields to return
#  k: topN
# return:
#  result : k-NN search result  
#############################################################
def search_using_aos_knn(q, hostname, username,passwd, index, source_includes, size):
    print(1, q)
    awsauth = (username, passwd)
    query = {
          "size": num_output,
          "_source": {
            "includes": source_includes
          },
          "size": size,
          "query": {
            "knn": {
              "sentence_vector": {
                "vector": get_vector_by_sm_endpoint(q),
                "k": size
              }
            }
          }
        }
    r = requests.post(hostname + index + '/_search', auth=awsauth, headers=headers, json=query)
    return r.text



########k-nn ingestion by native AOS########
# input:
#  docs:ingestion source documents
#  index:AOS k-NN index name
#  hostname: aos endpoint
#  username: aos username
#  passwd: aos password
# return:
#  result : N/A  
#############################################################
def k-nn_ingestion_by_aos(docs,index,hostname,passwd)
    search = OpenSearch(
         hosts = [{'host': host, 'port': 443}],
         ##http_auth = awsauth ,
         http_auth = auth ,
         use_ssl = True,
         verify_certs = True,
         connection_class = RequestsHttpConnection
     )
    for doc in docs:
        vector_field = doc['k_nn_vector']
        question_filed = doc['question']
        answer = doc['answer']
        document = { "question": question_filed, 'answer':answer_field, "knn_vector": vector_field} 
        search.index(index=index, body=document) 


########k-nn ingestion by lanchain #########################
# input:
#  docs:ingestion source documents
#  vectorStore: lanchain AOS vectorStore instance
# return:
#  result : N/A  
#############################################################
def k-nn_ingestion_by_lanchain(docs,vectorStore)
    for doc in docs:
        vectorStore.add_texts(docs,batch_size=10)