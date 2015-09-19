#
# PagaRank job processor (DIST step)
#
import sys, io, json

# syntax
def syntax():
  print """
Syntax: %s <EC_HOME> <STEP_NAME> <BULK_SIZE>" % sys.argv[0]

Options:
  EC_HOME   - ElasticCrawler home directory.
  STEP_NAME - PageRank step name (INIT, DIST, RANK or PUBL).
  BULK_SIZE - ElasticSearch bulk size ( > 0 ) for batch updates.
  """

# check the arguments
if len(sys.argv) != (1+3):
  syntax()
  sys.exit(1)

# get input arguments
EC_HOME = sys.argv[1]
STEP_NAME = sys.argv[2]
BULK_SIZE = int(sys.argv[3])

# validate input arguments
if not STEP_NAME in ['INIT', 'DIST', 'RANK', 'PUBL'] or BULK_SIZE <= 0:
  syntax()
  sys.exit(2)

# add ElasticCrawler lib to system path
sys.path.append("%s/lib" % EC_HOME)
from properties import Properties
from elasticsearch import ElasticSearch
from elasticcrawler import get_url_id

# load ElasticCrawler configuarion
conf = Properties()
conf.load(open("%s/conf/elasticcrawler.conf" % EC_HOME))

ES_HOST = conf['ES_HOST']
ES_PORT = conf['ES_PORT']
ES_INDEX = conf['ES_INDEX']
ES_BASE = "http://%s:%s/%s" % (ES_HOST, ES_PORT, ES_INDEX)

# load ElasticSearch REST API module
es = ElasticSearch()

# create batch update container
update_list = list()

# create fetch doc id container
fetch_list = list()

# invalid urls may contain tabs, therefore we need to hand-stitch the urls
def get_docID_URL(line):
  parts = line.strip().split('\t')
  return (parts[0], '\t'.join(parts[1:]))

# update a batch of documents
def batch_update(type):
  global update_list
  print "Updating %d %s docs" % (len(update_list)/2, type)

  # send bulk request
  request = "%s/%s/_bulk" % (ES_BASE, type)
  update = es.bulk("%s/%s/_bulk" % (ES_BASE, type), update_list)
  update_list = list()

# fetch a batch of docuemnts
def batch_fetch(type):
  global fetch_list
  print "Fetching %d %s docs" % (len(fetch_list), type)

  # send mget request
  data = dict()
  data['ids'] = fetch_list
  return es.post("%s/%s/_mget" % (ES_BASE, type), data)

# distribute a batch of docuemnts
def batch_distribute(nodes, ranks):  
  global update_list
  print "Distributing %d docs" % (len(nodes['docs']))

  # process batch of nodes and ranks
  for node, rank in zip(nodes['docs'], ranks['docs']):
    # get outlinks
    try:
      olinks = node['_source']['olinks']
      if len(olinks) == 0:
        continue
    except KeyError:
      continue
    # get ranking
    try:
      ranking = rank['_source']['rank']
    except KeyError:
      continue

    # calculate weighted probability
    prob = float(ranking) / len(olinks)

    # distribute weighted probability
    for link in olinks:
      doc_id = get_url_id(link)
      update_list.append({"update":{"_id":doc_id}})
      update_list.append({"script":"ec_rank_prob_add","params":{"delta":prob}})
      if len(update_list) >= 2 * BULK_SIZE:
        batch_update('rank')

# publish a batch of docuemnts
def batch_publish(ranks):  
  print "Publishing %d docs" % (len(ranks['docs']))
  global update_list

  # process batch of nodes and ranks
  for rank in ranks['docs']:
    # get ranking
    try:
      doc_id = rank['_id']
      ranking = rank['_source']['rank']
    except KeyError:
      continue

    update_list.append({"update":{"_id":doc_id}})
    update_list.append({"doc":{"rank":ranking+1.0}})
    if len(update_list) >= 2 * BULK_SIZE:
      batch_update('page')

# pagerank INIT job
def pagerank_INIT():
  global update_list

  # process urls from stdin
  for line in sys.stdin:
    doc_id, url = get_docID_URL(line)
    update_list.append({"create":{"_id":doc_id}})
    update_list.append({"rank":1.0,"prob":0.0})
    if len(update_list) >= 2 * BULK_SIZE:
      batch_update('rank')
  
  # process remaining urls
  if len(update_list) > 0:
    batch_update('rank')

# pagerank DIST job
def pagerank_DIST():
  global fetch_list

  # process urls from stdin
  for line in sys.stdin:
    doc_id, url = get_docID_URL(line)
    fetch_list.append(doc_id)
    if len(fetch_list) >= BULK_SIZE:
      nodes = batch_fetch('node')
      ranks = batch_fetch('rank')
      batch_distribute(nodes, ranks)
      fetch_list = list()

  # process remaining urls
  if len(fetch_list) > 0:
    nodes = batch_fetch('node')
    ranks = batch_fetch('rank')
    batch_distribute(nodes, ranks)

  # process remaining batch 
  if len(update_list) > 0:
    batch_update('rank')

# pagerank RANK job
def pagerank_RANK():
  global update_list

  # process urls from stdin
  for line in sys.stdin:
    doc_id, url = get_docID_URL(line)
    update_list.append({"update":{"_id":doc_id}})
    update_list.append({"script":"ec_rank_update"})
    if len(update_list) >= 2 * BULK_SIZE:
      batch_update('rank')
  
  # process remaining urls
  if len(update_list) > 0:
    batch_update('rank')

# pagerank PUBL job
def pagerank_PUBL():
  global fetch_list

  # process urls from stdin
  for line in sys.stdin:
    doc_id, url = get_docID_URL(line)
    fetch_list.append(doc_id)
    if len(fetch_list) >= BULK_SIZE:
      ranks = batch_fetch('rank')
      batch_publish(ranks)
      fetch_list = list()

  # process remaining urls
  if len(fetch_list) > 0:
    ranks = batch_fetch('rank')
    batch_publish(ranks)
       
  # process remaining urls
  if len(update_list) > 0:
    batch_update('page')

# call local pagerank_[STEP_NAME] method
locals()["pagerank_%s" % STEP_NAME]()

