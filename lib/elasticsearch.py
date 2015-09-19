import json, pycurl
from io import BytesIO

"""
Read ES nodes stats from json.
"""
class ElasticNodes:
  # ES nodes
  nodes = None

  def __init__(self, nodestats = None):
    if nodestats is not None:
      self.load(nodestats)

  def load(self, nodestats):
    # read nodestats
    with open(nodestats, 'r') as f:
      self.nodes = json.load(f)['nodes']

  def get_nodes(self):
    return self.nodes.keys()

  def get_host(self, node_id):
    return self.nodes[node_id]['host']

  def get_ip(self, node_id):
    return self.nodes[node_id]['ip'][0][6:].split(':')[0]
    

"""
Read ES shards stats from json.
"""
class ElasticShards:
  # ES shards and nodes
  shards = None
  nodes = None

  def __init__(self, searchshards = None):
    if searchshards is not None:
      self.load(searchshards)

  def load(self, searchshards):
    # read searchshards
    if type(searchshards) is file:
      shards = json.load(searchshards)
    else:
      with open(searchshards) as f:
        shards = json.load(f)
    self.shards = shards['shards']
    self.nodes = shards['nodes']

  def get_nodes(self):
    return self.nodes.keys()

  def get_node_ip(self, node_id):
    return self.nodes[node_id]['transport_address'][6:].split(':')[0]

  def get_shard_ip(self, index):
    node_id = self.get_shard_node(index)
    return self.nodes[node_id]['transport_address'][6:].split(':')[0]

  def get_shard_count(self):
    return len(self.shards)

  def get_shard_node(self, index):
    count = len(self.shards[index])
    # find primary
    primary = 0
    for i in range(len(self.shards[index])):
      if self.shards[index][i]['primary'] is True:
        primary = i
        break
    return self.shards[index][primary]['node'] if len(self.shards[index]) > primary else None

  def get_shard_index(self, index):
    # find primary
    primary = 0
    for i in range(len(self.shards[index])):
      if self.shards[index][i]['primary'] is True:
        primary = i
        break
    return self.shards[index][primary]['index'] if len(self.shards[index]) > primary else None

"""
ES RESTful API
"""
class ElasticSearch:
  # curl instance
  curl = None

  # json converters
  jenc = json.JSONEncoder()
  jdec = json.JSONDecoder()

  def __init__(self, keepAlive = False):
    if keepAlive:
      self.curl = pycurl.Curl()

  # close ES connection
  def close(self):
    if not self.curl is None:
      self.curl.close()
      self.curl = None

  # ES get
  def get(self, url):

    # create output buffer
    output = BytesIO()

    # setup connection
    if self.curl is None:
      c = pycurl.Curl()
    else:
      c = self.curl

    # make a request
    c.setopt(c.URL, url)
    c.setopt(c.WRITEFUNCTION, output.write)
    c.perform()

    # close connection
    if self.curl is None:
      c.close()

    # convert output json to object
    joutput = output.getvalue()
    return self.jdec.decode(joutput)

  # ES post
  def post(self, url, input):

    # convert input object to json
    jinput = self.jenc.encode(input)
    output = BytesIO()

    # setup connection
    if self.curl is None:
      c = pycurl.Curl()
    else:
      c = self.curl

    # make a request
    c.setopt(c.URL, url)
    c.setopt(c.POSTFIELDS, jinput)
    c.setopt(c.WRITEFUNCTION, output.write)
    c.perform()

    # close connection
    if self.curl is None:
      c.close()

    # convert output json to object
    joutput = output.getvalue()
    return self.jdec.decode(joutput)

  # ES bulk update
  def bulk(self, url, input):

    # convert input object to json
    binput = BytesIO()
    for action in input:
      binput.write(self.jenc.encode(action))
      binput.write('\n')
    boutput = BytesIO()

    # setup connection
    if self.curl is None:
      c = pycurl.Curl()
    else:
      c = self.curl

    # make a request
    c.setopt(c.URL, url)
    c.setopt(c.POSTFIELDS, binput.getvalue())
    c.setopt(c.WRITEFUNCTION, boutput.write)
    c.perform()

    # close connection
    if self.curl is None:
      c.close()

    # convert output json to object
    joutput = boutput.getvalue()
    return self.jdec.decode(joutput)

