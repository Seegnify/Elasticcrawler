import os, sys, socket, hashlib, traceback, json, robotparser
from BeautifulSoup import BeautifulSoup as BS
from urlparse import urlparse, urlunparse
from curlheaders import Curlheaders
from properties import Properties 
from IPy import IP

# check if IP4 address is valid
def is_valid_ipv4_address(address):
    try:
        socket.inet_pton(socket.AF_INET, address)
    except AttributeError:
        try:
            socket.inet_aton(address)
        except socket.error:
            return False
        return address.count('.') == 3
    except socket.error:
        return False

    return True

# check if IP4 address is valid
def is_valid_ipv6_address(address):
    try:
        socket.inet_pton(socket.AF_INET6, address)
    except socket.error:
        return False
    return True

# check if IP4 address is marked as private
def is_ip_address_private(address):
  try:
      return IP(address).iptype() == 'PRIVATE'
  except Exception:
      return False
  return True

# get domain name form url
def get_domain(url):
  scheme = urlparse(url)

  if is_valid_ipv4_address(scheme.hostname):
    return scheme.hostname
  elif is_valid_ipv6_address(scheme.hostname):
    return scheme.hostname

  parts = scheme.hostname.split('.')

  if len(parts) > 1:    
    return parts[len(parts)-2] + '.' + parts[len(parts)-1]
  else:
    return scheme.hostname

# get host name form url
def get_host(url):
  return urlparse(url).hostname

# encode utf-8
def encode_utf8(utf8):
  try:
    encoded = utf8.encode('utf-8')
  except UnicodeDecodeError:
    encoded = utf8
  return encoded

# get url id
def get_url_id(url):
  hash = hashlib.sha1()
  url_encoded = encode_utf8(url)
  hash.update(url_encoded)
  return hash.hexdigest()

# get valid url
def get_valid_url(url):
  scheme, netloc, path, params, query, fragment = urlparse(url)
  return urlunparse((scheme, netloc, path, params, query, scheme))

# get robots url
def get_robots_url(url):
  scheme, netloc, path, params, query, fragment = urlparse(url)
  return urlunparse((scheme, netloc, 'robots.txt', '', '', ''))

# get network location id
def get_netloc_id(url):
  hash = hashlib.sha1()
  hash.update(urlparse(url).netloc)
  return hash.hexdigest();

# get HTTP response code
def get_response_code(headers):
  ch = Curlheaders(headers)
  count = ch.response_count()
  return ch.http_code(count-1)

# get HTTP header value
def get_header_value(headers, name):
  ch = Curlheaders(headers)
  count = ch.response_count()
  return ch.http_header(count-1, name)

# get robots create request
def get_robots_create_request(robots, robots_url):
  # create document
  doc = dict()
  doc['url'] = robots_url

  # read robots.txt
  with open (robots) as f:
    doc['robots'] = f.read()

  # convert to json doc
  jdoc = json.JSONEncoder().encode(doc)
  return jdoc

# decode json encoded file
def json_decode(jsonfile):
  # read json file
  with open (jsonfile) as f:
    jtext = f.read()

  # decode json
  text = json.JSONDecoder().decode(jtext)
  return text if text is not None else ''

# get search index update request
def get_index_update_request(url, status, okcodes, subject, content, outlinks):
  
  # create empty updates list
  request = []

  # url id, host id
  url_id = get_url_id(url)
  host_id = get_netloc_id(url)

  # node update action
  node_action = dict()
  node_action['update'] = dict()
  node_action['update']['_type'] = 'node'
  node_action['update']['_id'] = url_id

  # node update data
  node_data = dict()
  node_data['doc_as_upsert'] = True
  node_data['doc'] = dict()
  node_data['doc']['url'] = url
  node_data['doc']['status'] = status

  # host update action
  host_action = dict()
  host_action['update'] = dict()
  host_action['update']['_type'] = 'host'
  host_action['update']['_id'] = host_id

  # host update data - empty doc just to update _timestamp
  host_data = dict()
  host_data['doc_as_upsert'] = True
  host_data['doc'] = dict()

  # if success update page
  if status in okcodes:
    # extended node data
    with open(outlinks) as f:
      node_data['doc']['olinks'] = [link.strip() for link in f.readlines()]

    # separate title and content
    with open(subject) as f:
      title = f.read()
    with open(content) as f:
      body = f.read()
    if body.startswith(title):
      body = body[len(title):]

    # page update action
    page_action = dict()
    page_action['update'] = dict()
    page_action['update']['_type'] = 'page'
    page_action['update']['_id'] = url_id

    # page update data
    page_data = dict()
    page_data['doc_as_upsert'] = True
    page_data['doc'] = dict()
    page_data['doc']['title'] = title
    page_data['doc']['body'] = body
    page_data['doc']['url'] = url

    # output create actions
    request.append(json.JSONEncoder().encode(page_action))
    request.append(json.JSONEncoder().encode(page_data))

  # output update action
  request.append(json.JSONEncoder().encode(node_action))
  request.append(json.JSONEncoder().encode(node_data))

  # output host update
  request.append(json.JSONEncoder().encode(host_action))
  request.append(json.JSONEncoder().encode(host_data))

  return '\n'.join(line for line in request)

# is access allowed by client
def is_client_access_allowed(url, config, allowed, excluded):

  # get url blocks
  url_parsed = urlparse(url)
  protocol = url_parsed.scheme
  hostname = url_parsed.hostname
  path = url_parsed.path

  # read config variables
  conf = Properties()
  with open(config) as f:
    conf.load(f)

  # check allowed protocols
  allowed_protocols = conf['ALLOWED_PROTOCOLS'].replace(' ', '').split(',')
  if url_parsed.scheme not in allowed_protocols:
    return False

  # check excluded file types
  exluded_types = conf['EXCLUDE_FILE_TYPES'].replace(' ', '').split(',')
  dot_index = path.rfind('.', 0)
  if dot_index > 0 and path[dot_index+1:].lower() in exluded_types:
    return False

  # get host groups flags
  exclude_privates = True if conf['EXCLUDE_PRIVATE_HOSTS'] == 'true' else False
  exclude_singles = True if conf['EXCLUDE_SINGLE_HOSTS'] == 'true' else False

  # read exluded hosts
  with open(excluded) as f:
    excluded_hosts = [host.strip() for host in f.readlines()]

  # read allowed hosts
  with open(allowed) as f:
    allowed_hosts = [host.strip() for host in f.readlines()]

  # validate address
  if hostname == None or len(hostname) == 0:
    return False;

  # check excluded hosts
  if hostname in excluded_hosts:
    return False

  # check allowed hosts
  if len(allowed_hosts) > 0 and (hostname not in allowed_hosts):
    return False

  # exclude private hosts
  if exclude_privates == True:
    if is_ip_address_private(hostname):
      return False

  # exclude single hosts
  if exclude_singles == True:
    if len(hostname.split('.')) == 1:
      return False

  # now we can confirm positive
  return True

# is robot access allowed
def is_robot_access_allowed(url, robots, user_agent):
  # read robots
  with open(robots) as f:
    lines = [line.strip() for line in f]

  # parse robots
  parser = robotparser.RobotFileParser()
  parser.parse(lines)

  # check access
  agent  = user_agent.split('/')[0]
  if not parser.can_fetch(agent, url):
    sys.exit(1)

# extract links and title
def extract_links_and_title(url, html, protocols, excluded, outlinks, subject):
  # parse parent URL
  p_scheme, p_netloc, p_path, p_params, p_query, p_fragment = urlparse(url)

  # parse protocols (ALLOWED_PROTOCOLS)
  protocols = protocols.replace(' ', '').split(',')

  # read fetched content
  with open(html) as f:
    bs = BS(f.read())

  # find HTML anchors
  links = bs.findAll('a')
  links += bs.findAll('A')

  # unique hrefs
  hrefs = set()

  # get HTML anchors
  for l in links:
    href = None
    if l.has_key('href'):
      href = l['href']
    if l.has_key('HREF'):
      href = l['HREF']
    if href is not None:
      scheme, netloc, path, params, query, fragment = urlparse(href)
      scheme    = p_scheme    if scheme   is '' else scheme
      netloc    = p_netloc    if netloc   is '' else netloc
      path      = p_path      if path     is '' else path
      params    = p_params    if params   is '' else params
      params    = p_params    if params   is '' else params
      fragment  = ''
      href = urlunparse((scheme, netloc, path, params, query, fragment))
      if scheme in protocols and href not in hrefs and href is not url:
        hrefs.add(href)

  # get HTML title, if present
  try:
    title = bs.html.head.title.text
  except AttributeError:
    title = ''

  # fileter out excluded file types
  exluded_types = excluded.replace(' ', '').split(',')

  # write links
  with open(outlinks, 'w') as f:
    for href in hrefs:
      dot_index = href.rfind('.', 0)
      if dot_index > 0 and href[dot_index+1:].lower() in exluded_types:
        continue
      f.write((u'%s\n' % href).encode('utf-8'))

  # write title
  with open(subject, 'w') as f:
    f.write(title.encode('utf-8'))

