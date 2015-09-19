"""
API to extract bits and pieces from CURL (command line utility) headers file.
The headers can be obtained by calling: curl -D 'headers' 'url'.
Currenlty supported formats are for protocols: HTTP, HTTPS.

"""

class Curlheaders:
  # response codes and headers container
  reponses = list()

  def __init__(self, headers = None):
    if headers is not None:
      self.load(headers)

  def load(self, headers):
    # read headers
    with open(headers) as f:
      lines = [line.strip() for line in f]

      # create response list
      resps = list()
      line_iter = iter(lines)

      # consume response code
      line = next(line_iter, None)
      resp = dict()
      resp['code'] = line.split()[1]
      resp['head'] = dict()

      # iterate over headers
      for line in line_iter:
        if len(line) is 0:
          # append last response
          resps.append(resp)
          # consume response code
          line = next(line_iter, None)
          if line is None: break
          resp = dict()
          resp['code'] = line.split()[1]
          resp['head'] = dict()
        else:
          # consume response header
          head = line.find(': ')
          name = line[0:head].lower()
          val = line[head+2:len(line)]
          resp['head'][name] = val

    # update loaded reponses
    self.responses = resps

  def response_count(self):
    return len(self.responses)

  def http_code(self, response_index):
    return self.responses[response_index]['code']

  def http_header(self, response_index, header_name):
    header_name = header_name.lower()
    try:
      return self.responses[response_index]['head'][header_name]
    except KeyError:
      return None
      
