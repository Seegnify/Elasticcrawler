#
# This is a local (non-distributed), interative PageRank alorithm used for
# verification of distributed PageRank implemenatations such as in EC.
# Here we are calculating ranking of a N-size sub-graph of the entire web.
# In he code below we will refer to the N-size sub-graph as "the graph".
#

# the graph size
N = 5

# dumping factor
D = 0.85

# maximum epsilon (algorithm stop condition)
E = 1e-1

# the graph link matrix (i'th node is set to 1 when it links to i'th node)
m = [[0 for i in xrange(N)] for i in xrange(N)]

# the graph outlink count vector (values include links to outside nodes)
c = [0 for i in xrange(N)]

# the graph rank vector (initial values for each graph node set to 1)
r = [1.0 for i in xrange(N)]

# define outlink matrix for the graph, count outlinks of the graph
m[0] = [0,0,0,1,1]; c[0] = 3
m[1] = [0,0,0,1,1]; c[1] = 3
m[2] = [1,1,0,1,1]; c[2] = 5
m[3] = [1,1,0,0,0]; c[3] = 2
m[4] = [1,1,0,1,0]; c[4] = 4

def print_graph():
  out = list()
  out.append(["[node]", "[outlinks]", "[count]", "[ranking]"])
  for i in range(0,N):
    out.append([i, m[i], c[i], r[i]]);

  s = [[str(e) for e in row] for row in out]
  lens = [max(map(len, col)) for col in zip(*s)]
  fmt = '\t'.join('{{:{}}}'.format(x) for x in lens)
  table = [fmt.format(*row) for row in s]
  print '\n'.join(table)

# iterate without inlinks (outlinks only)
def iterate():
  # iteration difference
  delta = 0

  # probability vector
  p = [0.0 for i in xrange(N)]

  # distribute rank probailities to outlinks
  for i in range(0,N):
    if c[i] > 0:
      for j in range(0,N): 
          p[j] = p[j] + m[i][j] * r[i] / c[i]

  # calculate rank vector and find max delta
  for i in range(0,N):
    diff = r[i] - (1.0 - D + D * p[i])
    delta = max(delta, abs(diff))
    r[i] = r[i] - diff

  #return delta
  return delta

def pagerank():
  steps = 0
  delta = E + 1
  print "=== initial graph with target epsilon E", E, "==="
  print_graph()
  while delta > E:
    delta = iterate()
    steps = steps + 1
    if delta > E:
      print "=== iteration step %d === error %f > E ===" % (steps, delta)
    else:
      print "=== iteration step %d === error %f <= E ===" % (steps, delta)
    print_graph()

pagerank()

