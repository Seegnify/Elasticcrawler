Elasticcrawler
==============

Distributed web crawler for Elasticsearch.

Elasticcrawler + Elasticsearch = Web Search Engine.

See it live at www.seegnify.com.

Features
--------

Elasticcrawler does the following things:

* Create index for crawling, ranking and searching
* Seed index with arbitrary URLs
* Fetch URLs via HTTP and/or HTTPS
* Parse web pages with Tika or alternative parsers
* Populate searchable index with parsed content
* Generate web graph from fetched URLs
* Rank web pages using PageRank algorithm
* Tag and filter URLs
* Prune low ranked URLs
* Export index to another cluster

Folders
-------

    bin  - executable control scripts
    lib  - internal libraries
    job  - internal scripts to run distributed jobs
    conf - configuration files
    test - test cases and tools

How to crawl
------------

There are four essential functions a crawler does, create index, seed the index 
with arbitrary URLs, fetch and rank the URLs. Each of the following steps 
requires that access to Elasticsearch is already defined (see the Configuration 
section).

### Creating index

Creating a new index is very easy, just execute the following command.

    ec-index -n index1

To list all existing indexes including the newly created one run `ec-index -l`.

### Seeding index

Initial URLs can be seeded from a file with each URL listed on separate line.

    ec-create-urls urls.txt

### Fetching

Fetching is distributed. Number of fetcher jobs is defined by configuration
setting MAX_JOB_COUNT. Elasticsearch relies on Shellcloud to distribute the 
crawler jobs. To start the crawler on new URls run the following command:

    ec-fetcher -n

### Ranking

Ranking also is distributed and it depends on fetching. Make sure that at least 
some urls have been fetched prior to running ranking. To start the ranking run 
the following command:

    ./bin/ec-ranking -f 2 -o 1m

The command arguments mean that a full ranking algorightm will run 2 iterations 
on URLs that are at least 1 minute old, that is have been fetched more than 1 
minute ago.

Installation
------------

Installation is as easy as copying the project files to a desired location and 
adding Elasticcrawler 'bin' folder environment variable PATH.

Configuration
-------------

All configuration files are in the conf folder.

    conf/elasticcrawler.conf
    conf/allowed.hosts
    conf/excluded.hosts  
    conf/statuscodes.conf

The main configuration options are in elasticcrawler.conf. The meaning of each 
option is described in the file. ES_HOST must be set to allow Elasticcrawler to 
connect to Elasticsearch. You may want to change ES_INDEX value from the 
default 'web' to your own, and make sure to set HTTP_USER_AGENT to indicate 
that it's your crawler.

The allowed.hosts and excluded.hosts files define hosts that the fetcher  
respectively can or cannot access. The fetcher checks the excluded list first 
and then the allowed list. If allowed list is empty, all hosts can be accessed.

The statuscodes.conf file defines codes used to report crawling errors.

Elasticcrawler uses Elasticsearch scripting. To complete the configuration copy 
lib/*.groovy files to Elasticsearch scripts folder.

To verify the configuration run the following command on each cluster node.

    ec-server config

The output of the command should look similar to the one below.

    OK - curl present at /usr/bin/curl
    OK - python present at /usr/bin/python
    OK - python module IPy present
    OK - python module pycurl present
    OK - python module BeautifulSoup present
    OK - nc present at /bin/nc
    OK - jq present at /usr/local/bin/jq
    OK - awk present at /usr/bin/awk
    OK - shc present at /opt/shellcloud/bin/shc
    OK - java present at /usr/bin/java
    OK - logs folder present at /opt/elasticcrawler/logs
    OK - tika file present at /opt/apache-tika/tika-app-1.6.jar
    OK - tika parse server present at localhost:9900
    OK - elasticsearch present at server.seegnify.net:9200

Elasticsarch jobs may create child jobs, therefore Shellcloud nodes must be set 
up for SSH or RHS access from other nodes.

Performance
-----------

For performance tips please review the NOTES file.
