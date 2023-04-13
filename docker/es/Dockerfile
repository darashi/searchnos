FROM docker.elastic.co/elasticsearch/elasticsearch:8.7.0

RUN elasticsearch-plugin install analysis-icu

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD curl -u elastic:${ELASTIC_PASSWORD} -s -f http://localhost:9200/_cat/health > /dev/null || exit 1