docker run \
    --rm \
    --init \
    -it \
    -p 9200:9200 \
    -e EVA_STOCK_URL='http://eva-backend-service.prod-rituals.svc.cluster.local:8080/middleware/pim/stock' \
    -v C:/code/eva-stock-plugin/bin/../build/elasticsearch:/usr/share/elasticsearch/plugins/eva-stock-plugin \
    -v C:/code/eva-stock-plugin/bin/elasticsearch.yml:/usr/share/elasticsearch/config/elasticsearch.yml \
    elasticsearch:6.6.1