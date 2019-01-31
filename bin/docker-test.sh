docker run \
    --rm \
    --init \
    -p 9200:9200 \
    -e EVA_STOCK_URL='http://10.140.0.3:8080/middleware/pim/stock' \
    -v C:/code/eva-stock-plugin/bin/../build/elasticsearch:/usr/share/elasticsearch/plugins/eva-stock-plugin \
    -v C:/code/eva-stock-plugin/bin/elasticsearch.yml:/usr/share/elasticsearch/config/elasticsearch.yml \
    elasticsearch:5.6.4