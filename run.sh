
# ./gradlew -p solr assemble

cd solr/packaging/build/solr-9.0.0-SNAPSHOT

bin/solr -e techproducts -Dsolr.ltr.enabled=true

curl -XPUT 'http://localhost:8983/solr/techproducts/schema/feature-store' --data-binary "@../../../..//feature-store.minimal.json" -H 'Content-type:application/json'

curl -XPOST 'http://localhost:8983/solr/techproducts/update?wt=json&commitWithin=1&overwrite=true' --data-binary "@../../../..//sample_documents.json" -H 'Content-type:application/json'

curl -XGET 'http://localhost:8983/solr/techproducts/select?fl=id%2C%5Bfeatures%20store%3Ddev_2020_08_26%20efi.term%3D"husa%20cu%20tastatura%20tableta%2010%20inch"%5D&q=husa%20cu%20tastatura%20tableta%2010%20inch&rows=500'

bin/solr delete -c techproducts

bin/solr stop -all

cd -

# grep -h "cpoerschke debug" `find . -name solr-8983-console.log`

