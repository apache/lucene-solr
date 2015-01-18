bin/solr stop
rm -Rf server/solr/files/

# templates extracted with:
#    unzip  -j dist/solr-velocity-*.jar velocity/* -x *.properties -d example/files/templates/
bin/solr start -Dvelocity.template.base.dir=<absolute path to example/files/templates>
# TODO: make it so an install dir relative path can be used somehow?
bin/solr create_core -c files
bin/post -c files ~/Documents
curl http://localhost:8983/solr/files/config/params -H 'Content-type:application/json'  -d '{
"update" : {
  "facets": {
    "facet.field":"content_type"
    }
  }
}'