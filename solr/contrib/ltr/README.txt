Apache Solr Learning to Rank
========

This is the main [learning to rank integrated into solr](http://www.slideshare.net/lucidworks/learning-to-rank-in-solr-presented-by-michael-nilsson-diego-ceccarelli-bloomberg-lp)
repository.
[Read up on learning to rank](https://en.wikipedia.org/wiki/Learning_to_rank)

Apache Solr Learning to Rank (LTR) provides a way for you to extract features
directly inside Solr for use in training a machine learned model.  You can then
deploy that model to Solr and use it to rerank your top X search results.


# Changes to solrconfig.xml
```xml
<config>
  ...

  <!-- Query parser used to rerank top docs with a provided model -->
  <queryParser name="ltr" class="org.apache.solr.ltr.ranking.LTRQParserPlugin" />

  <!--  Transformer that will encode the document features in the response.
  For each document the transformer will add the features as an extra field
  in the response. The name of the field we will be the the name of the
  transformer enclosed between brackets (in this case [features]).
  In order to get the feature vector you will have to
  specify that you want the field (e.g., fl="*,[features])  -->
  <transformer name="features" class="org.apache.solr.ltr.ranking.LTRFeatureLoggerTransformerFactory" />


  <!-- Component that hooks up managed resources for features and models -->
  <searchComponent name="ltrComponent" class="org.apache.solr.ltr.ranking.LTRComponent"/>
  <requestHandler name="/query" class="solr.SearchHandler">
    <lst name="defaults">
      <str name="echoParams">explicit</str>
      <str name="wt">json</str>
      <str name="indent">true</str>
      <str name="df">id</str>
    </lst>
    <arr name="last-components">
      <!-- Use the component in your requestHandler -->
      <str>ltrComponent</str>
    </arr>
  </requestHandler>

  <query>
    ...

    <!-- Cache for storing and fetching feature vectors -->
    <cache name="QUERY_DOC_FV"
      class="solr.search.LRUCache"
      size="4096"
      initialSize="2048"
      autowarmCount="4096"
      regenerator="solr.search.NoOpRegenerator" />
  </query>

</config>

```


# Build the plugin
In the solr/contrib/ltr directory run
`ant dist`

# Install the plugin
In your solr installation, navigate to your collection's lib directory.
In the solr install example, it would be solr/collection1/lib.
If lib doesn't exist you will have to make it, and then copy the plugin's jar there.

`cp lucene-solr/solr/dist/solr-ltr-X.Y.Z-SNAPSHOT.jar mySolrInstallPath/solr/myCollection/lib`

Restart your collection using the admin page and you are good to go.
You can find more detailed instructions [here](https://wiki.apache.org/solr/SolrPlugins).


# Defining Features
In the learning to rank plugin, you can define features in a feature space
using standard Solr queries. As an example:

###### features.json
```json
[
{ "name": "isBook",
  "type": "org.apache.solr.ltr.feature.impl.SolrFeature",
  "params":{ "fq": ["{!terms f=category}book"] }
},
{
  "name":  "documentRecency",
  "type": "org.apache.solr.ltr.feature.impl.SolrFeature",
  "params": {
      "q": "{!func}recip( ms(NOW,publish_date), 3.16e-11, 1, 1)"
  }
},
{
  "name":"originalScore",
  "type":"org.apache.solr.ltr.feature.impl.OriginalScoreFeature",
  "params":{}
},
{
  "name" : "userTextTitleMatch",
  "type" : "org.apache.solr.ltr.feature.impl.SolrFeature",
  "params" : { "q" : "{!field f=title}${user_text}" }
}
]
```

Defines four features. Anything that is a valid Solr query can be used to define
a feature.

### Filter Query Features
The first feature isBook fires if the term 'book' matches the category field
for the given examined document. Since in this feature q was not specified,
either the score 1 (in case of a match) or the score 0 (in case of no match)
will be returned.

### Query Features
In the second feature (documentRecency) q was specified using a function query.
In this case the score for the feature on a given document is whatever the query
returns (1 for docs dated now, 1/2 for docs dated 1 year ago, 1/3 for docs dated
2 years ago, etc..) . If both an fq and q is used, documents that don't match
the fq will receive a score of 0 for the documentRecency feature, all other
documents will receive the score specified by the query for this feature.

### Original Score Feature
The third feature (originalScore) has no parameters, and uses the
OriginalScoreFeature class instead of the SolrFeature class.  Its purpose is
to simply return the score for the original search request against the current
matching document.

### External Features
Users can specify external information that can to be passed in as
part of the query to the ltr ranking framework. In this case, the
fourth feature (userTextPhraseMatch) will be looking for an external field
called 'user_text' passed in through the request, and will fire if there is
a term match for the document field 'title' from the value of the external
field 'user_text'. See the "Run a Rerank Query" section for how
to pass in external information.

### Custom Features
Custom features can be created by extending from
org.apache.solr.ltr.ranking.Feature, however this is generally not recommended.
The majority of features should be possible to create using the methods described
above.

# Defining Models
Currently the Learning to Rank plugin supports 2 main types of
ranking models: [Ranking SVM](http://www.cs.cornell.edu/people/tj/publications/joachims_02c.pdf)
and [LambdaMART](http://research.microsoft.com/pubs/132652/MSR-TR-2010-82.pdf)

### Ranking SVM
Currently only a linear ranking svm is supported. Use LambdaMART for
a non-linear model. If you'd like to introduce a bias set a constant feature
to the bias value you'd like and make a weight of 1.0 for that feature.

###### model.json
```json
{
    "type":"org.apache.solr.ltr.ranking.RankSVMModel",
    "name":"myModelName",
    "features":[
        { "name": "userTextTitleMatch"},
        { "name": "originalScore"},
        { "name": "isBook"}
    ],
    "params":{
        "weights": {
            "userTextTitleMatch": 1.0,
            "originalScore": 0.5,
            "isBook": 0.1
        }

    }
}
```

This is an example of a toy Ranking SVM model. Type specifies the class to be
using to interpret the model (RankSVMModel in the case of Ranking SVM).
Name is the model identifier you will use when making request to the ltr
framework. Features specifies the feature space that you want extracted
when using this model. All features that appear in the model params will
be used for scoring and must appear in the features list.  You can add
extra features to the features list that will be computed but not used in the
model for scoring, which can be useful for logging.
Params are the Ranking SVM parameters.

Good library for training SVM's (https://www.csie.ntu.edu.tw/~cjlin/liblinear/ ,
https://www.csie.ntu.edu.tw/~cjlin/libsvm/) . You will need to convert the
libSVM model format to the format specified above.

### LambdaMART

###### model2.json
```json
{
    "type":"org.apache.solr.ltr.ranking.LambdaMARTModel",
    "name":"lambdamartmodel",
    "features":[
        { "name": "userTextTitleMatch"},
        { "name": "originalScore"}
    ],
    "params":{
        "trees": [
            {
                "weight" : 1,
                "tree": {
                    "feature": "userTextTitleMatch",
                    "threshold": 0.5,
                    "left" : {
                        "value" : -100
                    },
                    "right": {
                        "feature" : "originalScore",
                        "threshold": 10.0,
                        "left" : {
                            "value" : 50
                        },
                        "right" : {
                            "value" : 75
                        }
                    }
                }
            },
            {
                "weight" : 2,
                "tree": {
                    "value" : -10
                }
            }
        ]
    }
}
```
This is an example of a toy LambdaMART. Type specifies the class to be using to
interpret the model (LambdaMARTModel in the case of LambdaMART). Name is the
model identifier you will use when making request to the ltr framework.
Features specifies the feature space that you want extracted when using this
model. All features that appear in the model params will be used for scoring and
must appear in the features list.  You can add extra features to the features
list that will be computed but not used in the model for scoring, which can
be useful for logging. Params are the LambdaMART specific parameters. In this
case we have 2 trees, one with 3 leaf nodes and one with 1 leaf node.

A good library for training LambdaMART ( http://sourceforge.net/p/lemur/wiki/RankLib/ ).
You will need to convert the RankLib model format to the format specified above.

# Deploy Models and Features
To send features run

`curl -XPUT 'http://localhost:8983/solr/collection1/schema/fstore' --data-binary @/path/features.json -H 'Content-type:application/json'`

To send models run

`curl -XPUT 'http://localhost:8983/solr/collection1/schema/mstore' --data-binary @/path/model.json -H 'Content-type:application/json'`


# View Models and Features
`curl -XGET 'http://localhost:8983/solr/collection1/schema/fstore'`
`curl -XGET 'http://localhost:8983/solr/collection1/schema/mstore'`


# Run a Rerank Query
Add to your original solr query
`rq={!ltr model=myModelName reRankDocs=25}`

The model name is the name of the model you sent to solr earlier.
The number of documents you want reranked, which can be larger than the
number you display, is reRankDocs.

### Pass in external information for external features
Add to your original solr query
`rq={!ltr reRankDocs=3 model=externalmodel efi.field1='text1' efi.field2='text2'}`

Where "field1" specifies the name of the customized field to be used by one
or more of your features, and text1 is the information to be pass in. As an
example that matches the earlier shown userTextTitleMatch feature one could do:

`rq={!ltr reRankDocs=3 model=externalmodel efi.user_text='Casablanca' efi.user_intent='movie'}`

# Extract features
To extract features you need to use the feature vector transformer + set the
fv parameter to true (this required parameter will be removed in the future).
For now you need to also use a dummy model with all the features you want to
extract inside the features parameter list of the model (this limitation will
also be changed in the future so you can extract features without a dummy model).

`fv=true&fl=*,score,[features]&rq={!ltr model=dummyModel reRankDocs=25}`

## Test the plugin with solr/example/techproducts in 6 steps

Solr provides some simple example of indices. In order to test the plugin with
the techproducts example please follow these steps

1. compile solr and the examples

    cd solr
    ant dist
    ant example

2. run the example

   ./bin/solr -e techproducts

3. stop it and install the plugin:

   ./bin/solr stop
   #create the lib folder
   mkdir example/techproducts/solr/techproducts/lib
   # install the plugin in the lib folder
   cp build/contrib/ltr/lucene-ltr-6.0.0-SNAPSHOT.jar example/techproducts/solr/techproducts/lib/
   # replace the original solrconfig with one importing all the ltr componenet
   cp contrib/ltr/example/solrconfig.xml example/techproducts/solr/techproducts/conf/

4. run the example again

   ./bin/solr -e techproducts

5. index some features and a model

   curl -XPUT 'http://localhost:8983/solr/techproducts/schema/fstore'  --data-binary "@./contrib/ltr/example/techproducts-features.json"  -H 'Content-type:application/json'
   curl -XPUT 'http://localhost:8983/solr/techproducts/schema/mstore'  --data-binary "@./contrib/ltr/example/techproducts-model.json"  -H 'Content-type:application/json'

6. have fun !

   # access to the default feature store
   http://localhost:8983/solr/techproducts/schema/fstore/_DEFAULT_
   # access to the model store
   http://localhost:8983/solr/techproducts/schema/mstore
   # perform a query using the model, and retrieve the features
   http://localhost:8983/solr/techproducts/query?indent=on&q=test&wt=json&rq={!ltr%20model=svm%20reRankDocs=25%20efi.query=%27test%27}&fl=*,[features],price,score,name&fv=true
