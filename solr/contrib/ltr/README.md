Apache Solr Learning to Rank
========

This is the main [learning to rank integrated into solr](http://www.slideshare.net/lucidworks/learning-to-rank-in-solr-presented-by-michael-nilsson-diego-ceccarelli-bloomberg-lp)
repository.
[Read up on learning to rank](https://en.wikipedia.org/wiki/Learning_to_rank)

Apache Solr Learning to Rank (LTR) provides a way for you to extract features
directly inside Solr for use in training a machine learned model.  You can then
deploy that model to Solr and use it to rerank your top X search results.

# Test the plugin with solr/example/techproducts in a few easy steps!

Solr provides some simple example of indices. In order to test the plugin with
the techproducts example please follow these steps.
If you want to install the plugin on your instance of Solr, please refer
to the [Solr Ref Guide](https://cwiki.apache.org/confluence/display/solr/Result+Reranking).

1. Compile solr and the examples

    `cd solr`
    `ant dist`
    `ant server`

2. Run the example to setup the index, enabling the ltr plugin 

   `./bin/solr -e techproducts -Dsolr.ltr.enabled=true`

3. Deploy features and a model

      `curl -XPUT 'http://localhost:8983/solr/techproducts/schema/feature-store'  --data-binary "@./contrib/ltr/example/techproducts-features.json"  -H 'Content-type:application/json'`

      `curl -XPUT 'http://localhost:8983/solr/techproducts/schema/model-store'  --data-binary "@./contrib/ltr/example/techproducts-model.json"  -H 'Content-type:application/json'`

4. Have fun !

     * Access to the default feature store

       http://localhost:8983/solr/techproducts/schema/feature-store/\_DEFAULT\_
     * Access to the model store

       http://localhost:8983/solr/techproducts/schema/model-store
     * Perform a reranking query using the model, and retrieve the features

       http://localhost:8983/solr/techproducts/query?indent=on&q=test&wt=json&rq={!ltr%20model=linear%20reRankDocs=25%20efi.user_query=%27test%27}&fl=[features],price,score,name


BONUS: Train an actual machine learning model

1. Download and install [liblinear](https://www.csie.ntu.edu.tw/~cjlin/liblinear/)

2. Change `contrib/ltr/example/config.json` "trainingLibraryLocation" to point to the train directory where you installed liblinear.

3. Extract features, train a reranking model, and deploy it to Solr.

  `cd  contrib/ltr/example`

  `python  train_and_upload_demo_model.py -c config.json`

   This script deploys your features from `config.json` "featuresFile" to Solr.  Then it takes the relevance judged query
   document pairs of "userQueriesFile" and merges it with the features extracted from Solr into a training
   file.  That file is used to train a linear model, which is then deployed to Solr for you to rerank results.

4. Search and rerank the results using the trained model

   http://localhost:8983/solr/techproducts/query?indent=on&q=test&wt=json&rq={!ltr%20model=ExampleModel%20reRankDocs=25%20efi.user_query=%27test%27}&fl=price,score,name

# Changes to solrconfig.xml
```xml
<config>
  ...

  <!-- Query parser used to rerank top docs with a provided model -->
  <queryParser name="ltr" class="org.apache.solr.ltr.search.LTRQParserPlugin" />

  <!--  Transformer that will encode the document features in the response.
  For each document the transformer will add the features as an extra field
  in the response. The name of the field will be the the name of the
  transformer enclosed between brackets (in this case [features]).
  In order to get the feature vector you will have to
  specify that you want the field (e.g., fl="*,[features])  -->

  <transformer name="features" class="org.apache.solr.ltr.response.transform.LTRFeatureLoggerTransformerFactory">
    <str name="fvCacheName">QUERY_DOC_FV</str>
  </transformer>

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

# Defining Features
In the learning to rank plugin, you can define features in a feature space
using standard Solr queries. As an example:

###### features.json
```json
[
{ "name": "isBook",
  "class": "org.apache.solr.ltr.feature.SolrFeature",
  "params":{ "fq": ["{!terms f=category}book"] }
},
{
  "name":  "documentRecency",
  "class": "org.apache.solr.ltr.feature.SolrFeature",
  "params": {
      "q": "{!func}recip( ms(NOW,publish_date), 3.16e-11, 1, 1)"
  }
},
{
  "name":"originalScore",
  "class":"org.apache.solr.ltr.feature.OriginalScoreFeature",
  "params":{}
},
{
  "name" : "userTextTitleMatch",
  "class" : "org.apache.solr.ltr.feature.SolrFeature",
  "params" : { "q" : "{!field f=title}${user_text}" }
},
 {
   "name" : "userFromMobile",
   "class" : "org.apache.solr.ltr.feature.ValueFeature",
   "params" : { "value" : "${userFromMobile}", "required":true }
 }
]
```

Defines five features. Anything that is a valid Solr query can be used to define
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
field 'user_text'.  You can provide default values for external features as
well by specifying ${myField:myDefault}, similar to how you would in a Solr config.
In this case, the fifth feature (userFromMobile) will be looking for an external parameter
called 'userFromMobile' passed in through the request, if the ValueFeature is :
required=true, it will throw an exception if the external feature is not passed
required=false, it will silently ignore the feature and avoid the scoring ( at Document scoring time, the model will consider 0 as feature value)
The advantage in defining a feature as not required, where possible, is to avoid wasting caching space and time in calculating the featureScore.
See the [Run a Rerank Query](#run-a-rerank-query) section for how to pass in external information.

### Custom Features
Custom features can be created by extending from
org.apache.solr.ltr.feature.Feature, however this is generally not recommended.
The majority of features should be possible to create using the methods described
above.

# Defining Models
Currently the Learning to Rank plugin supports 2 generalized forms of
models: 1. Linear Model i.e. [RankSVM](http://www.cs.cornell.edu/people/tj/publications/joachims_02c.pdf), [Pranking](https://papers.nips.cc/paper/2023-pranking-with-ranking.pdf)
and 2. Multiple Additive Trees i.e. [LambdaMART](http://research.microsoft.com/pubs/132652/MSR-TR-2010-82.pdf), [Gradient Boosted Regression Trees (GBRT)](https://papers.nips.cc/paper/3305-a-general-boosting-method-and-its-application-to-learning-ranking-functions-for-web-search.pdf)

### Linear
If you'd like to introduce a bias set a constant feature
to the bias value you'd like and make a weight of 1.0 for that feature.

###### model.json
```json
{
    "class":"org.apache.solr.ltr.model.LinearModel",
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

This is an example of a toy Linear model. Class specifies the class to be
using to interpret the model. Name is the model identifier you will use 
when making request to the ltr framework. Features specifies the feature 
space that you want extracted when using this model. All features that 
appear in the model params will be used for scoring and must appear in 
the features list.  You can add extra features to the features list that 
will be computed but not used in the model for scoring, which can be useful 
for logging. Params are the Linear parameters.

Good library for training SVM, an example of a Linear model, is 
(https://www.csie.ntu.edu.tw/~cjlin/liblinear/ , https://www.csie.ntu.edu.tw/~cjlin/libsvm/) . 
You will need to convert the libSVM model format to the format specified above.

### Multiple Additive Trees

###### model2.json
```json
{
    "class":"org.apache.solr.ltr.model.MultipleAdditiveTreesModel",
    "name":"multipleadditivetreesmodel",
    "features":[
        { "name": "userTextTitleMatch"},
        { "name": "originalScore"}
    ],
    "params":{
        "trees": [
            {
                "weight" : 1,
                "root": {
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
                "root": {
                    "value" : -10
                }
            }
        ]
    }
}
```
This is an example of a toy Multiple Additive Trees. Class specifies the class to be using to
interpret the model. Name is the
model identifier you will use when making request to the ltr framework.
Features specifies the feature space that you want extracted when using this
model. All features that appear in the model params will be used for scoring and
must appear in the features list.  You can add extra features to the features
list that will be computed but not used in the model for scoring, which can
be useful for logging. Params are the Multiple Additive Trees specific parameters. In this
case we have 2 trees, one with 3 leaf nodes and one with 1 leaf node.

A good library for training LambdaMART, an example of Multiple Additive Trees, is ( http://sourceforge.net/p/lemur/wiki/RankLib/ ).
You will need to convert the RankLib model format to the format specified above.

# Deploy Models and Features
To send features run

`curl -XPUT 'http://localhost:8983/solr/collection1/schema/feature-store' --data-binary @/path/features.json -H 'Content-type:application/json'`

To send models run

`curl -XPUT 'http://localhost:8983/solr/collection1/schema/model-store' --data-binary @/path/model.json -H 'Content-type:application/json'`


# View Models and Features
`curl -XGET 'http://localhost:8983/solr/collection1/schema/feature-store'`

`curl -XGET 'http://localhost:8983/solr/collection1/schema/model-store'`

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
To extract features you need to use the feature vector transformer `features`

`fl=*,score,[features]&rq={!ltr model=yourModel reRankDocs=25}`

If you use `[features]` together with your reranking model, it will return
the array of features used by your model. Otherwise you can just ask solr to
produce the features without doing the reranking:

`fl=*,score,[features store=yourFeatureStore format=[dense|sparse] ]`

This will return the values of the features in the given store. The format of the 
extracted features will be based on the format parameter. The default is sparse.

# Assemble training data
In order to train a learning to rank model you need training data. Training data is
what "teaches" the model what the appropriate weight for each feature is. In general
training data is a collection of queries with associated documents and what their ranking/score
should be. As an example:
```
secretary of state|John Kerry|0.66|CROWDSOURCE
secretary of state|Cesar A. Perales|0.33|CROWDSOURCE
secretary of state|New York State|0.0|CROWDSOURCE
secretary of state|Colorado State University Secretary|0.0|CROWDSOURCE

microsoft ceo|Satya Nadella|1.0|CLICK_LOG
microsoft ceo|Microsoft|0.0|CLICK_LOG
microsoft ceo|State|0.0|CLICK_LOG
microsoft ceo|Secretary|0.0|CLICK_LOG
```
In this example the first column indicates the query, the second column indicates a unique id for that doc,
the third column indicates the relative importance or relevance of that doc, and the fourth column indicates the source.
There are 2 primary ways you might collect data for use with your machine learning algorithim. The first
is to collect the clicks of your users given a specific query. There are many ways of preparing this data
to train a model (http://www.cs.cornell.edu/people/tj/publications/joachims_etal_05a.pdf). The general idea
is that if a user sees multiple documents and clicks the one lower down, that document should be scored higher
than the one above it. The second way is explicitly through a crowdsourcing platform like Mechanical Turk or
CrowdFlower. These platforms allow you to show human workers documents associated with a query and have them
tell you what the correct ranking should be.

At this point you'll need to collect feature vectors for each query document pair. You can use the information
from the Extract features section above to do this. An example script has been included in example/train_and_upload_demo_model.py.

# Explanation of the core reranking logic
An LTR model is plugged into the ranking through the [LTRQParserPlugin](/solr/contrib/ltr/src/java/org/apache/solr/ltr/search/LTRQParserPlugin.java). The plugin will
read from the request the model, an instance of [LTRScoringModel](/solr/contrib/ltr/src/java/org/apache/solr/ltr/model/LTRScoringModel.java),
plus other parameters. The plugin will generate an LTRQuery, a particular [ReRankQuery](/solr/core/src/java/org/apache/solr/search/AbstractReRankQuery.java).
It wraps the original solr query for the first pass ranking, and uses the provided model in an
[LTRScoringQuery](/solr/contrib/ltr/src/java/org/apache/solr/ltr/LTRScoringQuery.java) to
rescore and rerank the top documents.  The LTRScoringQuery will take care of computing the values of all the
[features](/solr/contrib/ltr/src/java/org/apache/solr/ltr/feature/Feature.java) and then will delegate the final score
generation to the LTRScoringModel.

# Speeding up the weight creation with threads
About half the time for ranking is spent in the creation of weights for each feature used in ranking. If the number of features is significantly high (say, 500 or more), this increases the ranking overhead proportionally. To alleviate this problem, parallel weight creation is provided as a configurable option. In order to use this feature, the following lines need to be added to the solrconfig.xml
```xml

<config>
  <!-- Query parser used to rerank top docs with a provided model -->
  <queryParser name="ltr" class="org.apache.solr.ltr.search.LTRQParserPlugin">
     <int name="threadModule.totalPoolThreads">10</int> <!-- Maximum threads to share for all requests -->
     <int name="threadModule.numThreadsPerRequest">5</int> <!-- Maximum threads to use for a single request -->
  </queryParser>
  
  <!-- Transformer for extracting features -->
  <transformer name="features" class="org.apache.solr.ltr.response.transform.LTRFeatureLoggerTransformerFactory">
     <str name="fvCacheName">QUERY_DOC_FV</str>
     <int name="threadModule.totalPoolThreads">10</int> <!-- Maximum threads to share for all requests -->
     <int name="threadModule.numThreadsPerRequest">5</int> <!-- Maximum threads to use for a single request -->
  </transformer>
</config>

```
  
The threadModule.totalPoolThreads option limits the total number of threads to be used across all query instances at any given time. threadModule.numThreadsPerRequest limits the number of threads used to process a single query. In the above example, 10 threads will be used to services all queries and a maximum of 5 threads to service a single query. If the solr instance is expected to receive no more than one query at a time, it is best to set both these numbers to the same value. If multiple queries need to be serviced simultaneously, the numbers can be adjusted based on the expected response times. If the value of threadModule.numThreadsPerRequest is higher, the response time for a single query will be improved upto a point. If multiple queries are serviced simultaneously, the threadModule.totalPoolThreads imposes a contention between the queries if (threadModule.numThreadsPerRequest*total parallel queries > threadModule.totalPoolThreads).

