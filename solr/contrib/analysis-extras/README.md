Apache Solr - Analysis Extras
=============================

The analysis-extras plugin provides additional analyzers that rely
upon large dependencies/dictionaries.

It includes integration with ICU for multilingual support,
analyzers for Chinese and Polish, and integration with
OpenNLP for multilingual tokenization, part-of-speech tagging
lemmatization, phrase chunking, and named-entity recognition.

Each of the jars below relies upon including `/dist/solr-analysis-extras-X.Y.jar`
in the `solrconfig.xml`

* ICU relies upon `lucene-libs/lucene-analyzers-icu-X.Y.jar`
and `lib/icu4j-X.Y.jar`

* Smartcn relies upon `lucene-libs/lucene-analyzers-smartcn-X.Y.jar`

* Stempel relies on `lucene-libs/lucene-analyzers-stempel-X.Y.jar`

* Morfologik relies on `lucene-libs/lucene-analyzers-morfologik-X.Y.jar`
and `lib/morfologik-*.jar`

* OpenNLP relies on `lucene-libs/lucene-analyzers-opennlp-X.Y.jar`
and `lib/opennlp-*.jar`
