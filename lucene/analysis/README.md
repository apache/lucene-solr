<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 -->

#Analysis

##Introduction

The Analysis Module provides analysis capabilities to Lucene and Solr
applications.

The Lucene web site is at:
  http://lucene.apache.org/

Please join the Lucene-User mailing list by sending a message to:
  java-user-subscribe@lucene.apache.org

##Files

####lucene-analyzers-common-XX.jar
  The primary analysis module library, containing general-purpose analysis
  components and support for various languages.

####lucene-analyzers-icu-XX.jar
  An add-on analysis library that provides improved Unicode support via
  International Components for Unicode (ICU). Note: this module depends on
  the ICU4j jar file (version >= 4.6.0)

####lucene-analyzers-kuromoji-XX.jar
  An analyzer with morphological analysis for Japanese.

####lucene-analyzers-morfologik-XX.jar
  An analyzer using the Morfologik stemming library.

####lucene-analyzers-nori-XX.jar
  An analyzer with morphological analysis for Korean.

####lucene-analyzers-opennlp-XX.jar
  An analyzer using the OpenNLP natural-language processing library.

####lucene-analyzers-phonetic-XX.jar
  An add-on analysis library that provides phonetic encoders via Apache
  Commons-Codec. Note: this module depends on the commons-codec jar 
  file
  
####lucene-analyzers-smartcn-XX.jar
  An add-on analysis library that provides word segmentation for Simplified
  Chinese.

####lucene-analyzers-stempel-XX.jar
  An add-on analysis library that contains a universal algorithmic stemmer,
  including tables for the Polish language.

  The source code for the libraries.
  
    common/src/java
    icu/src/java
    kuromoji/src/java
    morfologik/src/java
    nori/src/java
    opennlp/src/java
    phonetic/src/java
    smartcn/src/java
    stempel/src/java

  Unit tests for the libraries.
  
    common/src/test
    icu/src/test
    kuromoji/src/test
    morfologik/src/test
    nori/src/test
    opennlp/src/test
    phonetic/src/test
    smartcn/src/test
    stempel/src/test
