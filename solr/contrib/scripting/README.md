Welcome to Apache Solr Scripting!
===============================

# Introduction

The Scripting contrib module pulls together various scripting related functions.  

Today, the ScriptUpdateProcessorFactory allows Java scripting engines to support scripts written in languages such as JavaScript, Ruby, Python, and Groovy to be used during Solr document update processing, allowing dramatic flexibility in expressing custom document processing before being indexed.  It exposes hooks for commit, delete, etc, but add is the most common usage.  It is implemented as an UpdateProcessor to be placed in an UpdateChain.

## Getting Started

For information on how to get started please see:
 * [Solr Reference Guide's section on Update Request Processors](https://lucene.apache.org/solr/guide/update-request-processors.html)
  * [Solr Reference Guide's section on ScriptUpdateProcessorFactory](https://lucene.apache.org/solr/guide/script-update-processor.html)
