Apache Solr Scripting Update Processor
===============================

# Introduction

The ScriptUpdateProcessor allows Java scripting engines to be used during the Solr document update processing, allowing dramatic flexibility in expressing custom document processing before being indexed.  It also allows hooks to commit, delete, etc, but add is the most common usage.  It is implemented as an UpdateProcessor to be placed in an UpdateChain.

# Getting Started

For information on how to get started please see:
 * [Solr Reference Guide's section on Update Request Processors](https://lucene.apache.org/solr/guide/update-request-processors.html)
