![Apache Lucene Logo](lucene_green_300.gif)

# Apache Lucene™ ${project.version} Documentation

Lucene is a Java full-text search engine. Lucene is not a complete application, 
but rather a code library and API that can easily be used to add search capabilities
to applications.

This is the official documentation for **Apache Lucene ${project.version}**.
Additional documentation is available in the
[Wiki](https://cwiki.apache.org/confluence/display/lucene).

## Getting Started

The following section is intended as a "getting started" guide. It has three
audiences: first-time users looking to install Apache Lucene in their
application; developers looking to modify or base the applications they develop
on Lucene; and developers looking to become involved in and contribute to the
development of Lucene. The goal is to help you "get started". It does not go into great depth
on some of the conceptual or inner details of Lucene:

* [Lucene demo, its usage, and sources](demo/overview-summary.html#overview.description):
  Tutorial and walk-through of the command-line Lucene demo.
* [Introduction to Lucene's APIs](core/overview-summary.html#overview.description):
  High-level summary of the different Lucene packages. </li>
* [Analysis overview](core/org/apache/lucene/analysis/package-summary.html#package.description):
  Introduction to Lucene's analysis API.  See also the
  [TokenStream consumer workflow](core/org/apache/lucene/analysis/TokenStream.html).

## Reference Documents

* [Changes](changes/Changes.html): List of changes in this release.
* [System Requirements](SYSTEM_REQUIREMENTS.html): Minimum and supported Java versions.
* [Migration Guide](MIGRATE.html): What changed in Lucene ${project.majorVersion()}; how to migrate code from
  Lucene ${project.majorVersion()-1}.x.
* [JRE Version Migration](JRE_VERSION_MIGRATION.html): Information about upgrading between major JRE versions.
* [File Formats](core/org/apache/lucene/codecs/${defaultCodecPackage}/package-summary.html#package.description):
  Guide to the supported index format used by Lucene. This can be customized by using
  [an alternate codec](core/org/apache/lucene/codecs/package-summary.html#package.description).
* [Search and Scoring in Lucene](core/org/apache/lucene/search/package-summary.html#package.description):
  Introduction to how Lucene scores documents.
* [Classic Scoring Formula](core/org/apache/lucene/search/similarities/TFIDFSimilarity.html):
  Formula of Lucene's classic [Vector Space](https://en.wikipedia.org/wiki/Vector_Space_Model) implementation
  (look [here](core/org/apache/lucene/search/similarities/package-summary.html#package.description) for other models).
* [Classic QueryParser Syntax](queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package.description):
  Overview of the Classic QueryParser's syntax and features.

## API Javadocs

${projectList}
