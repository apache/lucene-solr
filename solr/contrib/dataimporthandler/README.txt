                    Apache Solr - DataImportHandler

Introduction
------------
DataImportHandler is a data import tool for Solr which makes importing data from Databases, XML files and
HTTP data sources quick and easy.

Important Note
--------------
Although Solr strives to be agnostic of the Locale where the server is
running, some code paths in DataImportHandler are known to depend on the
System default Locale, Timezone, or Charset.  It is recommended that when
running Solr you set the following system properties:
  -Duser.language=xx -Duser.country=YY -Duser.timezone=ZZZ

where xx, YY, and ZZZ are consistent with any database server's configuration.
