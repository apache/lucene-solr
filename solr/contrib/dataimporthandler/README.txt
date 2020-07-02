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

Deprecation notice
------------------
This contrib module is deprecated as of v8.6, scheduled for removal in Solr 9.0.
The reason is that DIH is no longer being maintained in a manner we feel is necessary in order to keep it
healthy and secure. Also it was not designed to work with SolrCloud and does not meet current performance requirements.

The project hopes that the community will take over maintenance of DIH as a 3rd party package (See SOLR-14066 for more details). Please reach out to us at the dev@ mailing list if you want to help.

