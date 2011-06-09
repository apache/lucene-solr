Although Solr strives to be agnostic of the Locale where the server is
running, some code paths in DataImportHandler are known to depend on the
System default Locale, Timezone, or Charset.  It is recommended that when
running Solr you set the following system properties:
  -Duser.language=xx -Duser.country=YY -Duser.timezone=ZZZ

where xx, YY, and ZZZ are consistent with any database server's configuration.
