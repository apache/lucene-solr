package org.apache.solr.handler.extraction;

import org.apache.tika.metadata.Metadata;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.schema.IndexSchema;

import java.util.Collection;


/**
 *
 *
 **/
public class SolrContentHandlerFactory {
  protected Collection<String> dateFormats;

  public SolrContentHandlerFactory(Collection<String> dateFormats) {
    this.dateFormats = dateFormats;
  }

  public SolrContentHandler createSolrContentHandler(Metadata metadata, SolrParams params, IndexSchema schema) {
    return new SolrContentHandler(metadata, params, schema,
            dateFormats);
  }
}
