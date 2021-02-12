package org.apache.solr.common.util;

import org.apache.logging.log4j.core.appender.nosql.NoSqlObject;
import org.apache.solr.common.SolrInputDocument;

import java.util.ArrayList;
import java.util.List;

public class SolrObject implements NoSqlObject<SolrInputDocument> {
  private SolrInputDocument solrDocument;

  SolrObject() {
    solrDocument = new SolrInputDocument();
  }

  @Override
  public void set(String field, Object value) {
    solrDocument.addField(field, value);
  }

  @Override
  public void set(String field, NoSqlObject<SolrInputDocument> value) {
    solrDocument.addField(field, value.unwrap());
  }

  @Override
  public void set(String field, Object[] values) {
    final SolrInputDocument doc = new SolrInputDocument();
    for (final Object value : values) {
      doc.addField(field, value);
    }
    solrDocument.addField(field, doc);
  }

  @Override
  public void set(String field,  NoSqlObject<SolrInputDocument>[] values) {
    List<SolrInputDocument> docs = new ArrayList<>(values.length);
    for (NoSqlObject<SolrInputDocument> value : values) {
      docs.add(value.unwrap());
    }
    solrDocument.addField(field, docs);
  }

  @Override
  public SolrInputDocument unwrap() {
    return this.solrDocument;
  }
}
