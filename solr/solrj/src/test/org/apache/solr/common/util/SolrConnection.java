package org.apache.solr.common.util;

import org.apache.logging.log4j.core.appender.nosql.NoSqlConnection;
import org.apache.logging.log4j.core.appender.nosql.NoSqlObject;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;

public class SolrConnection implements NoSqlConnection<SolrInputDocument, SolrObject> {

  private final Http2SolrClient solrClient;
  private final String collectionName;
  private final String description;

  public SolrConnection(final Http2SolrClient solrClient, final String collectionName, final String description) {
    this.solrClient = solrClient;
    this.collectionName = collectionName;
    this.description = "solr{ " + description + " }";
  }

  @Override
  public SolrObject createObject() {
    return new SolrObject();
  }

  @Override
  public SolrObject[] createList(int length) {
    return new SolrObject[length];
  }

  @Override
  public void insertObject(NoSqlObject<SolrInputDocument> object) {
    UpdateRequest update = new UpdateRequest();
    update.add(object.unwrap());
    try {
      solrClient.request(update);
    } catch (SolrServerException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() {

  }

  @Override
  public boolean isClosed() {
    return false;
  }
}
