/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.VERSION_FIELD;

/**
 * Uses tuples to identify the uniqueKey values of documents to be deleted
 */
public final class DeleteStream extends UpdateStream implements Expressible {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final String ID_TUPLE_KEY = "id";

  public DeleteStream(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    final Explanation explanation = super.toExplanation(factory);
    explanation.setExpression("Delete docs from " + getCollectionName());
    
    return explanation;
  }

  /** 
   * {@link DeleteStream} returns <code>false</code> so that Optimistic Concurrency Constraints are 
   * respected by default when using this stream to wrap a {@link SearchStream} query.
   */
  @Override
  protected boolean defaultPruneVersionField() {
    return false;
  }
  
  /**
   * Overrides implementation to extract the <code>"id"</code> and <code>"_version_"</code> 
   * (if included) from each document and use that information to construct a "Delete By Id" request.  
   * Any other fields (ie: Tuple values) are ignored.
   */
  @Override
  protected void uploadBatchToCollection(List<SolrInputDocument> documentBatch) throws IOException {
    if (documentBatch.size() == 0) {
      return;
    }

    try {
      // convert each doc into a deleteById request...
      final UpdateRequest req = new UpdateRequest();
      for (SolrInputDocument doc : documentBatch) {
        final String id = doc.getFieldValue(ID_TUPLE_KEY).toString();
        final Long version = getVersion(doc);
        req.deleteById(id, version);
      }
      req.process(getCloudSolrClient(), getCollectionName());
    } catch (SolrServerException | NumberFormatException| IOException e) {
      log.warn("Unable to delete documents from collection due to unexpected error.", e);
      String className = e.getClass().getName();
      String message = e.getMessage();
      throw new IOException(String.format(Locale.ROOT,"Unexpected error when deleting documents from collection %s- %s:%s", getCollectionName(), className, message));
    }
  }

  /**
   * Helper method that can handle String values when dealing with odd 
   * {@link Tuple} -&gt; {@link SolrInputDocument} conversions 
   * (ie: <code>tuple(..)</code> in tests)
   */
  private static Long getVersion(final SolrInputDocument doc) throws NumberFormatException {
    if (! doc.containsKey(VERSION_FIELD)) {
      return null;
    }
    final Object v = doc.getFieldValue(VERSION_FIELD);
    if (null == v) {
      return null;
    }
    if (v instanceof Long) {
      return (Long)v;
    }
    return Long.parseLong(v.toString());
  }
}
