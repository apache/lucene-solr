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
package org.apache.solr.util;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/**
 * Simple mock client that collects added documents and supports simple search by id
 * (both <code>{!term f=id}</code> and <code>id:</code> syntax is supported) or *:*.
 */
public class MockSearchableSolrClient extends SolrClient {
  public Map<String, Map<String, SolrInputDocument>> docs = new ConcurrentHashMap<>();

  private AtomicLong numUpdates = new AtomicLong();
  private AtomicLong numQueries = new AtomicLong();

  public void clear() {
    docs.clear();
  }

  @Override
  public synchronized NamedList<Object> request(@SuppressWarnings({"rawtypes"})SolrRequest request,
                                                String coll) throws SolrServerException, IOException {
    if (coll == null) {
      if (request.getParams() != null) {
        coll = request.getParams().get("collection");
      }
    }
    if (coll == null) {
      coll = "";
    }
    final String collection = coll;
    NamedList<Object> res = new NamedList<>();
    if (request instanceof UpdateRequest) {
      List<SolrInputDocument> docList = ((UpdateRequest) request).getDocuments();
      if (docList != null) {
        docList.forEach(doc -> {
          String id = (String) doc.getFieldValue("id");
          Objects.requireNonNull(id, doc.toString());
          docs.computeIfAbsent(collection, c -> new LinkedHashMap<>()).put(id, doc);
          numUpdates.incrementAndGet();
        });
      }
    } else if (request instanceof QueryRequest) {
      SolrParams params = request.getParams();
      if (params == null) {
        throw new UnsupportedOperationException("invalid request, no params: " + request);
      }
      String query = params.get("q");
      final SolrDocumentList lst = new SolrDocumentList();
      if (query != null) {
        if (query.startsWith("{!term f=id}") || query.startsWith("id:")) {
          numQueries.incrementAndGet();
          String id;
          if (query.startsWith("{!")) {
            id = query.substring(12);
          } else {
            id = query.substring(3);
          }
          Map<String, SolrInputDocument> collDocs = docs.get(collection);
          if (collDocs != null) {
            SolrInputDocument doc = collDocs.get(id);
            if (doc != null) {
              SolrDocument d = new SolrDocument();
              doc.forEach((k, f) -> f.forEach(v -> d.addField(k, v)));
              lst.add(d);
              lst.setNumFound(1);
            }
          }
        } else if (query.equals("*:*")) {
          numQueries.incrementAndGet();
          Map<String, SolrInputDocument> collDocs = docs.get(collection);
          if (collDocs != null) {
            lst.setNumFound(collDocs.size());
            collDocs.values().forEach(doc -> {
              SolrDocument d = new SolrDocument();
              doc.forEach((k, f) -> f.forEach(v -> d.addField(k, v)));
              lst.add(d);
            });
          }
        }
      }
      res.add("response", lst);
    } else {
      throw new UnsupportedOperationException("Unsupported request type: " + request.getClass() + ":" + request);
    }
    return res;
  }

  public long getNumUpdates() {
    return numUpdates.get();
  }

  public long getNumQueries() {
    return numQueries.get();
  }

  @Override
  public void close() throws IOException {

  }
}
