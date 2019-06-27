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
package org.apache.solr.response;

import org.apache.lucene.search.Query;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocList;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrIndexSearcher;

public class BasicResultContext extends ResultContext {
  private DocList docList;
  private ReturnFields returnFields;
  private SolrIndexSearcher searcher;
  private Query query;
  private SolrQueryRequest req;

  public BasicResultContext(DocList docList, ReturnFields returnFields, SolrIndexSearcher searcher, Query query, SolrQueryRequest req) {
    assert docList!=null;
    this.docList = docList;
    this.returnFields = returnFields;
    this.searcher = searcher;
    this.query = query;
    this.req = req;
  }

  public BasicResultContext(ResponseBuilder rb) {
    this(rb.getResults().docList, rb.rsp.getReturnFields(), null, rb.getQuery(), rb.req);
  }

  public BasicResultContext(ResponseBuilder rb, DocList docList) {
    this(docList, rb.rsp.getReturnFields(), null, rb.getQuery(), rb.req);
  }

  @Override
  public DocList getDocList() {
    return docList;
  }

  @Override
  public ReturnFields getReturnFields() {
    return returnFields;
  }

  @Override
  public SolrIndexSearcher getSearcher() {
    if (searcher != null) return searcher;
    if (req != null) return req.getSearcher();
    return null;
  }

  @Override
  public Query getQuery() {
    return query;
  }

  @Override
  public SolrQueryRequest getRequest() {
    return req;
  }
}
