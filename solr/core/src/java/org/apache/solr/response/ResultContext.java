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

import java.util.Iterator;
import java.util.function.Predicate;

import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocList;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * A class to hold the QueryResult and the Query
 * 
 *
 */
public abstract class ResultContext {

  public abstract DocList getDocList();

  public abstract ReturnFields getReturnFields();

  public abstract SolrIndexSearcher getSearcher();

  public abstract Query getQuery();

  // TODO: any reason to allow for retrieval of any filters as well?

  /** Note: do not use the request to get the searcher!  A cross-core request may have a different
   *  searcher (for the other core) than the original request.
   */
  public abstract SolrQueryRequest getRequest();

  public boolean wantsScores() {
    return getReturnFields() != null && getReturnFields().wantsScore() && getDocList() != null && getDocList().hasScores();
  }

  public Iterator<SolrDocument> getProcessedDocuments() {
    return new DocsStreamer(this);
  }
  public static final ThreadLocal<Predicate<String>>  READASBYTES = new ThreadLocal<>();
}


