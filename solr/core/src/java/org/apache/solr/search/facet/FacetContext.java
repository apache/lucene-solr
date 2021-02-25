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
package org.apache.solr.search.facet;

import java.util.Map;

import org.apache.lucene.search.Query;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrIndexSearcher;

public class FacetContext {
  // Context info for actually executing a local facet command
  public static final int IS_SHARD=0x01;
  public static final int IS_REFINEMENT=0x02;
  public static final int SKIP_FACET=0x04;  // refinement: skip calculating this immediate facet, but proceed to specific sub-facets based on facetInfo

  FacetProcessor<?> processor;
  Map<String,Object> facetInfo; // refinement info for this node
  QueryContext qcontext;
  SolrQueryRequest req;  // TODO: replace with params?
  SolrIndexSearcher searcher;
  Query filter;  // TODO: keep track of as a DocSet or as a Query?
  DocSet base;
  FacetContext parent;
  boolean cache = true;
  int flags;
  FacetDebugInfo debugInfo;

  public void setDebugInfo(FacetDebugInfo debugInfo) {
    this.debugInfo = debugInfo;
  }

  public FacetDebugInfo getDebugInfo() {
    return debugInfo;
  }

  public boolean isShard() {
    return (flags & IS_SHARD) != 0;
  }

  public FacetProcessor<?> getFacetProcessor() {
    return processor;
  }

  public Map<String, Object> getFacetInfo() {
    return facetInfo;
  }

  public QueryContext getQueryContext() {
    return qcontext;
  }

  public SolrQueryRequest getRequest() {
    return req;
  }

  public SolrIndexSearcher getSearcher() {
    return searcher;
  }

  public Query getFilter() {
    return filter;
  }

  public DocSet getBase() {
    return base;
  }

  public FacetContext getParent() {
    return parent;
  }

  public int getFlags() {
    return flags;
  }

  /**
   * @param filter The filter for the bucket that resulted in this context/domain.  Can be null if this is the root context.
   * @param domain The resulting set of documents for this facet.
   */
  public FacetContext sub(Query filter, DocSet domain) {
    FacetContext ctx = new FacetContext();
    ctx.parent = this;
    ctx.base = domain;
    ctx.filter = filter;

    // carry over from parent
    ctx.cache = cache;
    ctx.flags = flags;
    ctx.qcontext = qcontext;
    ctx.req = req;
    ctx.searcher = searcher;

    return ctx;
  }
}
