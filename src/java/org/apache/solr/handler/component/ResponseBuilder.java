/**
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

package org.apache.solr.handler.component;

import org.apache.lucene.search.Query;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.RTimer;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SortSpec;

import java.util.List;

/**
 * TODO!
 * 
 * @version $Id$
 * @since solr 1.3
 */
public class ResponseBuilder 
{
  private boolean needDocList = false;
  private boolean needDocSet = false;
  private int fieldFlags = 0;
  private boolean debug = false;

  private QParser qparser = null;
  private String queryString = null;
  private Query query = null;
  private List<Query> filters = null;
  private SortSpec sortSpec = null;
  
  private DocListAndSet results = null;
  private NamedList<Object> debugInfo = null;
  private RTimer timer = null;
  
  private Query highlightQuery = null;

  //-------------------------------------------------------------------------
  
  public boolean isDebug() {
    return debug;
  }

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  public NamedList<Object> getDebugInfo() {
    return debugInfo;
  }

  public void setDebugInfo(NamedList<Object> debugInfo) {
    this.debugInfo = debugInfo;
  }

  public int getFieldFlags() {
    return fieldFlags;
  }

  public void setFieldFlags(int fieldFlags) {
    this.fieldFlags = fieldFlags;
  }

  public List<Query> getFilters() {
    return filters;
  }

  public void setFilters(List<Query> filters) {
    this.filters = filters;
  }

  public Query getHighlightQuery() {
    return highlightQuery;
  }

  public void setHighlightQuery(Query highlightQuery) {
    this.highlightQuery = highlightQuery;
  }

  public boolean isNeedDocList() {
    return needDocList;
  }

  public void setNeedDocList(boolean needDocList) {
    this.needDocList = needDocList;
  }

  public boolean isNeedDocSet() {
    return needDocSet;
  }

  public void setNeedDocSet(boolean needDocSet) {
    this.needDocSet = needDocSet;
  }

  public QParser getQparser() {
    return qparser;
  }

  public void setQparser(QParser qparser) {
    this.qparser = qparser;
  }

  public String getQueryString() {
    return queryString;
  }

  public void setQueryString(String qstr) {
    this.queryString = qstr;
  }

  public Query getQuery() {
    return query;
  }

  public void setQuery(Query query) {
    this.query = query;
  }

  public DocListAndSet getResults() {
    return results;
  }

  public void setResults(DocListAndSet results) {
    this.results = results;
  }

  public SortSpec getSortSpec() {
    return sortSpec;
  }

  public void setSortSpec(SortSpec sort) {
    this.sortSpec = sort;
  }

  public RTimer getTimer() {
    return timer;
  }

  public void setTimer(RTimer timer) {
    this.timer = timer;
  }
}
