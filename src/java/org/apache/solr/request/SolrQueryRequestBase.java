/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.request;

import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrException;

/**
* @author yonik
* @version $Id: SolrQueryRequestBase.java,v 1.6 2005/06/12 02:36:09 yonik Exp $
*/
public abstract class SolrQueryRequestBase implements SolrQueryRequest {
 // some standard query argument names
 public static final String QUERY_NAME="q";
 public static final String START_NAME="start";
 public static final String ROWS_NAME="rows";
 public static final String XSL_NAME="xsl";
 public static final String QUERYTYPE_NAME="qt";


 protected final SolrCore core;

 public SolrQueryRequestBase(SolrCore core) {
   this.core=core;
 }

 public int getIntParam(String name) {
   String s = getParam(name);
   if (s==null) {
     throw new SolrException(500,"Missing required parameter '"+name+"' from " + this);
   }
   return Integer.parseInt(s);
 }

 public int getIntParam(String name, int defval) {
   String s = getParam(name);
   return s==null ? defval : Integer.parseInt(s);
 }

 public String getStrParam(String name) {
   String s = getParam(name);
   if (s==null) {
     throw new SolrException(500,"Missing required parameter '"+name+"' from " + this);
   }
   return s;
 }

 public String getStrParam(String name, String defval) {
   String s = getParam(name);
   return s==null ? defval : s;
 }

 public String getQueryString() {
   return getParam(QUERY_NAME);
 }

 public String getQueryType() {
   return getParam(QUERYTYPE_NAME);
 }

 // starting position in matches to return to client
 public int getStart() {
   return getIntParam(START_NAME, 0);
 }

 // number of matching documents to return
 public int getLimit() {
   return getIntParam(ROWS_NAME, 10);
 }


 protected final long startTime=System.currentTimeMillis();
 // Get the start time of this request in milliseconds
 public long getStartTime() {
   return startTime;
 }

 // The index searcher associated with this request
 protected RefCounted<SolrIndexSearcher> searcherHolder;
 public SolrIndexSearcher getSearcher() {
   // should this reach out and get a searcher from the core singleton, or
   // should the core populate one in a factory method to create requests?
   // or there could be a setSearcher() method that Solr calls

   if (searcherHolder==null) {
     searcherHolder = core.getSearcher();
   }

   return searcherHolder.get();
 }

 // The solr core (coordinator, etc) associated with this request
 public SolrCore getCore() {
   return core;
 }

 // The index schema associated with this request
 public IndexSchema getSchema() {
   return core.getSchema();
 }


 public void close() {
   if (searcherHolder!=null) {
     searcherHolder.decref();
   }
 }

 public String toString() {
   return this.getClass().getSimpleName() + '{' + getParamString() + '}';
 }

}
