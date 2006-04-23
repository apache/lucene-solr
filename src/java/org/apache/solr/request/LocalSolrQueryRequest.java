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
import org.apache.solr.util.StrUtils;
import org.apache.solr.util.NamedList;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.core.SolrCore;

import java.util.Map;
import java.util.HashMap;

/**
 * @author yonik
 * @version $Id$
 */
public class LocalSolrQueryRequest extends SolrQueryRequestBase {
  private final NamedList args;
  private final String query;
  private final String qtype;
  private final int start;
  private final int limit;

  public final static Map emptyArgs = new HashMap(0,1);

  public LocalSolrQueryRequest(SolrCore core, String query, String qtype, int start, int limit, Map args) {
    super(core);
    this.query=query;
    this.qtype=qtype;
    this.start=start;
    this.limit=limit;

    this.args = new NamedList();
    if (query!=null) this.args.add(SolrQueryRequestBase.QUERY_NAME, query);
    if (qtype!=null) this.args.add(SolrQueryRequestBase.QUERYTYPE_NAME, qtype);
    this.args.add(SolrQueryRequestBase.START_NAME, Integer.toString(start));
    this.args.add(SolrQueryRequestBase.ROWS_NAME, Integer.toString(limit));

    if (args!=null) this.args.addAll(args);
  }


  public LocalSolrQueryRequest(SolrCore core, NamedList args) {
    super(core);
    this.args=args;
    this.query=getStrParam(QUERY_NAME,null);
    this.qtype=getStrParam(QUERYTYPE_NAME,null);
    this.start=getIntParam(START_NAME,0);
    this.limit=getIntParam(ROWS_NAME,10);
  }


  public String getParam(String name) {
    Object value = args.get(name);
    if (value == null || value instanceof String) {
      return (String) value;
    }
    else {
      return ((String[]) value)[0];
    }
  }

  public String[] getParams(String name) {
    Object value = args.get(name);
    if (value instanceof String) {
      return new String[] {(String)value};
    } else {
      return (String[]) value;
    }
  }

  public String getQueryString() {
    return query;
  }

  // signifies the syntax and the handler that should be used
  // to execute this query.
  public String getQueryType() {
    return qtype;
  }


  // starting position in matches to return to client
  public int getStart() {
    return start;
  }

  // number of matching documents to return
  public int getLimit() {
    return limit;
  }

  final long startTime=System.currentTimeMillis();
  // Get the start time of this request in milliseconds
  public long getStartTime() {
    return startTime;
  }

  // The index searcher associated with this request
  RefCounted<SolrIndexSearcher> searcherHolder;
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

  public String getParamString() {
    StringBuilder sb = new StringBuilder(128);
    try {

      boolean first=true;
      if (query!=null) {
        if (!first) {
          sb.append('&');
        }
        first=false;
        sb.append("q=");
        StrUtils.partialURLEncodeVal(sb,query);
      }

      // null, "", and "standard" are all the default query handler.
      if (qtype!=null && !(qtype.equals("") || qtype.equals("standard"))) {
        if (!first) {
          sb.append('&');
        }
        first=false;
        sb.append("qt=");
        sb.append(qtype);
      }

      if (start!=0) {
        if (!first) {
          sb.append('&');
        }
        first=false;
        sb.append("start=");
        sb.append(start);
      }

      if (!first) {
        sb.append('&');
      }
      first=false;
      sb.append("rows=");
      sb.append(limit);

      if (args != null && args.size() > 0) {
        for (int i=0; i<args.size(); i++) {
          if (!first) {
            sb.append('&');
          }
          first=false;

          sb.append(args.getName(i));
          sb.append('=');
          StrUtils.partialURLEncodeVal(sb,args.getVal(i).toString());
        }
      }

    } catch (Exception e) {
      // should never happen... we only needed this because
      // partialURLEncodeVal can throw an IOException, but it
      // never will when adding to a StringBuilder.
      throw new RuntimeException(e);
    }

    return sb.toString();
  }


  public void close() {
    if (searcherHolder!=null) {
      searcherHolder.decref();
    }
  }



}


