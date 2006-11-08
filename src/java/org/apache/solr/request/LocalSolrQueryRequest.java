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

package org.apache.solr.request;

import org.apache.solr.util.NamedList;
import org.apache.solr.core.SolrCore;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

// With the addition of SolrParams, this class isn't needed for much anymore... it's currently
// retained more for backward compatibility.

/**
 * @author yonik
 * @version $Id$
 */
public class LocalSolrQueryRequest extends SolrQueryRequestBase {
  public final static Map emptyArgs = new HashMap(0,1);

  protected static SolrParams makeParams(String query, String qtype, int start, int limit, Map args) {
    Map<String,String[]> map = new HashMap<String,String[]>();
    for (Iterator iter = args.entrySet().iterator(); iter.hasNext();) {
      Map.Entry e = (Map.Entry)iter.next();
      String k = e.getKey().toString();
      Object v = e.getValue();
      if (v instanceof String[]) map.put(k,(String[])v);
      else map.put(k,new String[]{v.toString()});
    }
    if (query!=null) map.put(SolrParams.Q, new String[]{query});
    if (qtype!=null) map.put(SolrParams.QT, new String[]{qtype});
    map.put(SolrParams.START, new String[]{Integer.toString(start)});
    map.put(SolrParams.ROWS, new String[]{Integer.toString(limit)});
    return new MultiMapSolrParams(map);
  }

  public LocalSolrQueryRequest(SolrCore core, String query, String qtype, int start, int limit, Map args) {
    super(core,makeParams(query,qtype,start,limit,args));
  }

  public LocalSolrQueryRequest(SolrCore core, NamedList args) {
    super(core, SolrParams.toSolrParams(args));
  }

  public LocalSolrQueryRequest(SolrCore core, Map<String,String[]> args) {
    super(core, new MultiMapSolrParams(args));
  }
}

