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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.RequiredSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.StrParser;
import org.apache.solr.search.SyntaxError;

import static org.apache.solr.common.params.CommonParams.SORT;

public class LegacyFacet {
  private SolrParams params;
  private Map<String,Object> json;
  private Map<String,Object> currentCommand = null;  // always points to the current facet command
  private Map<String,Object> currentSubs; // always points to the current facet:{} block

  String facetValue;
  String key;
  SolrParams localParams;
  SolrParams orig;
  SolrParams required;

  Map<String, List<Subfacet>> subFacets;  // only parsed once

  public LegacyFacet(SolrParams params) {
    this.params = params;
    this.orig = params;
    this.json = new LinkedHashMap<>();
    this.currentSubs = json;
  }


  Map<String,Object> getLegacy() {
    subFacets = parseSubFacets(params);
    String[] queries = params.getParams(FacetParams.FACET_QUERY);
    if (queries != null) {
      for (String q : queries) {
        addQueryFacet(q);
      }
    }
    String[] fields = params.getParams(FacetParams.FACET_FIELD);
    if (fields != null) {
      for (String field : fields) {
        addFieldFacet(field);
      }
    }
    String[] ranges = params.getParams(FacetParams.FACET_RANGE);
    if (ranges != null) {
      for (String range : ranges) {
        addRangeFacet(range);
      }
    }
    // SolrCore.log.error("###################### JSON FACET:" + json);
    return json;
  }


  protected static class Subfacet {
    public String parentKey;
    public String type; // query, range, field
    public String value;  // the actual field or the query, including possible local params
  }


  protected static Map<String, List<Subfacet>> parseSubFacets(SolrParams params) {
    Map<String,List<Subfacet>> map = new HashMap<>();
    Iterator<String> iter = params.getParameterNamesIterator();

    String SUBFACET="subfacet.";
    while (iter.hasNext()) {
      String key = iter.next();

      if (key.startsWith(SUBFACET)) {
        List<String> parts = StrUtils.splitSmart(key, '.');
        if (parts.size() != 3) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "expected subfacet parameter name of the form subfacet.mykey.field, got:" + key);
        }
        Subfacet sub = new Subfacet();
        sub.parentKey = parts.get(1);
        sub.type = parts.get(2);
        sub.value = params.get(key);

        List<Subfacet> subs = map.get(sub.parentKey);
        if (subs == null) {
          subs = new ArrayList<>(1);
        }
        subs.add(sub);
        map.put(sub.parentKey, subs);
      }
    }

    return map;
  }


  protected void addQueryFacet(String q) {
    parseParams(FacetParams.FACET_QUERY, q);
    Map<String,Object> cmd = new HashMap<String,Object>(2);
    Map<String,Object> type = new HashMap<String,Object>(1);
    type.put("query", cmd);
    cmd.put("q", q);
    addSub(key, type);
    handleSubs(cmd);
  }

  protected void addRangeFacet(String field)  {
    parseParams(FacetParams.FACET_RANGE, field);
    Map<String,Object> cmd = new HashMap<String,Object>(5);
    Map<String,Object> type = new HashMap<String,Object>(1);
    type.put("range", cmd);

    String f = key;
    cmd.put("field", facetValue);
    cmd.put("start", required.getFieldParam(f,FacetParams.FACET_RANGE_START));
    cmd.put("end", required.getFieldParam(f,FacetParams.FACET_RANGE_END));
    cmd.put("gap", required.getFieldParam(f, FacetParams.FACET_RANGE_GAP));
    String[] p = params.getFieldParams(f, FacetParams.FACET_RANGE_OTHER);
    if (p != null) cmd.put("other", p.length==1 ? p[0] : Arrays.asList(p));
    p = params.getFieldParams(f, FacetParams.FACET_RANGE_INCLUDE);
    if (p != null) cmd.put("include", p.length==1 ? p[0] : Arrays.asList(p));

    final int mincount = params.getFieldInt(f,FacetParams.FACET_MINCOUNT, 0);
    cmd.put("mincount", mincount);

    boolean hardend = params.getFieldBool(f,FacetParams.FACET_RANGE_HARD_END,false);
    if (hardend) cmd.put("hardend", hardend);

    addSub(key, type);
    handleSubs(cmd);
  }

  protected void addFieldFacet(String field) {
    parseParams(FacetParams.FACET_FIELD, field);

    String f = key;  // the parameter to use for per-field parameters... f.key.facet.limit=10

    int offset = params.getFieldInt(f, FacetParams.FACET_OFFSET, 0);
    int limit = params.getFieldInt(f, FacetParams.FACET_LIMIT, 10);

    int mincount = params.getFieldInt(f, FacetParams.FACET_MINCOUNT, 1);

    boolean missing = params.getFieldBool(f, FacetParams.FACET_MISSING, false);

    // default to sorting if there is a limit.
    String sort = params.getFieldParam(f, FacetParams.FACET_SORT, limit>0 ? FacetParams.FACET_SORT_COUNT : FacetParams.FACET_SORT_INDEX);
    String prefix = params.getFieldParam(f, FacetParams.FACET_PREFIX);

    Map<String,Object> cmd = new HashMap<>();
    cmd.put("field", facetValue);
    if (offset != 0) cmd.put("offset", offset);
    if (limit != 10) cmd.put("limit", limit);
    if (mincount != 1) cmd.put("mincount", mincount);
    if (missing) cmd.put("missing", missing);
    if (prefix != null) cmd.put("prefix", prefix);
    if (sort.equals("count")) {
      // our default
    } else if (sort.equals("index")) {
      cmd.put(SORT, "index asc");
    } else {
      cmd.put(SORT, sort);  // can be sort by one of our stats
    }

    Map<String,Object> type = new HashMap<>(1);
    type.put("terms", cmd);

    addSub(key, type);
    handleSubs(cmd);
  }

  private void handleSubs(Map<String,Object> cmd) {
    Map<String,Object> savedCmd = currentCommand;
    Map<String,Object> savedSubs = currentSubs;
   try {
     currentCommand = cmd;
     currentSubs = null;

     // parse stats for this facet
     String[] stats = params.getFieldParams(key, "facet.stat");
     if (stats != null) {
       for (String stat : stats) {
         addStat(stat);
       }
     }

     List<Subfacet> subs = subFacets.get(key);
     if (subs != null) {
       for (Subfacet subfacet : subs) {
         if ("field".equals(subfacet.type)) {
           addFieldFacet(subfacet.value);
         } else if ("query".equals(subfacet.type)) {
           addQueryFacet(subfacet.value);
         } else if ("range".equals(subfacet.type)) {
           addQueryFacet(subfacet.value);
         }
       }
     }


   } finally {
     currentCommand = savedCmd;
     currentSubs = savedSubs;
   }
  }


  private void addStat(String val) {
    StrParser sp = new StrParser(val);
    int start = 0;
    sp.eatws();
    if (sp.pos >= sp.end) addStat(val, val);

    // try key:func() format
    String key = null;
    String funcStr = val;

    if (key == null) {
      key = SolrReturnFields.getFieldName(sp);
      if (key != null && sp.opt(':')) {
        // OK, we got the key
        funcStr = val.substring(sp.pos);
      } else {
        // an invalid key... it must not be present.
        sp.pos = start;
        key = null;
      }
    }

    if (key == null) {
      key = funcStr;  // not really ideal
    }

    addStat(key, funcStr);
  }

  private void addStat(String key, String val) {
    if ("count".equals(val) || "count()".equals(val)) return;  // we no longer have a count function, we always return the count
    getCurrentSubs().put(key, val);
  }

  private void addSub(String key, Map<String,Object> sub) {
    getCurrentSubs().put(key, sub);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private Map<String,Object> getCurrentSubs() {
    if (currentSubs == null) {
      currentSubs = new LinkedHashMap();
      currentCommand.put("facet", currentSubs);
    }
    return currentSubs;
  }



  protected void parseParams(String type, String param)  {
    facetValue = param;
    key = param;

    try {
      localParams = QueryParsing.getLocalParams(param, orig);

      if (localParams == null) {
        params = orig;
        required = new RequiredSolrParams(params);
        // setupStats();
        return;
      }

      params = SolrParams.wrapDefaults(localParams, orig);
      required = new RequiredSolrParams(params);

      // remove local params unless it's a query
      if (type != FacetParams.FACET_QUERY) {
        facetValue = localParams.get(CommonParams.VALUE);
      }

      // reset set the default key now that localParams have been removed
      key = facetValue;

      // allow explicit set of the key
      key = localParams.get(CommonParams.OUTPUT_KEY, key);

      // setupStats();
    } catch (SyntaxError e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }


}
