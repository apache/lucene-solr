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
package org.apache.solr.request.json;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.macro.MacroExpander;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

import static org.apache.solr.common.params.CommonParams.JSON;
import static org.apache.solr.common.params.CommonParams.SORT;

public class RequestUtil {
  /**
   * Set default-ish params on a SolrQueryRequest as well as do standard macro processing and JSON request parsing.
   *
   * @param handler The search handler this is for (may be null if you don't want this method touching the content streams)
   * @param req The request whose params we are interested in
   * @param defaults values to be used if no values are specified in the request params
   * @param appends values to be appended to those from the request (or defaults) when dealing with multi-val params, or treated as another layer of defaults for singl-val params.
   * @param invariants values which will be used instead of any request, or default values, regardless of context.
   */
  public static void processParams(SolrRequestHandler handler, SolrQueryRequest req, SolrParams defaults,
                                   SolrParams appends, SolrParams invariants) {

    boolean searchHandler = handler instanceof SearchHandler;
    SolrParams params = req.getParams();

    // Handle JSON stream for search requests
    if (searchHandler && req.getContentStreams() != null) {

      Map<String,String[]> map = MultiMapSolrParams.asMultiMap(params, false);

      if (!(params instanceof MultiMapSolrParams || params instanceof ModifiableSolrParams)) {
        // need to set params on request since we weren't able to access the original map
        params = new MultiMapSolrParams(map);
        req.setParams(params);
      }

      String[] jsonFromParams = map.remove(JSON);  // params from the query string should come after (and hence override) JSON content streams

      for (ContentStream cs : req.getContentStreams()) {
        String contentType = cs.getContentType();
        if (contentType==null || !contentType.contains("/json")) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Bad contentType for search handler :" + contentType + " request="+req);
        }

        try {
          String jsonString = IOUtils.toString( cs.getReader() );
          if (jsonString != null) {
            MultiMapSolrParams.addParam(JSON, jsonString, map);
          }
        } catch (IOException e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Exception reading content stream for request:"+req, e);
        }
      }

      // append existing "json" params
      if (jsonFromParams != null) {
        for (String json : jsonFromParams) {
          MultiMapSolrParams.addParam(JSON, json, map);
        }
      }
    }

    String[] jsonS = params.getParams(JSON);

    boolean hasAdditions = defaults != null || invariants != null || appends != null || jsonS != null;

    // short circuit processing
    if (!hasAdditions && !params.getBool("expandMacros", true)) {
      return;  // nothing to do...
    }

    boolean isShard = params.getBool("isShard", false);

    Map<String, String[]> newMap = MultiMapSolrParams.asMultiMap(params, hasAdditions);


    // see if the json has a "params" section
    // TODO: we should currently *not* do this if this is a leaf of a distributed search since it could overwrite parameters set by the top-level
    // The parameters we extract will be propagated anyway.
    if (jsonS != null && !isShard) {
      for (String json : jsonS) {
        getParamsFromJSON(newMap, json);
      }
    }

    // first populate defaults, etc..
    if (defaults != null) {
      Map<String, String[]> defaultsMap = MultiMapSolrParams.asMultiMap(defaults);
      for (Map.Entry<String, String[]> entry : defaultsMap.entrySet()) {
        String key = entry.getKey();
        if (!newMap.containsKey(key)) {
          newMap.put(key, entry.getValue());
        }
      }
    }

    if (appends != null) {
      Map<String, String[]> appendsMap = MultiMapSolrParams.asMultiMap(appends);

      for (Map.Entry<String, String[]> entry : appendsMap.entrySet()) {
        String key = entry.getKey();
        String[] arr = newMap.get(key);
        if (arr == null) {
          newMap.put(key, entry.getValue());
        } else {
          String[] appendArr = entry.getValue();
          String[] newArr = new String[arr.length + appendArr.length];
          System.arraycopy(arr, 0, newArr, 0, arr.length);
          System.arraycopy(appendArr, 0, newArr, arr.length, appendArr.length);
          newMap.put(key, newArr);
        }
      }
    }


    if (invariants != null) {
      newMap.putAll( MultiMapSolrParams.asMultiMap(invariants) );
    }

    if (!isShard) { // Don't expand macros in shard requests
      String[] doMacrosStr = newMap.get("expandMacros");
      boolean doMacros = true;
      if (doMacrosStr != null) {
        doMacros = "true".equals(doMacrosStr[0]);
      }

      if (doMacros) {
        newMap = MacroExpander.expand(newMap);
      }
    }
    // Set these params as soon as possible so if there is an error processing later, things like
    // "wt=json" will take effect from the defaults.
    SolrParams newParams = new MultiMapSolrParams(newMap);  // newMap may still change below, but that should be OK
    req.setParams(newParams);


    // Skip the rest of the processing (including json processing for now) if this isn't a search handler.
    // For example json.command started to be used  in SOLR-6294, and that caused errors here.
    if (!searchHandler) return;


    Map<String, Object> json = null;
    // Handle JSON body first, so query params will always overlay on that
    jsonS = newMap.get(JSON);
    if (jsonS != null) {
      if (json == null) {
        json = new LinkedHashMap<>();
      }
      mergeJSON(json, JSON, jsonS, new ObjectUtil.ConflictHandler());
    }
    for (Map.Entry<String, String[]> entry : newMap.entrySet()) {
      String key = entry.getKey();
      // json.nl, json.wrf are existing query parameters
      if (key.startsWith("json.") && !("json.nl".equals(key) || "json.wrf".equals(key))) {
        if (json == null) {
          json = new LinkedHashMap<>();
        }
        mergeJSON(json, key, entry.getValue(), new ObjectUtil.ConflictHandler());
      }
    }

    // implement compat for existing components...
    JsonQueryConverter jsonQueryConverter = new JsonQueryConverter();
    if (json != null && !isShard) {
      for (Map.Entry<String,Object> entry : json.entrySet()) {
        String key = entry.getKey();
        String out = null;
        boolean isQuery = false;
        boolean arr = false;
        if ("query".equals(key)) {
          out = "q";
          isQuery = true;
        } else if ("filter".equals(key)) {
          out = "fq";
          arr = true;
          isQuery = true;
        } else if ("fields".equals(key)) {
          out = "fl";
          arr = true;
        } else if ("offset".equals(key)) {
          out = "start";
        } else if ("limit".equals(key)) {
          out = "rows";
        } else if (SORT.equals(key)) {
          out = SORT;
        } else if ("params".equals(key) || "facet".equals(key) ) {
          // handled elsewhere
          continue;
        } else {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown top-level key in JSON request : " + key);
        }

        Object val = entry.getValue();

        if (arr) {
          String[] existing = newMap.get(out);
          List lst = val instanceof List ? (List)val : null;
          int existingSize = existing==null ? 0 : existing.length;
          int jsonSize = lst==null ? 1 : lst.size();
          String[] newval = new String[ existingSize + jsonSize ];
          for (int i=0; i<existingSize; i++) {
            newval[i] = existing[i];
          }
          if (lst != null) {
            for (int i = 0; i < jsonSize; i++) {
              Object v = lst.get(i);
              newval[existingSize + i] = isQuery ? jsonQueryConverter.toLocalParams(v, newMap) : v.toString();
            }
          } else {
            newval[newval.length-1] = isQuery ? jsonQueryConverter.toLocalParams(val, newMap) : val.toString();
          }
          newMap.put(out, newval);
        } else {
          newMap.put(out, new String[]{isQuery ? jsonQueryConverter.toLocalParams(val, newMap) : val.toString()});
        }

      }


    }

    if (json != null) {
      req.setJSON(json);
    }

  }



  // queryParamName is something like json.facet or json.query, or just json...
  private static void mergeJSON(Map<String,Object> json, String queryParamName, String[] vals, ObjectUtil.ConflictHandler handler) {
    try {
      List<String> path = StrUtils.splitSmart(queryParamName, ".", true);
      path = path.subList(1, path.size());
      for (String jsonStr : vals) {
        Object o = ObjectBuilder.fromJSONStrict(jsonStr);
        // zero-length strings or comments can cause this to be null (and a zero-length string can result from a json content-type w/o a body)
        if (o != null) {
          ObjectUtil.mergeObjects(json, path, o, handler);
        }
      }
    } catch (JSONParser.ParseException e ) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    } catch (IOException e) {
      // impossible
    }
  }


  private static void getParamsFromJSON(Map<String, String[]> params, String json) {
    if (json.indexOf("params") < 0) {
      return;
    }

    JSONParser parser = new JSONParser(json);
    try {
      JSONUtil.expect(parser, JSONParser.OBJECT_START);
      boolean found = JSONUtil.advanceToMapKey(parser, "params", false);
      if (!found) {
        return;
      }

      parser.nextEvent();  // advance to the value

      Object o = ObjectBuilder.getVal(parser);
      if (!(o instanceof Map)) return;
      Map<String,Object> map = (Map<String,Object>)o;
      // To make consistent with json.param handling, we should make query params come after json params (i.e. query params should
      // appear to overwrite json params.

      // Solr params are based on String though, so we need to convert
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        String key = entry.getKey();
        Object val = entry.getValue();
        if (params.get(key) != null) {
          continue;
        }

        if (val == null) {
          params.remove(key);
        } else if (val instanceof List) {
          List lst = (List) val;
          String[] vals = new String[lst.size()];
          for (int i = 0; i < vals.length; i++) {
            vals[i] = lst.get(i).toString();
          }
          params.put(key, vals);
        } else {
          params.put(key, new String[]{val.toString()});
        }
      }

    } catch (Exception e) {
      // ignore parse exceptions at this stage, they may be caused by incomplete macro expansions
      return;
    }

  }



}
