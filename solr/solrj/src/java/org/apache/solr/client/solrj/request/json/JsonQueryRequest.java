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

package org.apache.solr.client.solrj.request.json;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;

/**
 * Represents a query using the <a href="https://lucene.apache.org/solr/guide/json-request-api.html">JSON Query DSL</a>
 *
 * This class constructs the request using setters for individual properties.  For a more monolithic approach to
 * constructing the JSON request, see {@link DirectJsonQueryRequest}
 */
public class JsonQueryRequest extends QueryRequest {
  private final Map<String, Object> jsonRequestMap;

  /**
   * Creates a {@link JsonQueryRequest} with an empty {@link SolrParams} object
   */
  public JsonQueryRequest() {
    this(new ModifiableSolrParams());
  }

  /**
   * Creates a {@link JsonQueryRequest} using the provided {@link SolrParams}
   */
  public JsonQueryRequest(SolrParams params) {
    super(params, METHOD.POST);
    this.jsonRequestMap = new HashMap<>();
  }

  /**
   * Specify the query sent as a part of this JSON request
   *
   * This method may be called multiple times, but each call overwrites the value specified by previous calls.
   *
   * @param query a String in either of two formats: a query string for the default deftype (e.g. "title:solr"), or a
   *              localparams query (e.g. "{!lucene df=text v='solr'}" )
   *
   * @throws IllegalArgumentException if {@code query} is null
   */
  public JsonQueryRequest setQuery(String query) {
    if (query == null) {
      throw new IllegalArgumentException("'query' parameter must be non-null");
    }
    jsonRequestMap.put("query", query);
    return this;
  }

  /**
   * Specify the query sent as a part of this JSON request.
   *
   * This method may be called multiple times, but each call overwrites the value specified by previous calls.
   * <p>
   * <b>Example:</b> You wish to send the JSON request: "{'limit': 5, 'query': {'lucene': {'df':'genre_s', 'query': 'scifi'}}}".  The
   * query subtree of this request is: "{'lucene': {'df': 'genre_s', 'query': 'scifi'}}".  You would represent this query
   * JSON as follows:
   * <pre>{@code
   *     final Map<String, Object> queryMap = new HashMap<>();
   *     final Map<String, Object> luceneQueryParamMap = new HashMap<>();
   *     queryMap.put("lucene", luceneQueryParamMap);
   *     luceneQueryParamMap.put("df", "genre_s");
   *     luceneQueryParamMap.put("query", "scifi");
   * }</pre>
   *
   * @param queryJson a Map of values representing the query subtree of the JSON request you wish to send.
   * @throws IllegalArgumentException if {@code queryJson} is null.
   */
  public JsonQueryRequest setQuery(Map<String, Object> queryJson) {
    if (queryJson == null) {
      throw new IllegalArgumentException("'queryJson' parameter must be non-null");
    }
    jsonRequestMap.put("query", queryJson);
    return this;
  }

  /**
   * Specify whether results should be fetched starting from a particular offset (or 'start').
   *
   * Defaults to 0 if not set.
   *
   * @param offset a non-negative integer representing the offset (or 'start') to use when returning results
   *
   * @throws IllegalArgumentException if {@code offset} is negative
   */
  public JsonQueryRequest setOffset(int offset) {
    if (offset < 0) {
      throw new IllegalArgumentException("'offset' parameter must be non-negative");
    }
    jsonRequestMap.put("offset", offset);
    return this;
  }

  /**
   * Specify how many results should be returned from the JSON request
   *
   * @param limit a non-negative integer representing the maximum results to return from a search
   * @throws IllegalArgumentException if {@code limit} is negative
   */
  public JsonQueryRequest setLimit(int limit) {
    if (limit < 0) {
      throw new IllegalArgumentException("'limit' parameter must be non-negative");
    }
    jsonRequestMap.put("limit", limit);
    return this;
  }

  /**
   * Specify how results to the JSON request should be sorted before being returned by Solr
   *
   * @param sort a string representing the desired result sort order (e.g. "price asc")
   *
   * @throws IllegalArgumentException if {@code sort} is null
   */
  public JsonQueryRequest setSort(String sort) {
    if (sort == null) {
      throw new IllegalArgumentException("'sort' parameter must be non-null");
    }
    jsonRequestMap.put("sort", sort);
    return this;
  }

  /**
   * Add a filter query to run as a part of the JSON request
   *
   * This method may be called multiple times; each call will add a new filter to the request
   *
   * @param filterQuery a String in either of two formats: a query string for the default deftype (e.g. "title:solr"), or a
   *                    localparams query (e.g. "{!lucene df=text v='solr'}" )
   * @throws IllegalArgumentException if {@code filterQuery} is null
   */
  public JsonQueryRequest withFilter(String filterQuery) {
    if (filterQuery == null) {
      throw new IllegalArgumentException("'filterQuery' must be non-null");
    }
    jsonRequestMap.putIfAbsent("filter", new ArrayList<Object>());
    final List<Object> filters = (List<Object>) jsonRequestMap.get("filter");
    filters.add(filterQuery);
    return this;
  }

  /**
   * Add a filter query to run as a part of the JSON request
   *
   * This method may be called multiple times; each call will add a new filter to the request
   * <p>
   * <b>Example:</b> You wish to send the JSON request: "{'query':'*:*', 'filter': [{'lucene': {'df':'genre_s', 'query': 'scifi'}}]}".
   * The filter you want to add is: "{'lucene': {'df': 'genre_s', 'query': 'scifi'}}".  You would represent this filter
   * query as follows:
   * <pre>{@code
   *     final Map<String, Object> filterMap = new HashMap<>();
   *     final Map<String, Object> luceneQueryParamMap = new HashMap<>();
   *     filterMap.put("lucene", luceneQueryParamMap);
   *     luceneQueryParamMap.put("df", "genre_s");
   *     luceneQueryParamMap.put("query", "scifi");
   * }</pre>
   *
   * @param filterQuery a Map of values representing the filter request you wish to send.
   * @throws IllegalArgumentException if {@code filterQuery} is null
   */
  public JsonQueryRequest withFilter(Map<String, Object> filterQuery) {
    if (filterQuery == null) {
      throw new IllegalArgumentException("'filterQuery' parameter must be non-null");
    }
    jsonRequestMap.putIfAbsent("filter", new ArrayList<Object>());
    final List<Object> filters = (List<Object>) jsonRequestMap.get("filter");
    filters.add(filterQuery);
    return this;
  }

  /**
   * Specify fields which should be returned by the JSON request.
   *
   * This method may be called multiple times; each call will add a new field to the list of those to be returned.
   *
   * @param fieldNames the field names that should be returned by the request
   */
  public JsonQueryRequest returnFields(String... fieldNames) {
    jsonRequestMap.putIfAbsent("fields", new ArrayList<String>());
    final List<String> fields = (List<String>) jsonRequestMap.get("fields");
    for (String fieldName : fieldNames) {
      fields.add(fieldName);
    }
    return this;
  }

  /**
   * Specify fields which should be returned by the JSON request.
   *
   * This method may be called multiple times; each call will add a new field to the list of those to be returned.
   *
   * @param fieldNames the field names that should be returned by the request
   * @throws IllegalArgumentException if {@code fieldNames} is null
   */
  public JsonQueryRequest returnFields(Iterable<String> fieldNames) {
    if (fieldNames == null) {
      throw new IllegalArgumentException("'fieldNames' parameter must be non-null");
    }
    jsonRequestMap.putIfAbsent("fields", new ArrayList<String>());
    final List<String> fields = (List<String>) jsonRequestMap.get("fields");
    for (String fieldName : fieldNames) {
      fields.add(fieldName);
    }
    return this;
  }

  /**
   * Add a property to the "params" block supported by the JSON query DSL
   *
   * The JSON query DSL has special support for a few query parameters (limit/rows, offset/start, filter/fq, etc.).  But
   * many other query parameters are not explicitly covered by the query DSL.  This method can be used to add any of
   * these other parameters to the JSON request.
   * <p>
   * This method may be called multiple times; each call with a different {@code name} will add a new param name/value
   * to the params subtree. Invocations that repeat a {@code name} will overwrite the previously specified parameter
   * values associated with that name.
   *
   * @param name the name of the parameter to add
   * @param value the value of the parameter to add.  Usually a String, Number (Integer, Long, Double), or Boolean.
   *
   * @throws IllegalArgumentException if either {@code name} or {@code value} are null
   */
  public JsonQueryRequest withParam(String name, Object value) {
    if (name == null) {
      throw new IllegalArgumentException("'name' parameter must be non-null");
    }
    if (value == null) {
      throw new IllegalArgumentException("'value' parameter must be non-null");
    }

    jsonRequestMap.putIfAbsent("params", new HashMap<String, Object>());
    final Map<String, Object> miscParamsMap = (Map<String, Object>) jsonRequestMap.get("params");
    miscParamsMap.put(name, value);
    return this;
  }

  public RequestWriter.ContentWriter getContentWriter(String expectedType) {
    return new RequestWriter.ContentWriter() {
      @Override
      public void write(OutputStream os) throws IOException {
        //TODO consider whether using Utils.writeJson would work here as that'd be more mem efficient
        OutputStreamWriter writer = new OutputStreamWriter(os, StandardCharsets.UTF_8);

        writer.write(Utils.toJSONString(jsonRequestMap));
        writer.flush();
      }

      @Override
      public String getContentType() {
        return ClientUtils.TEXT_JSON;
      }
    };
  }

  @Override
  public void setMethod(METHOD m) {
    if (METHOD.POST != m) {
      final String message = getClass().getName() + " only supports POST for sending JSON queries.";
      throw new UnsupportedOperationException(message);
    }
  }
}
