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

package org.apache.solr.prometheus.exporter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;

import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.DOMUtil;
import org.w3c.dom.Node;

public class MetricsQuery {

  private final String path;
  private final ModifiableSolrParams parameters;
  private final String core;
  private final String collection;
  private final List<JsonQuery> jsonQueries;

  private MetricsQuery(
      String path,
      ModifiableSolrParams parameters,
      String core,
      String collection,
      List<JsonQuery> jsonQueries) {
    this.path = path;
    this.parameters = parameters;
    this.core = core;
    this.collection = collection;
    this.jsonQueries = jsonQueries;
  }

  public MetricsQuery withCore(String core) {
    return new MetricsQuery(
        getPath(),
        getParameters(),
        core,
        getCollection().orElse(null),
        getJsonQueries()
    );
  }

  public MetricsQuery withCollection(String collection) {
    return new MetricsQuery(
        getPath(),
        getParameters(),
        getCore().orElse(null),
        collection,
        getJsonQueries()
    );
  }

  public String getPath() {
    return path;
  }

  public Optional<String> getCore() {
    return Optional.ofNullable(core);
  }

  public Optional<String> getCollection() {
    return Optional.ofNullable(collection);
  }

  public List<JsonQuery> getJsonQueries() {
    return jsonQueries;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static List<MetricsQuery> from(Node node, Map<String,MetricsQueryTemplate> jqTemplates) throws JsonQueryException {
    List<MetricsQuery> metricsQueries = new ArrayList<>();

    NamedList config = DOMUtil.childNodesToNamedList(node);
    List<NamedList> requests = config.getAll("request");

    for (NamedList request : requests) {
      NamedList query = (NamedList) request.get("query");
      NamedList<Object> queryParameters = (NamedList<Object>) query.get("params");
      String path = (String) query.get("path");
      String core = (String) query.get("core");
      String collection = (String) query.get("collection");
      List<String> jsonQueries = (ArrayList<String>) request.get("jsonQueries");

      ModifiableSolrParams params = new ModifiableSolrParams();
      if (queryParameters != null) {
        for (Map.Entry<String, Object> entrySet : queryParameters.asShallowMap().entrySet()) {
          if (entrySet.getValue() instanceof Collection) {
            Collection<Object> values = (Collection<Object>) entrySet.getValue();
            for (Object value : values) {
              params.add(entrySet.getKey(), String.valueOf(value));
            }
          } else {
            params.add(entrySet.getKey(), String.valueOf(entrySet.getValue()));
          }
        }
      }

      QueryRequest queryRequest = new QueryRequest(params);
      queryRequest.setPath(path);

      List<JsonQuery> compiledQueries = new ArrayList<>();
      if (jsonQueries != null) {
        for (String jsonQuery : jsonQueries) {

          // does this query refer to a reusable jq template to reduce boilerplate in the config?
          final String jsonQueryCollapseWs = jsonQuery.replaceAll("\\s+", " ").trim();
          if (jsonQueryCollapseWs.startsWith("$jq:")) {
            Optional<Matcher> maybeMatcher = MetricsQueryTemplate.matches(jsonQueryCollapseWs);
            if (maybeMatcher.isPresent()) {
              Matcher matcher = maybeMatcher.get();
              String templateName = matcher.group("TEMPLATE");
              MetricsQueryTemplate template = jqTemplates.get(templateName);
              if (template == null) {
                throw new IllegalStateException("jq template '" + matcher.group("TEMPLATE") + "' not found!");
              }

              jsonQuery = template.applyTemplate(matcher);
            }
          }

          JsonQuery compiledJsonQuery = JsonQuery.compile(jsonQuery);
          compiledQueries.add(compiledJsonQuery);
        }
      }

      metricsQueries.add(new MetricsQuery(
          path,
          params,
          core,
          collection,
          compiledQueries));
    }

    return metricsQueries;
  }

  public ModifiableSolrParams getParameters() {
    return parameters;
  }
}
