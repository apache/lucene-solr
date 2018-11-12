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

import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

/**
 * Represents a query using the <a href="https://lucene.apache.org/solr/guide/json-request-api.html">JSON Query DSL</a>
 *
 * This class doesn't construct the request body itself.  It uses a provided String without any modification.  Often
 * used in combination with the JSON DSL's <a href="https://lucene.apache.org/solr/guide/json-request-api.html#parameter-substitution-macro-expansion">macro expansion capabilities</a>.
 * The JSON body can contain template parameters which are replaced with values fetched from the {@link SolrParams}
 * used by this request.  For a more flexible, guided approach to constructing JSON DSL requests, see
 * {@link JsonQueryRequest}.
 */
public class DirectJsonQueryRequest extends QueryRequest {
  private final String jsonString;

  public DirectJsonQueryRequest(String jsonString) {
    this(jsonString, new ModifiableSolrParams());
  }

  public DirectJsonQueryRequest(String jsonString, SolrParams params) {
    super(params, METHOD.POST);
    this.jsonString = jsonString;
  }

  public RequestWriter.ContentWriter getContentWriter(String expectedType) {
    return new RequestWriter.StringPayloadContentWriter(jsonString, ClientUtils.TEXT_JSON);
  }
}
