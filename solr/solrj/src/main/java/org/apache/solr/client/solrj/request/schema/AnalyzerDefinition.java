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
package org.apache.solr.client.solrj.request.schema;

import java.util.List;
import java.util.Map;

public class AnalyzerDefinition {
  private Map<String, Object> attributes;

  private List<Map<String, Object>> charFilters;

  private Map<String, Object> tokenizer;

  private List<Map<String, Object>> filters;

  public Map<String, Object> getAttributes() {
    return attributes;
  }

  public void setAttributes(Map<String, Object> attributes) {
    this.attributes = attributes;
  }

  public List<Map<String, Object>> getCharFilters() {
    return charFilters;
  }

  public void setCharFilters(List<Map<String, Object>> charFilters) {
    this.charFilters = charFilters;
  }

  public Map<String, Object> getTokenizer() {
    return tokenizer;
  }

  public void setTokenizer(Map<String, Object> tokenizer) {
    this.tokenizer = tokenizer;
  }

  public List<Map<String, Object>> getFilters() {
    return filters;
  }

  public void setFilters(List<Map<String, Object>> filters) {
    this.filters = filters;
  }
}