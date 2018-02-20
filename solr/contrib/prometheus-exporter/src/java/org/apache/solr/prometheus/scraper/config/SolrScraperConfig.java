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
package org.apache.solr.prometheus.scraper.config;

import net.thisptr.jackson.jq.exception.JsonQueryException;

import java.util.ArrayList;
import java.util.List;

/**
 * SolrScraperConfig
 */
public class SolrScraperConfig implements Cloneable {
  private SolrQueryConfig query = new SolrQueryConfig();
  private List<String> jsonQueries = new ArrayList<>();

  public SolrQueryConfig getQuery() {
    return this.query;
  }

  public void setQuery(SolrQueryConfig query) {
    this.query = query;
  }

  public List<String> getJsonQueries() {
    return jsonQueries;
  }

  public void setJsonQueries(List<String> jsonQueries) throws JsonQueryException {
    this.jsonQueries = jsonQueries;
  }

  public SolrScraperConfig clone() throws CloneNotSupportedException {
    SolrScraperConfig scraperConfig = null;

    try {
      scraperConfig = (SolrScraperConfig) super.clone();
      scraperConfig.setQuery(this.query.clone());
      scraperConfig.setJsonQueries(new ArrayList<>(this.jsonQueries));
    }catch (Exception e){
      e.printStackTrace();
    }

    return scraperConfig;
  }
}
