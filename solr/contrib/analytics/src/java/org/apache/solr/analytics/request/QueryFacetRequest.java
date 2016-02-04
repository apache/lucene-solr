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
package org.apache.solr.analytics.request;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Contains all of the specifications for a query facet.
 */
public class QueryFacetRequest implements FacetRequest {
  private String name;
  private List<String> queries;
  private Set<String> dependencies;
  
  public QueryFacetRequest() {
    dependencies = new HashSet<>();
  }

  public QueryFacetRequest(String name) {
    this.name = name;
    this.queries = new ArrayList<>();
    dependencies = new HashSet<>();
  }
 
  public List<String> getQueries() {
    return queries;
  }

  public void setQueries(List<String> queries) {
    this.queries = queries;
  }

  public void addQuery(String query) {
    queries.add(query);
  }

  public Set<String> getDependencies() {
    return dependencies;
  }

  public void setDependencies(Set<String> dependencies) {
    this.dependencies = dependencies;
  }

  public void addDependency(String dependency) {
    dependencies.add(dependency);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
  
}
