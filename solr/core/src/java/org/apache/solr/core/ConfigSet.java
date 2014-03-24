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

package org.apache.solr.core;

import org.apache.solr.schema.IndexSchema;

/**
 * Stores a core's configuration in the form of a SolrConfig and IndexSchema
 */
public class ConfigSet {

  private final String name;

  private final SolrConfig solrconfig;

  private final IndexSchema indexSchema;

  public ConfigSet(String name, SolrConfig solrConfig, IndexSchema indexSchema) {
    this.name = name;
    this.solrconfig = solrConfig;
    this.indexSchema = indexSchema;
  }

  public String getName() {
    return name;
  }

  public SolrConfig getSolrConfig() {
    return solrconfig;
  }

  public IndexSchema getIndexSchema() {
    return indexSchema;
  }
}
