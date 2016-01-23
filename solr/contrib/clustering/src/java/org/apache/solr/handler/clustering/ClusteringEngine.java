package org.apache.solr.handler.clustering;
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
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;

/**
 * A base class for {@link SearchClusteringEngine} and {@link DocumentClusteringEngine}.
 * @lucene.experimental
 */
public abstract class ClusteringEngine {
  public static final String ENGINE_NAME = "name";
  public static final String DEFAULT_ENGINE_NAME = "default";

  private String name;

  public String init(NamedList<?> config, SolrCore core) {
    name = (String) config.get(ENGINE_NAME);
    return name;
  }

  public String getName() {
    return name;
  }

  public abstract boolean isAvailable();
}
