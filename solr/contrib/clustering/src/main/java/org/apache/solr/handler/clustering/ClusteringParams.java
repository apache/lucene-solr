package org.apache.solr.handler.clustering;
/**
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


/**
 *
 *
 **/
public interface ClusteringParams {

  public static final String CLUSTERING_PREFIX = "clustering.";

  public static final String ENGINE_NAME = CLUSTERING_PREFIX + "engine";

  public static final String USE_SEARCH_RESULTS = CLUSTERING_PREFIX + "results";

  public static final String USE_COLLECTION = CLUSTERING_PREFIX + "collection";
  /**
   * When document clustering, cluster on the Doc Set
   */
  public static final String USE_DOC_SET = CLUSTERING_PREFIX + "docs.useDocSet";
}
