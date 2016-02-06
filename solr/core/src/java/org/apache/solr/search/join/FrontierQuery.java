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
package org.apache.solr.search.join;

import org.apache.lucene.search.Query;
/**
 * Frontier Query represents the next hop of a GraphTraversal.
 * It contains the query to execute and the number of edges to traverse.
 * @lucene.internal
 */
class FrontierQuery {
  
  private final Query query;
  private final Integer frontierSize;
  
  public FrontierQuery(Query query, Integer frontierSize) {
    super();
    this.query = query;
    this.frontierSize = frontierSize;
  }
  /**
   * Return the query that represents the frontier at the current level.
   */
  public Query getQuery() {
    return query;
  }
  /**
   * Return the number of edges in the frontier query.
   */
  public Integer getFrontierSize() {
    return frontierSize;
  }
  
}
