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
package org.apache.solr.handler.component;

import org.apache.solr.search.SolrIndexSearcher;

import java.util.Comparator;
import java.io.IOException;

/**
* The MergeStrategy class defines custom merge logic for distributed searches.
*
*  <b>Note: This API is experimental and may change in non backward-compatible ways in the future</b>
**/


public interface MergeStrategy {

  /**
  *  merge defines the merging behaving of results that are collected from the
  *  shards during a distributed search.
  *
  **/

  public void merge(ResponseBuilder rb, ShardRequest sreq);

  /**
  * mergesIds must return true if the merge method merges document ids from the shards.
  * If it merges other output from the shards it must return false.
  * */

  public boolean mergesIds();


  /**
  * handlesMergeFields must return true if the MergeStrategy
  * implements a custom handleMergeFields(ResponseBuilder rb, SolrIndexSearch searcher)
  * */

  public boolean handlesMergeFields();


  /**
  *  Implement handleMergeFields(ResponseBuilder rb, SolrIndexSearch searcher) if
  *  your merge strategy needs more complex data then the sort fields provide.
  * */

  public void handleMergeFields(ResponseBuilder rb, SolrIndexSearcher searcher) throws IOException;

  /**
  *  Defines the order that the mergeStrategies are applied. Lower costs are applied first.
  * */
  public int getCost();

  @SuppressWarnings({"rawtypes"})
  final Comparator MERGE_COMP = (o1, o2) -> {
    MergeStrategy m1 = (MergeStrategy) o1;
    MergeStrategy m2 = (MergeStrategy) o2;
    return m1.getCost() - m2.getCost();
  };

}