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
package org.apache.solr.search;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Query;
import org.apache.solr.handler.component.MergeStrategy;

import java.io.IOException;

/**
 *  <b>Note: This API is experimental and may change in non backward-compatible ways in the future</b>
 **/

public abstract class RankQuery extends ExtendedQueryBase {

  @SuppressWarnings({"rawtypes"})
  public abstract TopDocsCollector getTopDocsCollector(int len, QueryCommand cmd, IndexSearcher searcher) throws IOException;
  public abstract MergeStrategy getMergeStrategy();
  public abstract RankQuery wrap(Query mainQuery);

}
