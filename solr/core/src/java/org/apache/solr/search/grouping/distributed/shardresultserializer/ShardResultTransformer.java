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
package org.apache.solr.search.grouping.distributed.shardresultserializer;

import org.apache.lucene.search.Sort;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;

/**
 * A <code>ShardResultTransformer</code> is responsible for transforming a grouped shard result into group related
 * structures (such as {@link org.apache.lucene.search.grouping.TopGroups} and {@link org.apache.lucene.search.grouping.SearchGroup})
 * and visa versa.
 *
 * @lucene.experimental
 */
public interface ShardResultTransformer<T, R> {

  /**
   * Transforms data to a {@link NamedList} structure for serialization purposes.
   *
   * @param data The data to be transformed
   * @return {@link NamedList} structure
   * @throws IOException If I/O related errors occur during transforming
   */
  @SuppressWarnings({"rawtypes"})
  NamedList transform(T data) throws IOException;

  /**
   * Transforms the specified shard response into native structures.
   *
   * @param shardResponse The shard response containing data in a {@link NamedList} structure
   * @param groupSort The group sort
   * @param withinGroupSort The sort inside a group
   * @param shard The shard address where the response originated from
   * @return native structure of the data
   */
  @SuppressWarnings({"rawtypes"})
  R transformToNative(NamedList<NamedList> shardResponse, Sort groupSort, Sort withinGroupSort, String shard);

}
