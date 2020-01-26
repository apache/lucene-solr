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
package org.apache.solr.client.solrj.routing;

import java.util.List;

import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ShardParams;

public interface ReplicaListTransformer {

  /**
   * Transforms the passed in list of choices. Transformations can include (but are not limited to)
   * reordering of elements (e.g. via shuffling) and removal of elements (i.e. filtering).
   *
   * @param choices - a list of choices to transform, typically the choices are {@link Replica} objects but choices
   * can also be {@link String} objects such as URLs passed in via the {@link ShardParams#SHARDS} parameter.
   */
  public void transform(List<?> choices);

}
