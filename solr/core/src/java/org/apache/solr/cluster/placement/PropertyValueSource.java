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

package org.apache.solr.cluster.placement;

/**
 * Getting the {@link PropertyValue} for a {@link PropertyKey} involves a "resolution" of that key in a certain context.
 * Depending on the type of the key, the context can be the whole {@link Cluster}, a {@link Node}, a {@link SolrCollection},
 * a {@link Shard} or a {@link Replica}. Not all {@link PropertyKey}'s make sense for all {@link PropertyValueSource}'s of course.<p>
 *
 * When only one type of {@link PropertyValueSource} is appropriate for a given {@link PropertyKey}, that type (extending
 * {@link PropertyValueSource}) is used instead to reduce confusion.<p>
 *
 * This is an empty marker interface that identifies the possible targets for which a {@link PropertyKey} can be defined.
 */
public interface PropertyValueSource {
}
