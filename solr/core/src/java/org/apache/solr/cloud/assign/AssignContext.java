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

package org.apache.solr.cloud.assign;

import org.apache.solr.cluster.api.SolrCluster;

import java.util.List;

/**An interface through which the plugin can interact with the Assign framework system
 */
public interface AssignContext {

    /**get an instance of {@link SolrCluster} with enough data prefetched
     */
    SolrCluster getCluster(PrefetchHint... hints);

    /**Fetch metrics from a given node
     *
     * @param nodeName Nam eof the node
     * @param metricsKeys full name of metrics keys
     */
    List<Object> fetchMetrics(String nodeName, List<NodeMetrics> metricsKeys);

}
