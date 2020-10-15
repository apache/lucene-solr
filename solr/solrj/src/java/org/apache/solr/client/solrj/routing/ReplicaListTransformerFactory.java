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

import org.apache.solr.common.params.SolrParams;

public interface ReplicaListTransformerFactory {

  /**
   * 
   * @param configSpec spec for dynamic configuration of ReplicaListTransformer
   * @param requestParams the request parameters for which the ReplicaListTransformer is being generated
   * @param fallback used to generate fallback value; the getInstance() method of the specified fallback must not
   * return null; The fallback value itself may be null if this implementation is known to never return null (i.e., if
   * fallback will never be needed)
   * @return ReplicaListTransformer to be used for routing this request
   */
  ReplicaListTransformer getInstance(String configSpec, SolrParams requestParams, ReplicaListTransformerFactory fallback);

}
