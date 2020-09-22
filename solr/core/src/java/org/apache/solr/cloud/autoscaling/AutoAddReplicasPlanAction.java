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

package org.apache.solr.cloud.autoscaling;


import java.util.Collections;
import java.util.Map;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.core.SolrResourceLoader;

import static org.apache.solr.common.cloud.ZkStateReader.AUTO_ADD_REPLICAS;

/**
 * This class configures the parent ComputePlanAction to compute plan
 * only for collections which have autoAddReplicas=true.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class AutoAddReplicasPlanAction extends ComputePlanAction {

  @Override
  public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, Map<String, Object> properties) throws TriggerValidationException {
    properties.put("collections", Collections.singletonMap(AUTO_ADD_REPLICAS, "true"));
    super.configure(loader, cloudManager, properties);
  }
}
