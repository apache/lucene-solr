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

package org.apache.solr.cloud.rule;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Map;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.common.cloud.rule.SnitchContext;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @deprecated to be removed in Solr 9.0 (see SOLR-14930)
 *
 */
public class ServerSnitchContext extends SnitchContext {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  SolrCloudManager cloudManager;
  public ServerSnitchContext(SnitchInfo perSnitch,
                             String node, Map<String, Object> session,
                             SolrCloudManager cloudManager) {
    super(perSnitch, node, session);
    this.cloudManager = cloudManager;
  }


  @SuppressWarnings({"rawtypes"})
  public Map getZkJson(String path) throws KeeperException, InterruptedException {
    try {
      return Utils.getJson(cloudManager.getDistribStateManager(), path) ;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  public Map<String, Object> getNodeValues(String node, Collection<String> tags){
    return cloudManager.getNodeStateProvider().getNodeValues(node, tags);
  }


}
