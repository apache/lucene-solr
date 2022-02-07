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

package org.apache.solr.handler.admin;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.solr.cloud.SyncStrategy;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.CoreAdminHandler.CallInfo;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RequestSyncShardOp implements CoreAdminHandler.CoreAdminOp {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void execute(CallInfo it) throws Exception {
    final SolrParams params = it.req.getParams();

    log.info("I have been requested to sync up my shard");

    String cname = params.required().get(CoreAdminParams.CORE);

    ZkController zkController = it.handler.coreContainer.getZkController();
    if (zkController == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Only valid for SolrCloud");
    }

    SyncStrategy syncStrategy = null;
    try (SolrCore core = it.handler.coreContainer.getCore(cname)) {

      if (core != null) {
        syncStrategy = new SyncStrategy(core.getCoreContainer());

        Map<String, Object> props = new HashMap<>();
        props.put(ZkStateReader.CORE_NAME_PROP, cname);
        props.put(ZkStateReader.NODE_NAME_PROP, zkController.getNodeName());
        props.put(ZkStateReader.BASE_URL_PROP, zkController.getZkStateReader().getBaseUrlForNodeName(zkController.getNodeName()));

        boolean success = syncStrategy.sync(zkController, core, new ZkNodeProps(props), true).isSuccess();
        // solrcloud_debug
        if (log.isDebugEnabled()) {
          try {
            RefCounted<SolrIndexSearcher> searchHolder = core
                .getNewestSearcher(false);
            SolrIndexSearcher searcher = searchHolder.get();
            try {
              if (log.isDebugEnabled()) {
                log.debug("{} synched {}", core.getCoreContainer().getZkController().getNodeName()
                    , searcher.count(new MatchAllDocsQuery()));
              }
            } finally {
              searchHolder.decref();
            }
          } catch (Exception e) {
            log.debug("Error in solrcloud_debug block", e);
          }
        }
        if (!success) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Sync Failed");
        }
      } else {
        SolrException.log(log, "Could not find core to call sync:" + cname);
      }
    } finally {
      // no recoveryStrat close for now
      if (syncStrategy != null) {
        syncStrategy.close();
      }
    }
  }
}
