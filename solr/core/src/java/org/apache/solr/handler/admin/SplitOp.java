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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.SplitIndexCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.DocCollection.DOC_ROUTER;
import static org.apache.solr.common.params.CommonParams.PATH;


class SplitOp implements CoreAdminHandler.CoreAdminOp {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    SolrParams params = it.req.getParams();
    List<DocRouter.Range> ranges = null;

    String[] pathsArr = params.getParams(PATH);
    String rangesStr = params.get(CoreAdminParams.RANGES);    // ranges=a-b,c-d,e-f
    if (rangesStr != null) {
      String[] rangesArr = rangesStr.split(",");
      if (rangesArr.length == 0) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "There must be at least one range specified to split an index");
      } else {
        ranges = new ArrayList<>(rangesArr.length);
        for (String r : rangesArr) {
          try {
            ranges.add(DocRouter.DEFAULT.fromString(r));
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Exception parsing hexadecimal hash range: " + r, e);
          }
        }
      }
    }
    String splitKey = params.get("split.key");
    String[] newCoreNames = params.getParams("targetCore");
    String cname = params.get(CoreAdminParams.CORE, "");

    if ((pathsArr == null || pathsArr.length == 0) && (newCoreNames == null || newCoreNames.length == 0)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Either path or targetCore param must be specified");
    }

    log.info("Invoked split action for core: " + cname);
    SolrCore core = it.handler.coreContainer.getCore(cname);
    SolrQueryRequest req = new LocalSolrQueryRequest(core, params);
    List<SolrCore> newCores = null;

    try {
      // TODO: allow use of rangesStr in the future
      List<String> paths = null;
      int partitions = pathsArr != null ? pathsArr.length : newCoreNames.length;

      DocRouter router = null;
      String routeFieldName = null;
      if (it.handler.coreContainer.isZooKeeperAware()) {
        ClusterState clusterState = it.handler.coreContainer.getZkController().getClusterState();
        String collectionName = req.getCore().getCoreDescriptor().getCloudDescriptor().getCollectionName();
        DocCollection collection = clusterState.getCollection(collectionName);
        String sliceName = req.getCore().getCoreDescriptor().getCloudDescriptor().getShardId();
        Slice slice = collection.getSlice(sliceName);
        router = collection.getRouter() != null ? collection.getRouter() : DocRouter.DEFAULT;
        if (ranges == null) {
          DocRouter.Range currentRange = slice.getRange();
          ranges = currentRange != null ? router.partitionRange(partitions, currentRange) : null;
        }
        Object routerObj = collection.get(DOC_ROUTER); // for back-compat with Solr 4.4
        if (routerObj != null && routerObj instanceof Map) {
          Map routerProps = (Map) routerObj;
          routeFieldName = (String) routerProps.get("field");
        }
      }

      if (pathsArr == null) {
        newCores = new ArrayList<>(partitions);
        for (String newCoreName : newCoreNames) {
          SolrCore newcore = it.handler.coreContainer.getCore(newCoreName);
          if (newcore != null) {
            newCores.add(newcore);
          } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Core with core name " + newCoreName + " expected but doesn't exist.");
          }
        }
      } else {
        paths = Arrays.asList(pathsArr);
      }


      SplitIndexCommand cmd = new SplitIndexCommand(req, paths, newCores, ranges, router, routeFieldName, splitKey);
      core.getUpdateHandler().split(cmd);

      // After the split has completed, someone (here?) should start the process of replaying the buffered updates.

    } catch (Exception e) {
      log.error("ERROR executing split:", e);
      throw new RuntimeException(e);

    } finally {
      if (req != null) req.close();
      if (core != null) core.close();
      if (newCores != null) {
        for (SolrCore newCore : newCores) {
          newCore.close();
        }
      }
    }
  }
}
