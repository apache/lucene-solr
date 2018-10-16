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
package org.apache.solr.cloud;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CloudUtil {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  /**
   * See if coreNodeName has been taken over by another baseUrl and unload core
   * + throw exception if it has been.
   */
  public static void checkSharedFSFailoverReplaced(CoreContainer cc, CoreDescriptor desc) {
    if (!cc.isSharedFs(desc)) return;

    ZkController zkController = cc.getZkController();
    String thisCnn = zkController.getCoreNodeName(desc);
    String thisBaseUrl = zkController.getBaseUrl();

    log.debug("checkSharedFSFailoverReplaced running for coreNodeName={} baseUrl={}", thisCnn, thisBaseUrl);

    // if we see our core node name on a different base url, unload
    final DocCollection docCollection = zkController.getClusterState().getCollectionOrNull(desc.getCloudDescriptor().getCollectionName());
    if (docCollection != null && docCollection.getSlicesMap() != null) {
      Map<String,Slice> slicesMap = docCollection.getSlicesMap();
      for (Slice slice : slicesMap.values()) {
        for (Replica replica : slice.getReplicas()) {

          String cnn = replica.getName();
          String baseUrl = replica.getStr(ZkStateReader.BASE_URL_PROP);
          log.debug("compare against coreNodeName={} baseUrl={}", cnn, baseUrl);

          if (thisCnn != null && thisCnn.equals(cnn)
              && !thisBaseUrl.equals(baseUrl)) {
            if (cc.getLoadedCoreNames().contains(desc.getName())) {
              cc.unload(desc.getName());
            }

            try {
              FileUtils.deleteDirectory(desc.getInstanceDir().toFile());
            } catch (IOException e) {
              SolrException.log(log, "Failed to delete instance dir for core:"
                  + desc.getName() + " dir:" + desc.getInstanceDir());
            }
            log.error("", new SolrException(ErrorCode.SERVER_ERROR,
                "Will not load SolrCore " + desc.getName()
                    + " because it has been replaced due to failover."));
            throw new SolrException(ErrorCode.SERVER_ERROR,
                "Will not load SolrCore " + desc.getName()
                    + " because it has been replaced due to failover.");
          }
        }
      }
    }
  }

  public static boolean replicaExists(ClusterState clusterState, String collection, String shard, String coreNodeName) {
    DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    if (docCollection != null) {
      Slice slice = docCollection.getSlice(shard);
      if (slice != null) {
        return slice.getReplica(coreNodeName) != null;
      }
    }
    return false;
  }

  /**
   * Returns a displayable unified path to the given resource. For non-solrCloud that will be the
   * same as getConfigDir, but for Cloud it will be getConfigSetZkPath ending in a /
   * <p>
   * <b>Note:</b> Do not use this to generate a valid file path, but for debug printing etc
   * @param loader Resource loader instance
   * @return a String of path to resource
   */
  public static String unifiedResourcePath(SolrResourceLoader loader) {
    return (loader instanceof ZkSolrResourceLoader) ?
            ((ZkSolrResourceLoader) loader).getConfigSetZkPath() + "/" :
            loader.getConfigDir() + File.separator;
  }

  /**Read the list of public keys from ZK
   */

  public static Map<String, byte[]> getTrustedKeys(SolrZkClient zk, String dir) {
    Map<String, byte[]> result = new HashMap<>();
    try {
      List<String> children = zk.getChildren("/keys/" + dir, null, true);
      for (String key : children) {
        if (key.endsWith(".der")) result.put(key, zk.getData("/keys/" + dir +
            "/" + key, null, null, true));
      }
    } catch (KeeperException.NoNodeException e) {
      log.info("Error fetching key names");
      return Collections.EMPTY_MAP;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(ErrorCode.SERVER_ERROR,"Unable to read crypto keys",e );
    } catch (KeeperException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR,"Unable to read crypto keys",e );
    }
    return result;

  }

}
