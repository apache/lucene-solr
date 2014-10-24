package org.apache.solr.cloud;

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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CloudUtil {
  protected static Logger log = LoggerFactory.getLogger(CloudUtil.class);
  
  
  /**
   * See if coreNodeName has been taken over by another baseUrl and unload core
   * + throw exception if it has been.
   */
  public static void checkSharedFSFailoverReplaced(CoreContainer cc, CoreDescriptor desc) {
    
    ZkController zkController = cc.getZkController();
    String thisCnn = zkController.getCoreNodeName(desc);
    String thisBaseUrl = zkController.getBaseUrl();
    
    log.debug("checkSharedFSFailoverReplaced running for coreNodeName={} baseUrl={}", thisCnn, thisBaseUrl);

    // if we see our core node name on a different base url, unload
    Map<String,Slice> slicesMap = zkController.getClusterState().getSlicesMap(desc.getCloudDescriptor().getCollectionName());
    
    if (slicesMap != null) {
      for (Slice slice : slicesMap.values()) {
        for (Replica replica : slice.getReplicas()) {
          
          String cnn = replica.getName();
          String baseUrl = replica.getStr(ZkStateReader.BASE_URL_PROP);
          log.debug("compare against coreNodeName={} baseUrl={}", cnn, baseUrl);
          
          if (thisCnn != null && thisCnn.equals(cnn)
              && !thisBaseUrl.equals(baseUrl)) {
            if (cc.getCoreNames().contains(desc.getName())) {
              cc.unload(desc.getName());
            }
            
            File instanceDir = new File(desc.getInstanceDir());
            try {
              FileUtils.deleteDirectory(instanceDir);
            } catch (IOException e) {
              SolrException.log(log, "Failed to delete instance dir for core:"
                  + desc.getName() + " dir:" + instanceDir.getAbsolutePath());
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

  /**
   * Returns a displayable unified path to the given resource. For non-solrCloud that will be the
   * same as getConfigDir, but for Cloud it will be getCollectionZkPath ending in a /
   * <p/>
   * <b>Note:</b> Do not use this to generate a valid file path, but for debug printing etc
   * @param loader Resource loader instance
   * @return a String of path to resource
   */
  public static String unifiedResourcePath(SolrResourceLoader loader) {
    return (loader instanceof ZkSolrResourceLoader) ?
            ((ZkSolrResourceLoader) loader).getCollectionZkPath() + "/" :
            loader.getConfigDir();
  }
}
