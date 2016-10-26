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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.CommandOperation;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;
import static org.apache.solr.common.cloud.ZkStateReader.SOLR_SECURITY_CONF_PATH;

/**
 * Security Configuration Handler which works with Zookeeper
 */
public class SecurityConfHandlerZk extends SecurityConfHandler {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public SecurityConfHandlerZk(CoreContainer coreContainer) {
    super(coreContainer);
  }

  /**
   * Fetches security props from Zookeeper and adds version
   * @param getFresh refresh from ZK
   * @return SecurityConfig whose data property either contains security.json, or an empty map if not found
   */
  @Override
  public SecurityConfig getSecurityConfig(boolean getFresh) {
    ZkStateReader.ConfigData configDataFromZk = cores.getZkController().getZkStateReader().getSecurityProps(getFresh);
    return configDataFromZk == null ? 
        new SecurityConfig() :
        new SecurityConfig().setData(configDataFromZk.data).setVersion(configDataFromZk.version);
  }

  @Override
  protected void getConf(SolrQueryResponse rsp, String key) {
    ZkStateReader.ConfigData map = cores.getZkController().getZkStateReader().getSecurityProps(false);
    Object o = map == null ? null : map.data.get(key);
    if (o == null) {
      rsp.add(CommandOperation.ERR_MSGS, Collections.singletonList("No " + key + " configured"));
    } else {
      rsp.add(key+".enabled", getPlugin(key)!=null);
      rsp.add(key, o);
    }
  }
  
  @Override
  protected boolean persistConf(SecurityConfig securityConfig) throws IOException {
    try {
      cores.getZkController().getZkClient().setData(SOLR_SECURITY_CONF_PATH, 
          Utils.toJSON(securityConfig.getData()), 
          securityConfig.getVersion(), true);
      log.debug("Persisted security.json to {}", SOLR_SECURITY_CONF_PATH);
      return true;
    } catch (KeeperException.BadVersionException bdve){
      log.warn("Failed persisting security.json to {}", SOLR_SECURITY_CONF_PATH, bdve);
      return false;
    } catch (Exception e) {
      throw new SolrException(SERVER_ERROR, "Unable to persist security.json", e);
    }
  }
  
  @Override
  public String getDescription() {
    return "Edit or read security configuration from Zookeeper";
  }
  
}
