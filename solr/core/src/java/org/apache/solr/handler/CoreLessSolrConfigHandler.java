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

package org.apache.solr.handler;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.InputSourceUtil;
import org.apache.zookeeper.data.Stat;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class CoreLessSolrConfigHandler {
  private static final String PATH_PREFIX = "/cluster/configset/";
  private static final String PATH_POSTFIX_CONFIG = "/config";
  private static final String CONFIG_PREFIX = "/configs/";
  private final CoreContainer coreContainer;
  public final Write write = new Write();
  public final Read read = new Read();

  public CoreLessSolrConfigHandler(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  @EndPoint(method = SolrRequest.METHOD.GET,
      path = {PATH_PREFIX, PATH_PREFIX + "{name}" + PATH_POSTFIX_CONFIG},
      permission = PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public class Read {
    @Command()
    public void get(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      String configSetName = req.getPathTemplateValues().get("name");
      //trim path for consistency
      String fPath = trimPath(req, configSetName);
      req.getContext().put("path", fPath);
      SolrConfig solrConfig = getSolrConfig(configSetName);
      new SolrConfigManager().handleGET(req, rsp, null, solrConfig, coreContainer.getResourceLoader());
    }
  }

  private String trimPath(SolrQueryRequest req, String configSetName) {
    String fPath = (String) req.getContext().get("path");
    return fPath.replace("/cluster/configset/" + configSetName, "");
  }

  @EndPoint(method = SolrRequest.METHOD.POST,
      path = PATH_PREFIX + "{name}" + PATH_POSTFIX_CONFIG,
      permission = PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public class Write {
    @Command()
    public void post(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      String configSetName = req.getPathTemplateValues().get("name");
      SolrConfig solrConfig = getSolrConfig(configSetName);
      String fPath = trimPath(req, configSetName);
      req.getContext().put("path", fPath);
      new SolrConfigManager().handlePOST(req, rsp, solrConfig, coreContainer.getResourceLoader());
    }
  }

  private SolrConfig getSolrConfig(String configSetName) {
    String confNode = CONFIG_PREFIX + configSetName + "/" + SolrConfig.DEFAULT_CONF_FILE;
    InputSource confIS = InputSourceUtil.populate(coreContainer.getZkController().getZkClient(), confNode, new Stat());
    SolrConfig config;
    try {
      config = new SolrConfig(confIS);
    } catch (IOException | ParserConfigurationException | SAXException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to form solrConfig", e);
    }
    return config;
  }
}