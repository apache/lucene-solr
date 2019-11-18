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
import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.solr.api.ApiBag;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaManager;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.ConfigSetResourceUtil;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class ConfigSetSchemaHandler {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final CoreContainer coreContainer;
  private static final String SCHEMA_NAME = "managed-schema";
  private static final String CONFIG_PREFIX = ZkConfigManager.CONFIGS_ZKNODE + "/";
  private static final String PATH_PREFIX = "/cluster/configset/";
  private static final String PATH_POSTFIX_SCHEMA = "/schema";

  public ConfigSetSchemaHandler(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  public final Write write = new Write();
  public final Read read = new Read();

  @EndPoint(method = SolrRequest.METHOD.GET,
      path = {PATH_PREFIX, PATH_PREFIX + "{name}" + PATH_POSTFIX_SCHEMA},
      permission = PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public class Read {
    @Command()
    public void get(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      String configSetName = req.getPathTemplateValues().get("name");
      //@todo apv remove query param ?
      //@todo apv check get version call
      String schemaPath = req.getParams().get("query");
      //populating ManagedSchema
      SolrConfig config = getSolrConfig(configSetName);
      ManagedIndexSchema schema = getManagedSchema(configSetName, config);
      //running operations
      Map<String, Object> params = new SchemaManager().executeGET(schemaPath, schema, req.getParams(), coreContainer.getResourceLoader());
      Iterator<String> iterator = params.keySet().iterator();
      while (iterator.hasNext()) {
        String key = iterator.next();
        rsp.add(key, params.get(key));
      }
    }
  }

  @EndPoint(method = SolrRequest.METHOD.POST,
      path = PATH_PREFIX + "{name}" + PATH_POSTFIX_SCHEMA,
      permission = PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public class Write {
    @Command()
    public void post(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      String configSetName = req.getPathTemplateValues().get("name");
      try {
        //populating ManagedSchema
        SolrConfig config = getSolrConfig(configSetName);
        ManagedIndexSchema schema = getManagedSchema(configSetName, config);
        List errs = new SchemaManager().doCmdOperations(
            configSetName, req.getCommands(false),
            schema, config, req.getParams(), coreContainer.getResourceLoader(), coreContainer.getZkController());
        if (!errs.isEmpty())
          throw new ApiBag.ExceptionWithErrObject(SolrException.ErrorCode.BAD_REQUEST, "error processing commands", errs);
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error reading input String " + e.getMessage(), e);
      }
    }
  }

  private ManagedIndexSchema getManagedSchema(String configSetName, SolrConfig config) {
    String schemaNode = CONFIG_PREFIX + configSetName + "/" + SCHEMA_NAME;
    Stat stat = new Stat();
    InputSource schemaIS = ConfigSetResourceUtil.populate(coreContainer.getZkController().getZkClient(), schemaNode, new Stat());
    return new ManagedIndexSchema(
        config.luceneMatchVersion, coreContainer.getResourceLoader(), SCHEMA_NAME, schemaIS, stat.getVersion(), new Object());
  }

  private SolrConfig getSolrConfig(String configSetName) {
    SolrConfig config;
    try {
      config = new SolrConfig(configSetName, coreContainer.getZkController().getZkClient());
    } catch (IOException | ParserConfigurationException | SAXException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to form solrConfig", e);
    }
    return config;
  }

}
