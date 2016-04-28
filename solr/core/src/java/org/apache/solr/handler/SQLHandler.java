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

import java.lang.invoke.MethodHandles;
import java.util.Properties;

import org.apache.solr.client.solrj.io.stream.ExceptionStream;
import org.apache.solr.client.solrj.io.stream.JDBCStream;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.sql.SolrSchemaFactory;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLHandler extends RequestHandlerBase implements SolrCoreAware , PermissionNameProvider {

  private static String defaultZkhost = null;
  private static String defaultWorkerCollection = null;

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public void inform(SolrCore core) {

    CoreContainer coreContainer = core.getCoreDescriptor().getCoreContainer();

    if(coreContainer.isZooKeeperAware()) {
      defaultZkhost = core.getCoreDescriptor().getCoreContainer().getZkController().getZkServerAddress();
      defaultWorkerCollection = core.getCoreDescriptor().getCollectionName();
    }
  }

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext request) {
    return PermissionNameProvider.Name.READ_PERM;
  }

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams params = req.getParams();
    params = adjustParams(params);
    req.setParams(params);
    String sql = params.get("stmt");

    TupleStream tupleStream = null;
    try {
      if(sql == null) {
        throw new Exception("stmt parameter cannot be null");
      }

      Properties info = new Properties();
      info.setProperty("model",
          "inline:{\n" +
              "  \"version\": \"1.0\",\n" +
              "  \"defaultSchema\": \"" + defaultZkhost + "\",\n" +
              "  \"schemas\": [\n" +
              "    {\n" +
              "      \"name\": \"" + defaultZkhost + "\",\n" +
              "      \"type\": \"custom\",\n" +
              "      \"factory\": \"" + SolrSchemaFactory.class.getName() + "\",\n" +
              "      \"operand\": {\n" +
              "        \"zk\": \"" + defaultZkhost + "\"\n" +
              "      }\n" +
              "    }\n" +
              "  ]\n" +
              "}");
      info.setProperty("lex", "MYSQL");

      tupleStream = new StreamHandler.TimerStream(new ExceptionStream(
          new JDBCStream("jdbc:calcite:", sql, null, info, null)));

      rsp.add("result-set", tupleStream);
    } catch(Exception e) {
      //Catch the SQL parsing and query transformation exceptions.
      if(tupleStream != null) {
        tupleStream.close();
      }
      SolrException.log(logger, e);
      rsp.add("result-set", new StreamHandler.DummyErrorStream(e));
    }
  }

  private SolrParams adjustParams(SolrParams params) {
    ModifiableSolrParams adjustedParams = new ModifiableSolrParams();
    adjustedParams.add(params);
    adjustedParams.add(CommonParams.OMIT_HEADER, "true");
    return adjustedParams;
  }

  public String getDescription() {
    return "SQLHandler";
  }

  public String getSource() {
    return null;
  }
}
