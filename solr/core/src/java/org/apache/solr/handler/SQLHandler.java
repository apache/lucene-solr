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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.config.Lex;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.ExceptionStream;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.sql.CalciteSolrDriver;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLHandler extends RequestHandlerBase implements SolrCoreAware, PermissionNameProvider {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static String defaultZkhost = null;
  private static String defaultWorkerCollection = null;

  static final String sqlNonCloudErrorMsg = "/sql handler only works in Solr Cloud mode";

  private boolean isCloud = false;

  public void inform(SolrCore core) {
    CoreContainer coreContainer = core.getCoreContainer();

    if(coreContainer.isZooKeeperAware()) {
      defaultZkhost = core.getCoreContainer().getZkController().getZkServerAddress();
      defaultWorkerCollection = core.getCoreDescriptor().getCollectionName();
      isCloud = true;
    }
  }

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext request) {
    return PermissionNameProvider.Name.READ_PERM;
  }

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
    params = adjustParams(params);
    req.setParams(params);

    String sql = params.get("stmt");
    // Set defaults for parameters
    params.set("numWorkers", params.getInt("numWorkers", 1));
    params.set("workerCollection", params.get("workerCollection", defaultWorkerCollection));
    params.set("workerZkhost", params.get("workerZkhost", defaultZkhost));
    params.set("aggregationMode", params.get("aggregationMode", "facet"));

    TupleStream tupleStream = null;
    try {

      if(!isCloud) {
        throw new IllegalStateException(sqlNonCloudErrorMsg);
      }

      if(sql == null) {
        throw new Exception("stmt parameter cannot be null");
      }

      String url = CalciteSolrDriver.CONNECT_STRING_PREFIX;

      Properties properties = new Properties();
      // Add all query parameters
      Iterator<String> parameterNamesIterator = params.getParameterNamesIterator();
      while(parameterNamesIterator.hasNext()) {
        String param = parameterNamesIterator.next();
        properties.setProperty(param, params.get(param));
      }

      // Set these last to ensure that they are set properly
      properties.setProperty("lex", Lex.MYSQL.toString());
      properties.setProperty("zk", defaultZkhost);

      String driverClass = CalciteSolrDriver.class.getCanonicalName();

      // JDBC driver requires metadata from the SQLHandler. Default to false since this adds a new Metadata stream.
      boolean includeMetadata = params.getBool("includeMetadata", false);
      tupleStream = new SqlHandlerStream(url, sql, null, properties, driverClass, includeMetadata);

      tupleStream = new StreamHandler.TimerStream(new ExceptionStream(tupleStream));

      rsp.add("result-set", tupleStream);
    } catch(Exception e) {
      //Catch the SQL parsing and query transformation exceptions.
      if(tupleStream != null) {
        tupleStream.close();
      }
      SolrException.log(log, e);
      rsp.add("result-set", new StreamHandler.DummyErrorStream(e));
    }
  }

  public String getDescription() {
    return "SQLHandler";
  }

  public String getSource() {
    return null;
  }

  /*
   * Only necessary for SolrJ JDBC driver since metadata has to be passed back
   */
  private static class SqlHandlerStream extends CalciteJDBCStream {
    private final boolean includeMetadata;
    private boolean firstTuple = true;
    List<String> metadataFields = new ArrayList<>();
    Map<String, String> metadataAliases = new HashMap<>();

    SqlHandlerStream(String connectionUrl, String sqlQuery, StreamComparator definedSort,
                     Properties connectionProperties, String driverClassName, boolean includeMetadata)
        throws IOException {
      super(connectionUrl, sqlQuery, definedSort, connectionProperties, driverClassName);

      this.includeMetadata = includeMetadata;
    }

    @Override
    public Tuple read() throws IOException {
      // Return a metadata tuple as the first tuple and then pass through to the JDBCStream.
      if(firstTuple) {
        try {
          Tuple tuple = new Tuple();

          firstTuple = false;

          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

          for(int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnName = resultSetMetaData.getColumnName(i);
            String columnLabel = resultSetMetaData.getColumnLabel(i);
            metadataFields.add(columnName);
            metadataAliases.put(columnName, columnLabel);
          }

          if(includeMetadata) {
            tuple.put("isMetadata", true);
            tuple.put("fields", metadataFields);
            tuple.put("aliases", metadataAliases);
            return tuple;
          }
        } catch (SQLException e) {
          throw new IOException(e);
        }
      }

      Tuple tuple = super.read();
      if(!tuple.EOF) {
        tuple.setFieldNames(metadataFields);
        tuple.setFieldLabels(metadataAliases);
      }
      return tuple;
    }
  }

  private ModifiableSolrParams adjustParams(SolrParams params) {
    ModifiableSolrParams adjustedParams = new ModifiableSolrParams();
    adjustedParams.add(params);
    adjustedParams.add(CommonParams.OMIT_HEADER, "true");
    return adjustedParams;
  }
}
