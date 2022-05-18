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
package org.apache.solr.handler.sql;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.Holder;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.handler.sql.functions.ArrayContainsAll;
import org.apache.solr.handler.sql.functions.ArrayContainsAny;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * JDBC driver for Calcite Solr.
 *
 * <p>It accepts connect strings that start with "jdbc:calcitesolr:".</p>
 */
public class CalciteSolrDriver extends Driver {
  public final static String CONNECT_STRING_PREFIX = "jdbc:calcitesolr:";

  public static CalciteSolrDriver INSTANCE = new CalciteSolrDriver();

  private SolrClientCache solrClientCache;


  private CalciteSolrDriver() {
    super();
  }

  static {
    INSTANCE.register();
  }

  static void subQueryThreshold(Holder<SqlToRelConverter.Config> configHolder) {
    configHolder.accept(config -> config.withInSubQueryThreshold(Integer.MAX_VALUE));
  }

  static void relBuilderSimplify(Holder<Boolean> configHolder) {
    configHolder.accept(config -> false);
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if(!this.acceptsURL(url)) {
      return null;
    }

    // Configure SqlToRelConverter to allow more values for an 'IN' clause,
    // otherwise, Calcite will transform the query into a join with a static table of literals
    Hook.SQL2REL_CONVERTER_CONFIG_BUILDER.addThread(CalciteSolrDriver::subQueryThreshold);

    // disable Calcite's simplify (see SOLR-16009) as it erases some query
    // constructs that are still meaningful to Solr (such as AND'd filters on the same field,
    // which works for multi-valued fields in Solr but looks like nonsense to Calcite.
    Hook.REL_BUILDER_SIMPLIFY.addThread(CalciteSolrDriver::relBuilderSimplify);

    Connection connection = super.connect(url, info);
    CalciteConnection calciteConnection = (CalciteConnection) connection;
    registerUDFs();

    final SchemaPlus rootSchema = calciteConnection.getRootSchema();

    String schemaName = info.getProperty("zk");
    if(schemaName == null) {
      throw new SQLException("zk must be set");
    }
    final SolrSchema solrSchema = new SolrSchema(info, solrClientCache);
    rootSchema.add(schemaName, solrSchema);

    // Set the default schema
    calciteConnection.setSchema(schemaName);
    return calciteConnection;
  }

  private void registerUDFs() {
    final SqlStdOperatorTable stdOpTab = SqlStdOperatorTable.instance();
    stdOpTab.register(new ArrayContainsAll());
    stdOpTab.register(new ArrayContainsAny());
  }

  public void setSolrClientCache(SolrClientCache solrClientCache) {
    this.solrClientCache = solrClientCache;
  }
}
