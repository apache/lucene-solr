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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class TestFilterCalciteConnection extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void testConnectionClosesSchema() throws Exception {
    String zkHost = cluster.getSolrClient().getZkHost();
    Properties props = new Properties();
    props.setProperty("zk", zkHost);
    Connection conn = null;
    try {
      // load the driver
      Class.forName(CalciteSolrDriver.class.getName());
      conn = DriverManager.getConnection("jdbc:calcitesolr:test", props);
      assertTrue("unexpected connection impl: " + conn.getClass().getName(),
          conn instanceof FilterCalciteConnection);
      SchemaPlus rootSchema = ((FilterCalciteConnection)conn).getRootSchema();
      SchemaPlus subSchema = rootSchema.getSubSchema(zkHost);
      assertNotNull("missing SolrSchema", subSchema);
      SolrSchema solrSchema = subSchema.unwrap(SolrSchema.class);
      // test that conn.close() propagates to the schema
      conn.close();
      assertTrue("SolrSchema not closed after connection close!", solrSchema.isClosed());
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    }
  }

  @Test
  public void testMethodsOverriden() throws Exception {
    implTestDeclaredMethodsOverridden(CalciteConnection.class, FilterCalciteConnection.class);
  }

  private void implTestDeclaredMethodsOverridden(Class<?> superClass, Class<?> subClass) throws Exception {
    for (final Method superClassMethod : superClass.getDeclaredMethods()) {
      final int modifiers = superClassMethod.getModifiers();
      if (Modifier.isPrivate(modifiers)) continue;
      if (Modifier.isFinal(modifiers)) continue;
      if (Modifier.isStatic(modifiers)) continue;
      try {
        final Method subClassMethod = subClass.getDeclaredMethod(
            superClassMethod.getName(),
            superClassMethod.getParameterTypes());
        assertEquals("getReturnType() difference",
            superClassMethod.getReturnType(),
            subClassMethod.getReturnType());
      } catch (NoSuchMethodException e) {
        fail(subClass + " needs to override '" + superClassMethod + "'");
      }
    }
  }
}
