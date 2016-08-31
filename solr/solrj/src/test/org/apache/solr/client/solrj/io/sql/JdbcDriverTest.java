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
package org.apache.solr.client.solrj.io.sql;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

/**
 * Tests the connection string part of the JDBC Driver
 **/

public class JdbcDriverTest extends SolrTestCaseJ4 {

  @Test(expected = SQLException.class)
  public void testNullZKConnectionString() throws Exception {
    Connection con = DriverManager.getConnection("jdbc:solr://?collection=collection1");
  }

  @Test(expected = SQLException.class)
  public void testInvalidJDBCConnectionString() throws Exception {
    Connection con = DriverManager.getConnection("jdbc:mysql://");
  }

  @Test(expected = SQLException.class)
  public void testNoCollectionProvidedInURL() throws Exception {
    Connection con = DriverManager.getConnection("jdbc:solr://?collection=collection1");
  }

  @Test(expected = SQLException.class)
  public void testNoCollectionProvidedInProperties() throws Exception {
    Connection con = DriverManager.getConnection("jdbc:solr://", new Properties());
  }

  @Test(expected = SQLException.class)
  public void testConnectionStringJumbled() throws Exception {
    final String sampleZkHost="zoo1:9983/foo";
    DriverManager.getConnection("solr:jdbc://" + sampleZkHost + "?collection=collection1", new Properties());
  }

  @Test
  public void testProcessUrl() throws Exception {
    DriverImpl driver = new DriverImpl();

    List<String> zkHostStrings = Arrays.asList("zoo1", "zoo1:9983", "zoo1,zoo2,zoo3", "zoo1:9983,zoo2:9983,zoo3:9983");
    List<String> chroots = Arrays.asList("", "/", "/foo", "/foo/bar");
    List<String> paramStrings = Arrays.asList("", "collection=collection1", "collection=collection1&test=test1");

    for(String zkHostString : zkHostStrings) {
      for(String chroot : chroots) {
        for(String paramString : paramStrings) {
          String url = "jdbc:solr://" + zkHostString + chroot + "?" + paramString;

          URI uri = driver.processUrl(url);

          assertEquals(zkHostString, uri.getAuthority());
          assertEquals(chroot, uri.getPath());
          assertEquals(paramString, uri.getQuery());
        }
      }
    }
  }
}
