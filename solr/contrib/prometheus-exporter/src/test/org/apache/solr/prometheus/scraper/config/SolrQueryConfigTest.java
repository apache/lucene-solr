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
package org.apache.solr.prometheus.scraper.config;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Unit test for SolrQueryConfig.
 */
public class SolrQueryConfigTest extends SolrTestCaseJ4 {
  @Test
  public void testQueryConfig() throws Exception {
    SolrQueryConfig queryConfig = new SolrQueryConfig();

    assertNotNull(queryConfig);
  }

  @Test
  public void testGetCollection() throws Exception {
    SolrQueryConfig queryConfig = new SolrQueryConfig();

    String expected = "";
    String actual = queryConfig.getCollection();
    assertEquals(expected, actual);
  }

  @Test
  public void testSetCollection() throws Exception {
    SolrQueryConfig queryConfig = new SolrQueryConfig();

    queryConfig.setCollection("collection1");

    String expected = "collection1";
    String actual = queryConfig.getCollection();
    assertEquals(expected, actual);
  }

  @Test
  public void testGetPath() throws Exception {
    SolrQueryConfig queryConfig = new SolrQueryConfig();

    String expected = "";
    String actual = queryConfig.getPath();
    assertEquals(expected, actual);
  }

  @Test
  public void testSetPath() throws Exception {
    SolrQueryConfig queryConfig = new SolrQueryConfig();

    queryConfig.setPath("/select");

    String expected = "/select";
    String actual = queryConfig.getPath();
    assertEquals(expected, actual);
  }

  @Test
  public void testGetParams() throws Exception {
    SolrQueryConfig queryConfig = new SolrQueryConfig();

    List<LinkedHashMap<String, String>> expected = new ArrayList<>();
    List<LinkedHashMap<String, String>> actual = queryConfig.getParams();
    assertEquals(expected, actual);
  }

  @Test
  public void testSetParams() throws Exception {
    SolrQueryConfig queryConfig = new SolrQueryConfig();

    LinkedHashMap<String,String> param1 = new LinkedHashMap<>();
    param1.put("q", "*:*");

    LinkedHashMap<String,String> param2 = new LinkedHashMap<>();
    param2.put("facet", "on");

    queryConfig.setParams(Arrays.asList(param1, param2));

    List<LinkedHashMap<String, String>> expected = Arrays.asList(param1, param2);
    List<LinkedHashMap<String, String>> actual = queryConfig.getParams();
    assertEquals(expected, actual);
  }

  @Test
  public void testGetParamsString() throws Exception {
    SolrQueryConfig queryConfig = new SolrQueryConfig();

    LinkedHashMap<String,String> param1 = new LinkedHashMap<>();
    param1.put("q", "*:*");
    param1.put("fq", "manu:apple");

    LinkedHashMap<String,String> param2 = new LinkedHashMap<>();
    param2.put("facet", "on");

    queryConfig.setParams(Arrays.asList(param1, param2));

    String expected = "q=*:*&fq=manu:apple&facet=on";
    String actual = queryConfig.getParamsString();
    assertEquals(expected, actual);
  }
}
