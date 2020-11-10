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

import java.io.StringReader;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.RedactionUtils;
import org.junit.BeforeClass;
import org.junit.Test;


public class PropertiesRequestHandlerTest extends SolrTestCaseJ4 {

  public static final String PASSWORD = "secret123";
  public static final String REDACT_STRING = RedactionUtils.getRedactString();


  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testRedaction() throws Exception {
    RedactionUtils.setRedactSystemProperty(true);
    for(String propName: new String[]{"some.password", "javax.net.ssl.trustStorePassword"}){
      System.setProperty(propName, PASSWORD);
      NamedList<NamedList<NamedList<Object>>> properties = readProperties();

      assertEquals("Failed to redact "+propName, REDACT_STRING, properties.get(propName));
    }
  }

  @Test
  public void testDisabledRedaction() throws Exception {
    RedactionUtils.setRedactSystemProperty(false);
    for(String propName: new String[]{"some.password", "javax.net.ssl.trustStorePassword"}){
      System.setProperty(propName, PASSWORD);
      NamedList<NamedList<NamedList<Object>>> properties = readProperties();

      assertEquals("Failed to *not* redact "+propName, PASSWORD, properties.get(propName));
    }
  }

  @SuppressWarnings({"unchecked"})
  private NamedList<NamedList<NamedList<Object>>> readProperties() throws Exception {
    String xml = h.query(req(
        CommonParams.QT, "/admin/properties",
        CommonParams.WT, "xml"
    ));

    XMLResponseParser parser = new XMLResponseParser();
    return (NamedList<NamedList<NamedList<Object>>>)
        parser.processResponse(new StringReader(xml)).get("system.properties");
  }
}
