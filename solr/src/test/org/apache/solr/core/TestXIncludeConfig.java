package org.apache.solr.core;

/**
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

import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.util.AbstractSolrTestCase;

import javax.xml.parsers.DocumentBuilderFactory;


/**
 *
 *
 **/
public class TestXIncludeConfig extends AbstractSolrTestCase {
  protected boolean supports;

  @Override
  public String getSchemaFile() {
    return "schema.xml";
  }

  //public String getSolrConfigFile() { return "solrconfig.xml"; }
  @Override
  public String getSolrConfigFile() {
    return "solrconfig-xinclude.xml";
  }

  @Override
  public void setUp() throws Exception {
    supports = true;
    javax.xml.parsers.DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    try {
      //see whether it even makes sense to run this test
      dbf.setXIncludeAware(true);
      dbf.setNamespaceAware(true);
    } catch (UnsupportedOperationException e) {
      supports = false;
    }
    super.setUp();
  }

  public void testXInclude() throws Exception {
    //Figure out whether this JVM supports XInclude anyway, if it doesn't then don't run this test????
    // TODO: figure out a better way to handle this.
    if (supports == true){
      SolrCore core = h.getCore();
      SolrRequestHandler solrRequestHandler = core.getRequestHandler("includedHandler");
      assertNotNull("Solr Req Handler is null", solrRequestHandler);
    } else {
      log.info("Didn't run testXInclude, because this XML DocumentBuilderFactory doesn't support it");
    }

  }
}
