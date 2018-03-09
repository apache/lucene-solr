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
package org.apache.solr.core;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.processor.RegexReplaceProcessorFactory;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Assume;
import org.junit.BeforeClass;

/** 
 * Test both XInclude as well as more old school "entity includes"
 */
public class TestXIncludeConfig extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-xinclude.xml", "schema-xinclude.xml");
  }

  @Override
  public void setUp() throws Exception {
    javax.xml.parsers.DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    try {
      //see whether it even makes sense to run this test
      dbf.setXIncludeAware(true);
      dbf.setNamespaceAware(true);
    } catch (UnsupportedOperationException e) {
      Assume.assumeTrue(false);
    }
    super.setUp();
  }

  public void testXInclude() throws Exception {
    SolrCore core = h.getCore();

    assertNotNull("includedHandler is null", 
                  core.getRequestHandler("/includedHandler"));

    UpdateRequestProcessorChain chain 
      = core.getUpdateProcessingChain("special-include");
    assertNotNull("chain is missing included processor", chain);
    assertEquals("chain with inclued processor is wrong size", 
                 1, chain.getProcessors().size());
    assertEquals("chain has wrong included processor",
                 RegexReplaceProcessorFactory.class,
                 chain.getProcessors().get(0).getClass());

    IndexSchema schema = core.getLatestSchema();
    
    // xinclude
    assertNotNull("ft-included is null", schema.getFieldTypeByName("ft-included"));
    assertNotNull("field-included is null", schema.getFieldOrNull("field-included"));

    // entity include
    assertNotNull("ft-entity-include1 is null", 
                  schema.getFieldTypeByName("ft-entity-include1"));
    assertNotNull("ft-entity-include2 is null", 
                  schema.getFieldTypeByName("ft-entity-include2"));

    // sanity check
    assertNull("ft-entity-include3 is not null",  // Does Not Exist Anywhere
               schema.getFieldTypeByName("ft-entity-include3"));

  }
}
