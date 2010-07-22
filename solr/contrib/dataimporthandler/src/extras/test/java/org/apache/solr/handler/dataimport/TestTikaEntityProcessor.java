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
package org.apache.solr.handler.dataimport;

import org.junit.After;
import org.junit.Before;

/**Testcase for TikaEntityProcessor
 * @version $Id$
 * @since solr 1.5 
 */
public class TestTikaEntityProcessor extends AbstractDataImportHandlerTestCase {

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  public String getSchemaFile() {
    return "dataimport-schema-no-unique-key.xml";
  }

  public String getSolrConfigFile() {
    return "dataimport-solrconfig.xml";
  }

  public void testIndexingWithTikaEntityProcessor() throws Exception {
    String conf =
            "<dataConfig>" +
                    "  <dataSource type=\"BinFileDataSource\"/>" +
                    "  <document>" +
                    "    <entity processor=\"TikaEntityProcessor\" url=\"../../../../../extraction/src/test/resources/solr-word.pdf\" >" +
                    "      <field column=\"Author\" meta=\"true\" name=\"author\"/>" +
                    "      <field column=\"title\" meta=\"true\" name=\"docTitle\"/>" +
                    "      <field column=\"text\"/>" +
                    "     </entity>" +
                    "  </document>" +
                    "</dataConfig>";
    super.runFullImport(conf);
    assertQ(req("*:*"), "//*[@numFound='1']");
  }
}
