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
package org.apache.solr.handler.dataimport;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNonWritablePersistFile extends AbstractDataImportHandlerTestCase {
  private static final String FULLIMPORT_QUERY = "select * from x";

  private static final String DELTA_QUERY = "select id from x where last_modified > NOW";

  private static final String DELETED_PK_QUERY = "select id from x where last_modified > NOW AND deleted='true'";

  private static final String dataConfig_delta =
    "<dataConfig>" +
    "  <dataSource  type=\"MockDataSource\"/>\n" +
    "  <document>\n" +
    "    <entity name=\"x\" transformer=\"TemplateTransformer\"" +
    "            query=\"" + FULLIMPORT_QUERY + "\"" +
    "            deletedPkQuery=\"" + DELETED_PK_QUERY + "\"" +
    "            deltaImportQuery=\"select * from x where id='${dih.delta.id}'\"" +
    "            deltaQuery=\"" + DELTA_QUERY + "\">\n" +
    "      <field column=\"id\" name=\"id\"/>\n" +
    "      <entity name=\"y\" query=\"select * from y where y.A='${x.id}'\">\n" +
    "        <field column=\"desc\" />\n" +
    "      </entity>\n" +
    "    </entity>\n" +
    "  </document>\n" +
    "</dataConfig>\n";
  private static String tmpSolrHome;

  private static File f;

  @BeforeClass
  public static void createTempSolrHomeAndCore() throws Exception {
    tmpSolrHome = createTempDir().toFile().getAbsolutePath();
    FileUtils.copyDirectory(getFile("dih/solr"), new File(tmpSolrHome).getAbsoluteFile());
    initCore("dataimport-solrconfig.xml", "dataimport-schema.xml", 
             new File(tmpSolrHome).getAbsolutePath());
    
    // See SOLR-2551
    String configDir = h.getCore().getResourceLoader().getConfigDir();
    String filePath = configDir;
    if (configDir != null && !configDir.endsWith(File.separator))
      filePath += File.separator;
    filePath += "dataimport.properties";
    f = new File(filePath);
    // execute the test only if we are able to set file to read only mode
    assumeTrue("No dataimport.properties file", f.exists() || f.createNewFile());
    assumeTrue("dataimport.properties can't be set read only", f.setReadOnly());
    assumeFalse("dataimport.properties is still writable even though " + 
                "marked readonly - test running as superuser?", f.canWrite());
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    if (f != null) {
      f.setWritable(true);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNonWritablePersistFile() throws Exception {
    ignoreException("Properties is not writable");

    @SuppressWarnings("rawtypes")
    List parentRow = new ArrayList();
    parentRow.add(createMap("id", "1"));
    MockDataSource.setIterator(FULLIMPORT_QUERY, parentRow.iterator());
      
    @SuppressWarnings("rawtypes")
    List childRow = new ArrayList();
    childRow.add(createMap("desc", "hello"));
    MockDataSource.setIterator("select * from y where y.A='1'",
                                 childRow.iterator());
      
    runFullImport(dataConfig_delta);
    assertQ(req("id:1"), "//*[@numFound='0']");
  }  
}
