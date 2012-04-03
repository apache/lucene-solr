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

import org.junit.BeforeClass;
import org.junit.Test;

public class TestMultiThreadedFileReader extends AbstractDataImportHandlerTestCase {
  private String getConf() {
    int numThreads = random.nextInt(8) + 2; // between 2 and 10
    System.out.println(getFile("dih/TestMultiThreadedFileReader").getAbsolutePath());
    return
      "<dataConfig>" +
      "  <dataSource name=\"files\" type=\"FileDataSource\" encoding=\"UTF-8\"/>" +
      "  <document>" +
      "    <entity name=\"filesdata\" processor=\"FileListEntityProcessor\" threads=\"" + numThreads + "\" rootEntity=\"false\" fileName=\"\\.xml$\" recursive=\"true\" dataSource=\"null\" baseDir=\"" + getFile("dih/TestMultiThreadedFileReader").getAbsolutePath() + "\" >" +
      "      <entity name=\"records\" processor=\"XPathEntityProcessor\" rootEntity=\"true\" dataSource=\"files\" stream=\"true\" forEach=\"/documents/document\" url=\"${filesdata.fileAbsolutePath}\">" +
      "        <field column=\"id\"   xpath=\"/documents/document/@id\"/>" +
      "        <field column=\"desc\" xpath=\"/documents/document/element[@name='desc']/value\"/>" +
      "      </entity>" +
      "    </entity>" +
      "  </document>" +
      "</dataConfig>"
    ;
  }
  private String[] tests = {
      "//*[@numFound='6']"
  };

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("dataimport-solrconfig.xml", "dataimport-schema.xml");
  }

  @Test
  public void testMultiThreadedFileReader() throws Exception {
    runFullImport(getConf());
    assertQ(req("*:*"), tests );
  }

}
