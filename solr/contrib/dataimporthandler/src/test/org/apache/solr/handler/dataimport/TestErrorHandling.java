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
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Tests exception handling during imports in DataImportHandler
 *
 *
 * @since solr 1.4
 */
public class TestErrorHandling extends AbstractDataImportHandlerTestCase {

  //TODO: fix this test to not require FSDirectory.
  static String savedFactory;
  @BeforeClass
  public static void beforeClass() throws Exception {
    savedFactory = System.getProperty("solr.DirectoryFactory");
    System.setProperty("solr.directoryFactory", "solr.MockFSDirectoryFactory");
    initCore("dataimport-solrconfig.xml", "dataimport-schema.xml");
    ignoreException("Unexpected close tag");
  }
  
  @AfterClass
  public static void afterClass() {
    if (savedFactory == null) {
      System.clearProperty("solr.directoryFactory");
    } else {
      System.setProperty("solr.directoryFactory", savedFactory);
    }
  }
  
  @Before @Override
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }
  
  public void testMalformedStreamingXml() throws Exception {
    StringDataSource.xml = malformedXml;
    runFullImport(dataConfigWithStreaming);
    assertQ(req("id:1"), "//*[@numFound='1']");
    assertQ(req("id:2"), "//*[@numFound='1']");
  }

  public void testMalformedNonStreamingXml() throws Exception {
    StringDataSource.xml = malformedXml;
    runFullImport(dataConfigWithoutStreaming);
    assertQ(req("id:1"), "//*[@numFound='1']");
    assertQ(req("id:2"), "//*[@numFound='1']");
  }

  public void testAbortOnError() throws Exception {
    StringDataSource.xml = malformedXml;
    runFullImport(dataConfigAbortOnError);
    assertQ(req("*:*"), "//*[@numFound='0']");
  }

  @SuppressWarnings({"unchecked"})
  public void testTransformerErrorContinue() throws Exception {
    StringDataSource.xml = wellformedXml;
    List<Map<String, Object>> rows = new ArrayList<>();
    rows.add(createMap("id", "3", "desc", "exception-transformer"));
    MockDataSource.setIterator("select * from foo", rows.iterator());
    runFullImport(dataConfigWithTransformer);
    assertQ(req("*:*"), "//*[@numFound='3']");
  }

  public void testExternalEntity() throws Exception {
    StringDataSource.xml = wellformedXml;
    // This should not fail as external entities are replaced by an empty string during parsing:
    runFullImport(dataConfigWithEntity);
    assertQ(req("*:*"), "//*[@numFound='3']");
  }

  public static class StringDataSource extends DataSource<Reader> {
    public static String xml = "";

    @Override
    public void init(Context context, Properties initProps) {
    }

    @Override
    public Reader getData(String query) {
      return new StringReader(xml);
    }

    @Override
    public void close() {

    }
  }

  public static class ExceptionTransformer extends Transformer {
    @Override
    public Object transformRow(Map<String, Object> row, Context context) {
      throw new RuntimeException("Test exception");
    }
  }

  private String dataConfigWithStreaming = "<dataConfig>\n" +
          "        <dataSource name=\"str\" type=\"TestErrorHandling$StringDataSource\" />" +
          "    <document>\n" +
          "        <entity name=\"node\" dataSource=\"str\" processor=\"XPathEntityProcessor\" url=\"test\" stream=\"true\" forEach=\"/root/node\" onError=\"skip\">\n" +
          "            <field column=\"id\" xpath=\"/root/node/id\" />\n" +
          "            <field column=\"desc\" xpath=\"/root/node/desc\" />\n" +
          "        </entity>\n" +
          "    </document>\n" +
          "</dataConfig>";

  private String dataConfigWithoutStreaming = "<dataConfig>\n" +
          "        <dataSource name=\"str\" type=\"TestErrorHandling$StringDataSource\" />" +
          "    <document>\n" +
          "        <entity name=\"node\" dataSource=\"str\" processor=\"XPathEntityProcessor\" url=\"test\" forEach=\"/root/node\" onError=\"skip\">\n" +
          "            <field column=\"id\" xpath=\"/root/node/id\" />\n" +
          "            <field column=\"desc\" xpath=\"/root/node/desc\" />\n" +
          "        </entity>\n" +
          "    </document>\n" +
          "</dataConfig>";

  private String dataConfigAbortOnError = "<dataConfig>\n" +
          "        <dataSource name=\"str\" type=\"TestErrorHandling$StringDataSource\" />" +
          "    <document>\n" +
          "        <entity name=\"node\" dataSource=\"str\" processor=\"XPathEntityProcessor\" url=\"test\" forEach=\"/root/node\" onError=\"abort\">\n" +
          "            <field column=\"id\" xpath=\"/root/node/id\" />\n" +
          "            <field column=\"desc\" xpath=\"/root/node/desc\" />\n" +
          "        </entity>\n" +
          "    </document>\n" +
          "</dataConfig>";

  private String dataConfigWithTransformer = "<dataConfig>\n" +
          "        <dataSource name=\"str\" type=\"TestErrorHandling$StringDataSource\" />" +
          "<dataSource  type=\"MockDataSource\"/>" +
          "    <document>\n" +
          "        <entity name=\"node\" dataSource=\"str\" processor=\"XPathEntityProcessor\" url=\"test\" forEach=\"/root/node\">\n" +
          "            <field column=\"id\" xpath=\"/root/node/id\" />\n" +
          "            <field column=\"desc\" xpath=\"/root/node/desc\" />\n" +
          "            <entity name=\"child\" query=\"select * from foo\" transformer=\"TestErrorHandling$ExceptionTransformer\" onError=\"continue\">\n" +
          "            </entity>" +
          "        </entity>\n" +
          "    </document>\n" +
          "</dataConfig>";

  private String dataConfigWithEntity = "<!DOCTYPE dataConfig [\n" + 
          "  <!ENTITY internalTerm \"node\">\n" + 
          "  <!ENTITY externalTerm SYSTEM \"foo://bar.xyz/external\">\n" + 
          "]><dataConfig>\n" +
          "    <dataSource name=\"str\" type=\"TestErrorHandling$StringDataSource\" />" +
          "    <document>\n" +
          "        <entity name=\"&internalTerm;\" dataSource=\"str\" processor=\"XPathEntityProcessor\" url=\"test\" forEach=\"/root/node\" onError=\"skip\">\n" +
          "            <field column=\"id\" xpath=\"/root/node/id\">&externalTerm;</field>\n" +
          "            <field column=\"desc\" xpath=\"/root/node/desc\" />\n" +
          "        </entity>\n" +
          "    </document>\n" +
          "</dataConfig>";

  private String malformedXml = "<root>\n" +
          "    <node>\n" +
          "        <id>1</id>\n" +
          "        <desc>test1</desc>\n" +
          "    </node>\n" +
          "    <node>\n" +
          "        <id>2</id>\n" +
          "        <desc>test2</desc>\n" +
          "    </node>\n" +
          "    <node>\n" +
          "        <id/>3</id>\n" +
          "        <desc>test3</desc>\n" +
          "    </node>\n" +
          "</root>";

  private String wellformedXml = "<root>\n" +
          "    <node>\n" +
          "        <id>1</id>\n" +
          "        <desc>test1</desc>\n" +
          "    </node>\n" +
          "    <node>\n" +
          "        <id>2</id>\n" +
          "        <desc>test2</desc>\n" +
          "    </node>\n" +
          "    <node>\n" +
          "        <id>3</id>\n" +
          "        <desc>test3</desc>\n" +
          "    </node>\n" +
          "</root>";
}
