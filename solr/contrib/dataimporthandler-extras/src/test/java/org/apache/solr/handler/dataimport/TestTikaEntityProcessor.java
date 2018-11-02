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

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Locale;

/**Testcase for TikaEntityProcessor
 *
 * @since solr 3.1
 */
public class TestTikaEntityProcessor extends AbstractDataImportHandlerTestCase {
  private String conf =
  "<dataConfig>" +
  "  <dataSource type=\"BinFileDataSource\"/>" +
  "  <document>" +
  "    <entity name=\"Tika\" processor=\"TikaEntityProcessor\" url=\"" + getFile("dihextras/solr-word.pdf").getAbsolutePath() + "\" >" +
  "      <field column=\"Author\" meta=\"true\" name=\"author\"/>" +
  "      <field column=\"title\" meta=\"true\" name=\"title\"/>" +
  "      <field column=\"text\"/>" +
  "     </entity>" +
  "  </document>" +
  "</dataConfig>";

  private String skipOnErrConf =
      "<dataConfig>" +
          "  <dataSource type=\"BinFileDataSource\"/>" +
          "  <document>" +
          "    <entity name=\"Tika\" onError=\"skip\"  processor=\"TikaEntityProcessor\" url=\"" + getFile("dihextras/bad.doc").getAbsolutePath() + "\" >" +
          "<field column=\"content\" name=\"text\"/>" +
          " </entity>" +
          " <entity name=\"Tika\" processor=\"TikaEntityProcessor\" url=\"" + getFile("dihextras/solr-word.pdf").getAbsolutePath() + "\" >" +
          "      <field column=\"text\"/>" +
          "</entity>" +
          "  </document>" +
          "</dataConfig>";

  private String spatialConf =
      "<dataConfig>" +
          "  <dataSource type=\"BinFileDataSource\"/>" +
          "  <document>" +
          "    <entity name=\"Tika\" processor=\"TikaEntityProcessor\" url=\"" +
          getFile("dihextras/test_jpeg.jpg").getAbsolutePath() + "\" spatialMetadataField=\"home\">" +
          "      <field column=\"text\"/>" +
          "     </entity>" +
          "  </document>" +
          "</dataConfig>";

  private String vsdxConf =
      "<dataConfig>" +
          "  <dataSource type=\"BinFileDataSource\"/>" +
          "  <document>" +
          "    <entity name=\"Tika\" processor=\"TikaEntityProcessor\" url=\"" + getFile("dihextras/test_vsdx.vsdx").getAbsolutePath() + "\" >" +
          "      <field column=\"text\"/>" +
          "     </entity>" +
          "  </document>" +
          "</dataConfig>";

  private String[] tests = {
      "//*[@numFound='1']"
      ,"//str[@name='author'][.='Grant Ingersoll']"
      ,"//str[@name='title'][.='solr-word']"
      ,"//str[@name='text']"
  };

  private String[] testsHTMLDefault = {
      "//*[@numFound='1']"
      , "//str[@name='text'][contains(.,'Basic div')]"
      , "//str[@name='text'][contains(.,'<h1>')]"
      , "//str[@name='text'][not(contains(.,'<div>'))]" //default mapper lower-cases elements as it maps
      , "//str[@name='text'][not(contains(.,'<DIV>'))]"
  };

  private String[] testsHTMLIdentity = {
      "//*[@numFound='1']"
      , "//str[@name='text'][contains(.,'Basic div')]"
      , "//str[@name='text'][contains(.,'<h1>')]"
      , "//str[@name='text'][contains(.,'<div>')]"
      , "//str[@name='text'][contains(.,'class=\"classAttribute\"')]" //attributes are lower-cased
  };

  private String[] testsSpatial = {
      "//*[@numFound='1']"
  };

  private String[] testsEmbedded = {
      "//*[@numFound='1']",
      "//str[@name='text'][contains(.,'When in the Course')]"
  };

  private String[] testsIgnoreEmbedded = {
      "//*[@numFound='1']",
      "//str[@name='text'][not(contains(.,'When in the Course'))]"
  };

  private String[] testsVSDX = {
      "//*[@numFound='1']",
      "//str[@name='text'][contains(.,'Arrears')]"
  };

  @BeforeClass
  public static void beforeClass() throws Exception {
    assumeFalse("This test fails on UNIX with Turkish default locale (https://issues.apache.org/jira/browse/SOLR-6387)",
        new Locale("tr").getLanguage().equals(Locale.getDefault().getLanguage()));
    initCore("dataimport-solrconfig.xml", "dataimport-schema-no-unique-key.xml", getFile("dihextras/solr").getAbsolutePath());
  }

  @Test
  public void testIndexingWithTikaEntityProcessor() throws Exception {
    runFullImport(conf);
    assertQ(req("*:*"), tests );
  }

  @Test
  public void testSkip() throws Exception {
    runFullImport(skipOnErrConf);
    assertQ(req("*:*"), "//*[@numFound='1']");
  }

  @Test
  public void testVSDX() throws Exception {
    //this ensures that we've included the curvesapi dependency
    //and that the ConnectsType class is bundled with poi-ooxml-schemas.
    runFullImport(vsdxConf);
    assertQ(req("*:*"), testsVSDX);
  }

  @Test
  public void testTikaHTMLMapperEmpty() throws Exception {
    runFullImport(getConfigHTML(null));
    assertQ(req("*:*"), testsHTMLDefault);
  }

  @Test
  public void testTikaHTMLMapperDefault() throws Exception {
    runFullImport(getConfigHTML("default"));
    assertQ(req("*:*"), testsHTMLDefault);
  }

  @Test
  public void testTikaHTMLMapperIdentity() throws Exception {
    runFullImport(getConfigHTML("identity"));
    assertQ(req("*:*"), testsHTMLIdentity);
  }

  @Test
  public void testTikaGeoMetadata() throws Exception {
    runFullImport(spatialConf);
    String pt = "38.97,-77.018";
    Double distance = 5.0d;
    assertQ(req("q", "*:* OR foo_i:" + random().nextInt(100), "fq",
        "{!geofilt sfield=\"home\"}\"",
        "pt", pt, "d", String.valueOf(distance)), testsSpatial);
  }

  private String getConfigHTML(String htmlMapper) {
    return
        "<dataConfig>" +
            "  <dataSource type='BinFileDataSource'/>" +
            "  <document>" +
            "    <entity name='Tika' format='xml' processor='TikaEntityProcessor' " +
            "       url='" + getFile("dihextras/structured.html").getAbsolutePath() + "' " +
            ((htmlMapper == null) ? "" : (" htmlMapper='" + htmlMapper + "'")) + ">" +
            "      <field column='text'/>" +
            "     </entity>" +
            "  </document>" +
            "</dataConfig>";

  }

  @Test
  public void testEmbeddedDocsLegacy() throws Exception {
    //test legacy behavior: ignore embedded docs
    runFullImport(conf);
    assertQ(req("*:*"), testsIgnoreEmbedded);
  }

  @Test
  public void testEmbeddedDocsTrue() throws Exception {
    runFullImport(getConfigEmbedded(true));
    assertQ(req("*:*"), testsEmbedded);
  }

  @Test
  public void testEmbeddedDocsFalse() throws Exception {
    runFullImport(getConfigEmbedded(false));
    assertQ(req("*:*"), testsIgnoreEmbedded);
  }

  private String getConfigEmbedded(boolean extractEmbedded) {
    return
        "<dataConfig>" +
            "  <dataSource type=\"BinFileDataSource\"/>" +
            "  <document>" +
            "    <entity name=\"Tika\" processor=\"TikaEntityProcessor\" url=\"" +
                    getFile("dihextras/test_recursive_embedded.docx").getAbsolutePath() + "\" " +
            "       extractEmbedded=\""+extractEmbedded+"\">" +
            "      <field column=\"Author\" meta=\"true\" name=\"author\"/>" +
            "      <field column=\"title\" meta=\"true\" name=\"title\"/>" +
            "      <field column=\"text\"/>" +
            "     </entity>" +
            "  </document>" +
            "</dataConfig>";
  }
}
