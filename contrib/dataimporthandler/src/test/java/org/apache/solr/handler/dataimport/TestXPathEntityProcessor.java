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

import static org.apache.solr.handler.dataimport.AbstractDataImportHandlerTest.createMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * <p>
 * Test for XPathEntityProcessor
 * </p>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestXPathEntityProcessor {
  @Test
  public void withFieldsAndXpath() throws Exception {
    long time = System.currentTimeMillis();
    File tmpdir = new File("." + time);
    tmpdir.mkdir();
    tmpdir.deleteOnExit();
    TestFileListEntityProcessor.createFile(tmpdir, "x.xsl", xsl.getBytes(),
            false);
    Map entityAttrs = createMap("name", "e", "url", "cd.xml",
            XPathEntityProcessor.FOR_EACH, "/catalog/cd");
    List fields = new ArrayList();
    fields.add(createMap("column", "title", "xpath", "/catalog/cd/title"));
    fields.add(createMap("column", "artist", "xpath", "/catalog/cd/artist"));
    fields.add(createMap("column", "year", "xpath", "/catalog/cd/year"));
    Context c = AbstractDataImportHandlerTest.getContext(null,
            new VariableResolverImpl(), getds(), 0, fields, entityAttrs);
    XPathEntityProcessor xPathEntityProcessor = new XPathEntityProcessor();
    xPathEntityProcessor.init(c);
    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
    while (true) {
      Map<String, Object> row = xPathEntityProcessor.nextRow();
      if (row == null)
        break;
      result.add(row);
    }
    Assert.assertEquals(3, result.size());
    Assert.assertEquals("Empire Burlesque", result.get(0).get("title"));
    Assert.assertEquals("Bonnie Tyler", result.get(1).get("artist"));
    Assert.assertEquals("1982", result.get(2).get("year"));
  }

  @Test
  public void withDefaultSolrAndXsl() throws Exception {
    long time = System.currentTimeMillis();
    File tmpdir = new File("." + time);
    tmpdir.mkdir();
    tmpdir.deleteOnExit();
    TestFileListEntityProcessor.createFile(tmpdir, "x.xsl", xsl.getBytes(),
            false);
    Map entityAttrs = createMap("name", "e",
            XPathEntityProcessor.USE_SOLR_ADD_SCHEMA, "true", "xsl", ""
            + new File(tmpdir, "x.xsl").getAbsolutePath(), "url", "cd.xml");
    Context c = AbstractDataImportHandlerTest.getContext(null,
            new VariableResolverImpl(), getds(), 0, null, entityAttrs);
    XPathEntityProcessor xPathEntityProcessor = new XPathEntityProcessor();
    xPathEntityProcessor.init(c);
    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
    while (true) {
      Map<String, Object> row = xPathEntityProcessor.nextRow();
      if (row == null)
        break;
      result.add(row);
    }
    Assert.assertEquals(3, result.size());
    Assert.assertEquals("Empire Burlesque", result.get(0).get("title"));
    Assert.assertEquals("Bonnie Tyler", result.get(1).get("artist"));
    Assert.assertEquals("1982", result.get(2).get("year"));
  }

  private DataSource<Reader> getds() {
    return new DataSource<Reader>() {

      public void init(Context context, Properties initProps) {
      }

      public void close() {
      }

      public Reader getData(String query) {
        return new StringReader(cdData);
      }
    };
  }

  private static final String xsl = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<xsl:stylesheet version=\"1.0\"\n"
          + "xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">\n"
          + "<xsl:output version='1.0' method='xml' encoding='UTF-8' indent='yes'/>\n"
          + "\n"
          + "<xsl:template match=\"/\">\n"
          + "  <add> \n"
          + "      <xsl:for-each select=\"catalog/cd\">\n"
          + "      <doc>\n"
          + "      <field name=\"title\"><xsl:value-of select=\"title\"/></field>\n"
          + "      <field name=\"artist\"><xsl:value-of select=\"artist\"/></field>\n"
          + "      <field name=\"country\"><xsl:value-of select=\"country\"/></field>\n"
          + "      <field name=\"company\"><xsl:value-of select=\"company\"/></field>      \n"
          + "      <field name=\"price\"><xsl:value-of select=\"price\"/></field>\n"
          + "      <field name=\"year\"><xsl:value-of select=\"year\"/></field>      \n"
          + "      </doc>\n"
          + "      </xsl:for-each>\n"
          + "    </add>  \n"
          + "</xsl:template>\n" + "</xsl:stylesheet>";

  private static final String cdData = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<?xml-stylesheet type=\"text/xsl\" href=\"solr.xsl\"?>\n"
          + "<catalog>\n"
          + "\t<cd>\n"
          + "\t\t<title>Empire Burlesque</title>\n"
          + "\t\t<artist>Bob Dylan</artist>\n"
          + "\t\t<country>USA</country>\n"
          + "\t\t<company>Columbia</company>\n"
          + "\t\t<price>10.90</price>\n"
          + "\t\t<year>1985</year>\n"
          + "\t</cd>\n"
          + "\t<cd>\n"
          + "\t\t<title>Hide your heart</title>\n"
          + "\t\t<artist>Bonnie Tyler</artist>\n"
          + "\t\t<country>UK</country>\n"
          + "\t\t<company>CBS Records</company>\n"
          + "\t\t<price>9.90</price>\n"
          + "\t\t<year>1988</year>\n"
          + "\t</cd>\n"
          + "\t<cd>\n"
          + "\t\t<title>Greatest Hits</title>\n"
          + "\t\t<artist>Dolly Parton</artist>\n"
          + "\t\t<country>USA</country>\n"
          + "\t\t<company>RCA</company>\n"
          + "\t\t<price>9.90</price>\n"
          + "\t\t<year>1982</year>\n" + "\t</cd>\n" + "</catalog>\t";
}
