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

import org.junit.Test;

import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * Test for XPathEntityProcessor
 * </p>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestXPathEntityProcessor extends AbstractDataImportHandlerTestCase {
  boolean simulateSlowReader;
  boolean simulateSlowResultProcessor;
  int rowsToRead = -1;
  
  @Test
  public void withFieldsAndXpath() throws Exception {
    File tmpdir = File.createTempFile("test", "tmp", TEMP_DIR);
    tmpdir.delete();
    tmpdir.mkdir();
    tmpdir.deleteOnExit();
    createFile(tmpdir, "x.xsl", xsl.getBytes("UTF-8"), false);
    Map entityAttrs = createMap("name", "e", "url", "cd.xml",
            XPathEntityProcessor.FOR_EACH, "/catalog/cd");
    List fields = new ArrayList();
    fields.add(createMap("column", "title", "xpath", "/catalog/cd/title"));
    fields.add(createMap("column", "artist", "xpath", "/catalog/cd/artist"));
    fields.add(createMap("column", "year", "xpath", "/catalog/cd/year"));
    Context c = getContext(null,
            new VariableResolverImpl(), getDataSource(cdData), Context.FULL_DUMP, fields, entityAttrs);
    XPathEntityProcessor xPathEntityProcessor = new XPathEntityProcessor();
    xPathEntityProcessor.init(c);
    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
    while (true) {
      Map<String, Object> row = xPathEntityProcessor.nextRow();
      if (row == null)
        break;
      result.add(row);
    }
    assertEquals(3, result.size());
    assertEquals("Empire Burlesque", result.get(0).get("title"));
    assertEquals("Bonnie Tyler", result.get(1).get("artist"));
    assertEquals("1982", result.get(2).get("year"));
  }

  @Test
  public void testMultiValued() throws Exception  {
    Map entityAttrs = createMap("name", "e", "url", "testdata.xml",
            XPathEntityProcessor.FOR_EACH, "/root");
    List fields = new ArrayList();
    fields.add(createMap("column", "a", "xpath", "/root/a", DataImporter.MULTI_VALUED, "true"));
    Context c = getContext(null,
            new VariableResolverImpl(), getDataSource(testXml), Context.FULL_DUMP, fields, entityAttrs);
    XPathEntityProcessor xPathEntityProcessor = new XPathEntityProcessor();
    xPathEntityProcessor.init(c);
    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
    while (true) {
      Map<String, Object> row = xPathEntityProcessor.nextRow();
      if (row == null)
        break;
      result.add(row);
    }
    assertEquals(2, ((List)result.get(0).get("a")).size());
  }

  @Test
  public void testMultiValuedFlatten() throws Exception  {
    Map entityAttrs = createMap("name", "e", "url", "testdata.xml",
            XPathEntityProcessor.FOR_EACH, "/root");
    List fields = new ArrayList();
    fields.add(createMap("column", "a", "xpath", "/root/a" ,"flatten","true"));
    Context c = getContext(null,
            new VariableResolverImpl(), getDataSource(testXmlFlatten), Context.FULL_DUMP, fields, entityAttrs);
    XPathEntityProcessor xPathEntityProcessor = new XPathEntityProcessor();
    xPathEntityProcessor.init(c);
    Map<String, Object> result = null;
    while (true) {
      Map<String, Object> row = xPathEntityProcessor.nextRow();
      if (row == null)
        break;
      result = row;
    }
    assertEquals("1B2", result.get("a"));
  }

  @Test
  public void withFieldsAndXpathStream() throws Exception {
    final Object monitor = new Object();
    final boolean[] done = new boolean[1];
    
    Map entityAttrs = createMap("name", "e", "url", "cd.xml",
        XPathEntityProcessor.FOR_EACH, "/catalog/cd", "stream", "true", "batchSize","1");
    List fields = new ArrayList();
    fields.add(createMap("column", "title", "xpath", "/catalog/cd/title"));
    fields.add(createMap("column", "artist", "xpath", "/catalog/cd/artist"));
    fields.add(createMap("column", "year", "xpath", "/catalog/cd/year"));
    Context c = getContext(null,
        new VariableResolverImpl(), getDataSource(cdData), Context.FULL_DUMP, fields, entityAttrs);
    XPathEntityProcessor xPathEntityProcessor = new XPathEntityProcessor() {
      private int count;
      
      @Override
      protected Map<String, Object> readRow(Map<String, Object> record,
          String xpath) {
        synchronized (monitor) {
          if (simulateSlowReader && !done[0]) {
            try {
              monitor.wait(100);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        }
        
        return super.readRow(record, xpath);
      }
    };
    
    if (simulateSlowResultProcessor) {
      xPathEntityProcessor.blockingQueueSize = 1;
    }
    xPathEntityProcessor.blockingQueueTimeOut = 1;
    xPathEntityProcessor.blockingQueueTimeOutUnits = TimeUnit.MICROSECONDS;
    
    xPathEntityProcessor.init(c);
    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
    while (true) {
      if (rowsToRead >= 0 && result.size() >= rowsToRead) {
        Thread.currentThread().interrupt();
      }
      Map<String, Object> row = xPathEntityProcessor.nextRow();
      if (row == null)
        break;
      result.add(row);
      if (simulateSlowResultProcessor) {
        synchronized (xPathEntityProcessor.publisherThread) {
          if (xPathEntityProcessor.publisherThread.isAlive()) {
            xPathEntityProcessor.publisherThread.wait(1000);
          }
        }
      }
    }
    
    synchronized (monitor) {
      done[0] = true;
      monitor.notify();
    }
    
    // confirm that publisher thread stops.
    xPathEntityProcessor.publisherThread.join(1000);
    assertEquals("Expected thread to stop", false, xPathEntityProcessor.publisherThread.isAlive());
    
    assertEquals(rowsToRead < 0 ? 3 : rowsToRead, result.size());
    
    if (rowsToRead < 0) {
      assertEquals("Empire Burlesque", result.get(0).get("title"));
      assertEquals("Bonnie Tyler", result.get(1).get("artist"));
      assertEquals("1982", result.get(2).get("year"));
    }
  }

  @Test
  public void withFieldsAndXpathStreamContinuesOnTimeout() throws Exception {
    simulateSlowReader = true;
    withFieldsAndXpathStream();
  }
  
  @Test
  public void streamWritesMessageAfterBlockedAttempt() throws Exception {
    simulateSlowResultProcessor = true;
    withFieldsAndXpathStream();
  }
  
  @Test
  public void streamStopsAfterInterrupt() throws Exception {
    simulateSlowResultProcessor = true;
    rowsToRead = 1;
    withFieldsAndXpathStream();
  }
  
  @Test
  public void withDefaultSolrAndXsl() throws Exception {
    File tmpdir = File.createTempFile("test", "tmp", TEMP_DIR);
    tmpdir.delete();
    tmpdir.mkdir();
    tmpdir.deleteOnExit();
    TestFileListEntityProcessor.createFile(tmpdir, "x.xsl", xsl.getBytes("UTF-8"),
            false);
    Map entityAttrs = createMap("name", "e",
            XPathEntityProcessor.USE_SOLR_ADD_SCHEMA, "true", "xsl", ""
            + new File(tmpdir, "x.xsl").getAbsolutePath(), "url", "cd.xml");
    Context c = getContext(null,
            new VariableResolverImpl(), getDataSource(cdData), Context.FULL_DUMP, null, entityAttrs);
    XPathEntityProcessor xPathEntityProcessor = new XPathEntityProcessor();
    xPathEntityProcessor.init(c);
    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
    while (true) {
      Map<String, Object> row = xPathEntityProcessor.nextRow();
      if (row == null)
        break;
      result.add(row);
    }
    assertEquals(3, result.size());
    assertEquals("Empire Burlesque", result.get(0).get("title"));
    assertEquals("Bonnie Tyler", result.get(1).get("artist"));
    assertEquals("1982", result.get(2).get("year"));
  }

  private DataSource<Reader> getDataSource(final String xml) {
    return new DataSource<Reader>() {

      @Override
      public void init(Context context, Properties initProps) {
      }

      @Override
      public void close() {
      }

      @Override
      public Reader getData(String query) {
        return new StringReader(xml);
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

  private static final String testXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><root><a>1</a><a>2</a></root>";

  private static final String testXmlFlatten = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><root><a>1<b>B</b>2</a></root>";
}
