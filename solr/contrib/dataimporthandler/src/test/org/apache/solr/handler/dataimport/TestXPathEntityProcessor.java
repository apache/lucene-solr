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
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * <p>
 * Test for XPathEntityProcessor
 * </p>
 *
 *
 * @since solr 1.3
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class TestXPathEntityProcessor extends AbstractDataImportHandlerTestCase {
  boolean simulateSlowReader;
  boolean simulateSlowResultProcessor;
  int rowsToRead = -1;
  
  @Test
  public void withFieldsAndXpath() throws Exception {
    File tmpdir = createTempDir().toFile();
    
    createFile(tmpdir, "x.xsl", xsl.getBytes(StandardCharsets.UTF_8), false);
    Map entityAttrs = createMap("name", "e", "url", "cd.xml",
            XPathEntityProcessor.FOR_EACH, "/catalog/cd");
    List fields = new ArrayList();
    fields.add(createMap("column", "title", "xpath", "/catalog/cd/title"));
    fields.add(createMap("column", "artist", "xpath", "/catalog/cd/artist"));
    fields.add(createMap("column", "year", "xpath", "/catalog/cd/year"));
    Context c = getContext(null,
            new VariableResolver(), getDataSource(cdData), Context.FULL_DUMP, fields, entityAttrs);
    XPathEntityProcessor xPathEntityProcessor = new XPathEntityProcessor();
    xPathEntityProcessor.init(c);
    List<Map<String, Object>> result = new ArrayList<>();
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
            new VariableResolver(), getDataSource(testXml), Context.FULL_DUMP, fields, entityAttrs);
    XPathEntityProcessor xPathEntityProcessor = new XPathEntityProcessor();
    xPathEntityProcessor.init(c);
    List<Map<String, Object>> result = new ArrayList<>();
    while (true) {
      Map<String, Object> row = xPathEntityProcessor.nextRow();
      if (row == null)
        break;
      result.add(row);
    }
    List l = (List)result.get(0).get("a");
    assertEquals(3, l.size());
    assertEquals("1", l.get(0));
    assertEquals("2", l.get(1));
    assertEquals("ü", l.get(2));
  }
  
  @Test
  public void testMultiValuedWithMultipleDocuments() throws Exception {
    Map entityAttrs = createMap("name", "e", "url", "testdata.xml", XPathEntityProcessor.FOR_EACH, "/documents/doc");
    List fields = new ArrayList();
    fields.add(createMap("column", "id", "xpath", "/documents/doc/id", DataImporter.MULTI_VALUED, "false"));
    fields.add(createMap("column", "a", "xpath", "/documents/doc/a", DataImporter.MULTI_VALUED, "true"));
    fields.add(createMap("column", "s1dataA", "xpath", "/documents/doc/sec1/s1dataA", DataImporter.MULTI_VALUED, "true"));
    fields.add(createMap("column", "s1dataB", "xpath", "/documents/doc/sec1/s1dataB", DataImporter.MULTI_VALUED, "true")); 
    fields.add(createMap("column", "s1dataC", "xpath", "/documents/doc/sec1/s1dataC", DataImporter.MULTI_VALUED, "true")); 
    
    Context c = getContext(null,
            new VariableResolver(), getDataSource(textMultipleDocuments), Context.FULL_DUMP, fields, entityAttrs);
    XPathEntityProcessor xPathEntityProcessor = new XPathEntityProcessor();
    xPathEntityProcessor.init(c);
    List<Map<String, Object>> result = new ArrayList<>();
    while (true) {
      Map<String, Object> row = xPathEntityProcessor.nextRow();
      if (row == null)
        break;
      result.add(row);
    }
    {  
      assertEquals("1", result.get(0).get("id"));
      List a = (List)result.get(0).get("a");
      List s1dataA = (List)result.get(0).get("s1dataA");
      List s1dataB = (List)result.get(0).get("s1dataB");
      List s1dataC = (List)result.get(0).get("s1dataC");      
      assertEquals(2, a.size());
      assertEquals("id1-a1", a.get(0));
      assertEquals("id1-a2", a.get(1));
      assertEquals(3, s1dataA.size());
      assertEquals("id1-s1dataA-1", s1dataA.get(0));
      assertNull(s1dataA.get(1));
      assertEquals("id1-s1dataA-3", s1dataA.get(2));
      assertEquals(3, s1dataB.size());
      assertEquals("id1-s1dataB-1", s1dataB.get(0));
      assertEquals("id1-s1dataB-2", s1dataB.get(1));
      assertEquals("id1-s1dataB-3", s1dataB.get(2));
      assertEquals(3, s1dataC.size());
      assertNull(s1dataC.get(0));
      assertNull(s1dataC.get(1));
      assertNull(s1dataC.get(2));
    }
    { 
      assertEquals("2", result.get(1).get("id"));
      List a = (List)result.get(1).get("a");
      List s1dataA = (List)result.get(1).get("s1dataA");
      List s1dataB = (List)result.get(1).get("s1dataB");
      List s1dataC = (List)result.get(1).get("s1dataC");  
      assertTrue(a==null || a.size()==0);
      assertEquals(1, s1dataA.size()); 
      assertNull(s1dataA.get(0));
      assertEquals(1, s1dataB.size());
      assertEquals("id2-s1dataB-1", s1dataB.get(0));
      assertEquals(1, s1dataC.size());
      assertNull(s1dataC.get(0));
    }  
    {
      assertEquals("3", result.get(2).get("id"));
      List a = (List)result.get(2).get("a");
      List s1dataA = (List)result.get(2).get("s1dataA");
      List s1dataB = (List)result.get(2).get("s1dataB");
      List s1dataC = (List)result.get(2).get("s1dataC");  
      assertTrue(a==null || a.size()==0);
      assertEquals(1, s1dataA.size());
      assertEquals("id3-s1dataA-1", s1dataA.get(0));
      assertEquals(1, s1dataB.size());
      assertNull(s1dataB.get(0));
      assertEquals(1, s1dataC.size());
      assertNull(s1dataC.get(0)); 
    }
    {  
      assertEquals("4", result.get(3).get("id"));
      List a = (List)result.get(3).get("a");
      List s1dataA = (List)result.get(3).get("s1dataA");
      List s1dataB = (List)result.get(3).get("s1dataB");
      List s1dataC = (List)result.get(3).get("s1dataC");  
      assertTrue(a==null || a.size()==0);
      assertEquals(1, s1dataA.size());
      assertEquals("id4-s1dataA-1", s1dataA.get(0));
      assertEquals(1, s1dataB.size());
      assertEquals("id4-s1dataB-1", s1dataB.get(0));
      assertEquals(1, s1dataC.size());
      assertEquals("id4-s1dataC-1", s1dataC.get(0));
    }
    {
      assertEquals("5", result.get(4).get("id"));
      List a = (List)result.get(4).get("a");
      List s1dataA = (List)result.get(4).get("s1dataA");
      List s1dataB = (List)result.get(4).get("s1dataB");
      List s1dataC = (List)result.get(4).get("s1dataC");  
      assertTrue(a==null || a.size()==0);      
      assertEquals(1, s1dataA.size());
      assertNull(s1dataA.get(0)); 
      assertEquals(1, s1dataB.size());
      assertNull(s1dataB.get(0)); 
      assertEquals(1, s1dataC.size());
      assertEquals("id5-s1dataC-1", s1dataC.get(0));
    }
    {  
      assertEquals("6", result.get(5).get("id"));
      List a = (List)result.get(5).get("a");
      List s1dataA = (List)result.get(5).get("s1dataA");
      List s1dataB = (List)result.get(5).get("s1dataB");
      List s1dataC = (List)result.get(5).get("s1dataC");     
      assertTrue(a==null || a.size()==0); 
      assertEquals(3, s1dataA.size());
      assertEquals("id6-s1dataA-1", s1dataA.get(0));
      assertEquals("id6-s1dataA-2", s1dataA.get(1));
      assertNull(s1dataA.get(2));
      assertEquals(3, s1dataB.size());
      assertEquals("id6-s1dataB-1", s1dataB.get(0));
      assertEquals("id6-s1dataB-2", s1dataB.get(1));
      assertEquals("id6-s1dataB-3", s1dataB.get(2));
      assertEquals(3, s1dataC.size());
      assertEquals("id6-s1dataC-1", s1dataC.get(0));
      assertNull(s1dataC.get(1));
      assertEquals("id6-s1dataC-3", s1dataC.get(2));
    }
  }

  @Test
  public void testMultiValuedFlatten() throws Exception  {
    Map entityAttrs = createMap("name", "e", "url", "testdata.xml",
            XPathEntityProcessor.FOR_EACH, "/root");
    List fields = new ArrayList();
    fields.add(createMap("column", "a", "xpath", "/root/a" ,"flatten","true"));
    Context c = getContext(null,
            new VariableResolver(), getDataSource(testXmlFlatten), Context.FULL_DUMP, fields, entityAttrs);
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
        new VariableResolver(), getDataSource(cdData), Context.FULL_DUMP, fields, entityAttrs);
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
    List<Map<String, Object>> result = new ArrayList<>();
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
    File tmpdir = createTempDir().toFile();
    AbstractDataImportHandlerTestCase.createFile(tmpdir, "x.xsl", xsl.getBytes(StandardCharsets.UTF_8),
            false);

    Map entityAttrs = createMap("name", "e",
            XPathEntityProcessor.USE_SOLR_ADD_SCHEMA, "true", "xsl", ""
            + new File(tmpdir, "x.xsl").toURI(), "url", "cd.xml");
    Context c = getContext(null,
            new VariableResolver(), getDataSource(cdData), Context.FULL_DUMP, null, entityAttrs);
    XPathEntityProcessor xPathEntityProcessor = new XPathEntityProcessor();
    xPathEntityProcessor.init(c);
    List<Map<String, Object>> result = new ArrayList<>();
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

  private static final String testXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE root [\n<!ENTITY uuml \"&#252;\" >\n]>\n<root><a>1</a><a>2</a><a>&uuml;</a></root>";

  private static final String testXmlFlatten = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><root><a>1<b>B</b>2</a></root>";
  
  private static final String textMultipleDocuments = 
      "<?xml version=\"1.0\" ?>" +
          "<documents>" +          
          " <doc>" +
          "  <id>1</id>" +
          "  <a>id1-a1</a>" +
          "  <a>id1-a2</a>" +
          "  <sec1>" +
          "   <s1dataA>id1-s1dataA-1</s1dataA>" +
          "   <s1dataB>id1-s1dataB-1</s1dataB>" +
          "  </sec1>" +
          "  <sec1>" +
          "   <s1dataB>id1-s1dataB-2</s1dataB>" +
          "  </sec1>" +
          "  <sec1>" +
          "   <s1dataA>id1-s1dataA-3</s1dataA>" +
          "   <s1dataB>id1-s1dataB-3</s1dataB>" +
          "  </sec1>" +
          " </doc>" +
          " <doc>" +
          "  <id>2</id>" +          
          "  <sec1>" +
          "   <s1dataB>id2-s1dataB-1</s1dataB>" +
          "  </sec1>" + 
          " </doc>" +
          " <doc>" +
          "  <id>3</id>" +          
          "  <sec1>" +
          "   <s1dataA>id3-s1dataA-1</s1dataA>" +
          "  </sec1>" + 
          " </doc>" +
          " <doc>" +
          "  <id>4</id>" +          
          "  <sec1>" +
          "   <s1dataA>id4-s1dataA-1</s1dataA>" +
          "   <s1dataB>id4-s1dataB-1</s1dataB>" +
          "   <s1dataC>id4-s1dataC-1</s1dataC>" +
          "  </sec1>" + 
          " </doc>" +
          " <doc>" +
          "  <id>5</id>" +          
          "  <sec1>" +
          "   <s1dataC>id5-s1dataC-1</s1dataC>" +
          "  </sec1>" + 
          " </doc>" +
          " <doc>" +
          "  <id>6</id>" +
          "  <sec1>" +
          "   <s1dataA>id6-s1dataA-1</s1dataA>" +
          "   <s1dataB>id6-s1dataB-1</s1dataB>" +
          "   <s1dataC>id6-s1dataC-1</s1dataC>" +
          "  </sec1>" +
          "  <sec1>" +
          "   <s1dataA>id6-s1dataA-2</s1dataA>" +
          "   <s1dataB>id6-s1dataB-2</s1dataB>" +
          "  </sec1>" +
          "  <sec1>" +
          "   <s1dataB>id6-s1dataB-3</s1dataB>" +
          "   <s1dataC>id6-s1dataC-3</s1dataC>" +
          "  </sec1>" +
          " </doc>" +
          "</documents>"
         ;
}
