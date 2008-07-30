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
 * distributed under the License is distributed onT an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.dataimport;

import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * Test for XPathRecordReader
 * </p>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestXPathRecordReader {
  @Test
  public void basic() {
    String xml = "<root>\n" + "   <b>\n" + "     <c>Hello C1</c>\n"
            + "     <c>Hello C1</c>\n" + "   </b>\n" + "   <b>\n"
            + "     <c>Hello C2</c>\n" + "   </b>\n" + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/b");
    rr.addField("c", "/root/b/c", true);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    Assert.assertEquals(2, l.size());
    Assert.assertEquals(2, ((List) l.get(0).get("c")).size());
    Assert.assertEquals(1, ((List) l.get(1).get("c")).size());
  }

  @Test
  public void attributes() {
    String xml = "<root>\n" + "   <b a=\"x0\" b=\"y0\" />\n"
            + "   <b a=\"x1\" b=\"y1\" />\n" + "   <b a=\"x2\" b=\"y2\" />\n"
            + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/b");
    rr.addField("a", "/root/b/@a", false);
    rr.addField("b", "/root/b/@b", false);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    Assert.assertEquals(3, l.size());
    Assert.assertEquals("x0", l.get(0).get("a"));
    Assert.assertEquals("y1", l.get(1).get("b"));
  }

  @Test
  public void attributes2Level() {
    String xml = "<root>\n" + "<a>\n" + "   <b a=\"x0\" b=\"y0\" />\n"
            + "   <b a=\"x1\" b=\"y1\" />\n" + "   <b a=\"x2\" b=\"y2\" />\n"
            + "</a>" + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/a/b");
    rr.addField("a", "/root/a/b/@a", false);
    rr.addField("b", "/root/a/b/@b", false);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    Assert.assertEquals(3, l.size());
    Assert.assertEquals("x0", l.get(0).get("a"));
    Assert.assertEquals("y1", l.get(1).get("b"));
  }

  @Test
  public void attributes2LevelHetero() {
    String xml = "<root>\n" + "<a>\n" + "   <b a=\"x0\" b=\"y0\" />\n"
            + "   <b a=\"x1\" b=\"y1\" />\n" + "   <b a=\"x2\" b=\"y2\" />\n"
            + "</a>" + "<x>\n" + "   <b a=\"x4\" b=\"y4\" />\n"
            + "   <b a=\"x5\" b=\"y5\" />\n" + "   <b a=\"x6\" b=\"y6\" />\n"
            + "</x>" + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/a | /root/x");
    rr.addField("a", "/root/a/b/@a", false);
    rr.addField("b", "/root/a/b/@b", false);
    rr.addField("a", "/root/x/b/@a", false);
    rr.addField("b", "/root/x/b/@b", false);

    final List<Map<String, Object>> a = new ArrayList<Map<String, Object>>();
    final List<Map<String, Object>> x = new ArrayList<Map<String, Object>>();
    rr.streamRecords(new StringReader(xml), new XPathRecordReader.Handler() {
      public void handle(Map<String, Object> record, String xpath) {
        if (record == null)
          return;
        if (xpath.equals("/root/a"))
          a.add(record);
        if (xpath.equals("/root/x"))
          x.add(record);
      }
    });

    Assert.assertEquals(1, a.size());
    Assert.assertEquals(1, x.size());
  }

  @Test
  public void attributes2LevelMissingAttrVal() {
    String xml = "<root>\n" + "<a>\n" + "   <b a=\"x0\" b=\"y0\" />\n"
            + "   <b a=\"x1\" b=\"y1\" />\n" + "</a>" + "<a>\n"
            + "   <b a=\"x3\"  />\n" + "   <b b=\"y4\" />\n" + "</a>" + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/a");
    rr.addField("a", "/root/a/b/@a", true);
    rr.addField("b", "/root/a/b/@b", true);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    Assert.assertEquals(2, l.size());
    Assert.assertNull(((List) l.get(1).get("a")).get(1));
    Assert.assertNull(((List) l.get(1).get("b")).get(0));
  }

  @Test
  public void elems2LevelMissing() {
    String xml = "<root>\n" + "\t<a>\n" + "\t   <b>\n" + "\t      <x>x0</x>\n"
            + "\t      <y>y0</y>\n" + "\t   </b>\n" + "\t   <b>\n"
            + "\t   \t<x>x1</x>\n" + "\t   \t<y>y1</y>\n" + "\t   </b>\n"
            + "\t</a>\n" + "\t<a>\n" + "\t   <b>\n" + "\t      <x>x3</x>\n"
            + "\t   </b>\n" + "\t   <b>\n" + "\t   \t<y>y4</y>\n" + "\t   </b>\n"
            + "\t</a>\n" + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/a");
    rr.addField("a", "/root/a/b/x", true);
    rr.addField("b", "/root/a/b/y", true);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    Assert.assertEquals(2, l.size());
    Assert.assertNull(((List) l.get(1).get("a")).get(1));
    Assert.assertNull(((List) l.get(1).get("b")).get(0));
  }

  @Test
  public void elems2LevelWithAttrib() {
    String xml = "<root>\n" + "\t<a>\n" + "\t   <b k=\"x\">\n"
            + "\t      <x>x0</x>\n" + "\t      <y>y0</y>\n" + "\t   </b>\n"
            + "\t   <b k=\"y\">\n" + "\t   \t<x>x1</x>\n" + "\t   \t<y>y1</y>\n"
            + "\t   </b>\n" + "\t</a>\n" + "\t<a>\n" + "\t   <b>\n"
            + "\t      <x>x3</x>\n" + "\t   </b>\n" + "\t   <b>\n"
            + "\t   \t<y>y4</y>\n" + "\t   </b>\n" + "\t</a>\n" + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/a");
    rr.addField("x", "/root/a/b[@k]/x", true);
    rr.addField("y", "/root/a/b[@k]/y", true);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    Assert.assertEquals(2, l.size());
    Assert.assertEquals(2, ((List) l.get(0).get("x")).size());
    Assert.assertEquals(2, ((List) l.get(0).get("y")).size());
    Assert.assertEquals(0, l.get(1).size());
  }

  @Test
  public void elems2LevelWithAttribMultiple() {
    String xml = "<root>\n" + "\t<a>\n" + "\t   <b k=\"x\" m=\"n\" >\n"
            + "\t      <x>x0</x>\n" + "\t      <y>y0</y>\n" + "\t   </b>\n"
            + "\t   <b k=\"y\" m=\"p\">\n" + "\t   \t<x>x1</x>\n"
            + "\t   \t<y>y1</y>\n" + "\t   </b>\n" + "\t</a>\n" + "\t<a>\n"
            + "\t   <b k=\"x\">\n" + "\t      <x>x3</x>\n" + "\t   </b>\n"
            + "\t   <b m=\"n\">\n" + "\t   \t<y>y4</y>\n" + "\t   </b>\n"
            + "\t</a>\n" + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/a");
    rr.addField("x", "/root/a/b[@k][@m='n']/x", true);
    rr.addField("y", "/root/a/b[@k][@m='n']/y", true);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    Assert.assertEquals(2, l.size());
    Assert.assertEquals(1, ((List) l.get(0).get("x")).size());
    Assert.assertEquals(1, ((List) l.get(0).get("y")).size());
    Assert.assertEquals(0, l.get(1).size());
  }

  @Test
  public void elems2LevelWithAttribVal() {
    String xml = "<root>\n" + "\t<a>\n" + "\t   <b k=\"x\">\n"
            + "\t      <x>x0</x>\n" + "\t      <y>y0</y>\n" + "\t   </b>\n"
            + "\t   <b k=\"y\">\n" + "\t   \t<x>x1</x>\n" + "\t   \t<y>y1</y>\n"
            + "\t   </b>\n" + "\t</a>\n" + "\t<a>\n" + "\t   <b>\n"
            + "\t      <x>x3</x>\n" + "\t   </b>\n" + "\t   <b>\n"
            + "\t   \t<y>y4</y>\n" + "\t   </b>\n" + "\t</a>\n" + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/a");
    rr.addField("x", "/root/a/b[@k='x']/x", true);
    rr.addField("y", "/root/a/b[@k='x']/y", true);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    Assert.assertEquals(2, l.size());
    Assert.assertEquals(1, ((List) l.get(0).get("x")).size());
    Assert.assertEquals(1, ((List) l.get(0).get("y")).size());
    Assert.assertEquals(0, l.get(1).size());
  }

  @Test
  public void another() {
    String xml = "<root>\n"
            + "       <contenido id=\"10097\" idioma=\"cat\">\n"
            + "       <antetitulo></antetitulo>\n" + "       <titulo>\n"
            + "               This is my title\n" + "       </titulo>\n"
            + "       <resumen>\n" + "               This is my summary\n"
            + "       </resumen>\n" + "       <texto>\n"
            + "               This is the body of my text\n" + "       </texto>\n"
            + "       </contenido>\n" + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/contenido");
    rr.addField("id", "/root/contenido/@id", false);
    rr.addField("title", "/root/contenido/titulo", false);
    rr.addField("resume", "/root/contenido/resumen", false);
    rr.addField("text", "/root/contenido/texto", false);

    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    Assert.assertEquals(1, l.size());
    Map<String, Object> m = l.get(0);
    Assert.assertEquals("10097", m.get("id").toString().trim());
    Assert.assertEquals("This is my title", m.get("title").toString().trim());
    Assert
            .assertEquals("This is my summary", m.get("resume").toString().trim());
    Assert.assertEquals("This is the body of my text", m.get("text").toString()
            .trim());
  }

}
