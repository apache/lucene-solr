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

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

/**
 * <p> Test for XPathRecordReader </p>
 *
 *
 * @since solr 1.3
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class TestXPathRecordReader extends AbstractDataImportHandlerTestCase {
  @Test
  public void testBasic() {
    String xml="<root>\n"
             + "   <b><c>Hello C1</c>\n"
             + "      <c>Hello C1</c>\n"
             + "      </b>\n"
             + "   <b><c>Hello C2</c>\n"
             + "     </b>\n"
             + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/b");
    rr.addField("c", "/root/b/c", true);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    assertEquals(2, l.size());
    assertEquals(2, ((List) l.get(0).get("c")).size());
    assertEquals(1, ((List) l.get(1).get("c")).size());
  }

  @Test
  public void testAttributes() {
    String xml="<root>\n"
             + "   <b a=\"x0\" b=\"y0\" />\n"
             + "   <b a=\"x1\" b=\"y1\" />\n"
             + "   <b a=\"x2\" b=\"y2\" />\n"
            + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/b");
    rr.addField("a", "/root/b/@a", false);
    rr.addField("b", "/root/b/@b", false);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    assertEquals(3, l.size());
    assertEquals("x0", l.get(0).get("a"));
    assertEquals("x1", l.get(1).get("a"));
    assertEquals("x2", l.get(2).get("a"));
    assertEquals("y0", l.get(0).get("b"));
    assertEquals("y1", l.get(1).get("b"));
    assertEquals("y2", l.get(2).get("b"));
  }
  
  @Test
  public void testAttrInRoot(){
    String xml="<r>\n" +
            "<merchantProduct id=\"814636051\" mid=\"189973\">\n" +
            "                   <in_stock type=\"stock-4\" />\n" +
            "                   <condition type=\"cond-0\" />\n" +
            "                   <price>301.46</price>\n" +
               "   </merchantProduct>\n" +
            "<merchantProduct id=\"814636052\" mid=\"189974\">\n" +
            "                   <in_stock type=\"stock-5\" />\n" +
            "                   <condition type=\"cond-1\" />\n" +
            "                   <price>302.46</price>\n" +
               "   </merchantProduct>\n" +
            "\n" +
            "</r>";
     XPathRecordReader rr = new XPathRecordReader("/r/merchantProduct");
    rr.addField("id", "/r/merchantProduct/@id", false);
    rr.addField("mid", "/r/merchantProduct/@mid", false);
    rr.addField("price", "/r/merchantProduct/price", false);
    rr.addField("conditionType", "/r/merchantProduct/condition/@type", false);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    Map<String, Object> m = l.get(0);
    assertEquals("814636051", m.get("id"));
    assertEquals("189973", m.get("mid"));
    assertEquals("301.46", m.get("price"));
    assertEquals("cond-0", m.get("conditionType"));

    m = l.get(1);
    assertEquals("814636052", m.get("id"));
    assertEquals("189974", m.get("mid"));
    assertEquals("302.46", m.get("price"));
    assertEquals("cond-1", m.get("conditionType"));
  }

  @Test
  public void testAttributes2Level() {
    String xml="<root>\n"
             + "<a>\n  <b a=\"x0\" b=\"y0\" />\n"
             + "       <b a=\"x1\" b=\"y1\" />\n"
             + "       <b a=\"x2\" b=\"y2\" />\n"
             + "       </a>"
             + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/a/b");
    rr.addField("a", "/root/a/b/@a", false);
    rr.addField("b", "/root/a/b/@b", false);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    assertEquals(3, l.size());
    assertEquals("x0", l.get(0).get("a"));
    assertEquals("y1", l.get(1).get("b"));
  }

  @Test
  public void testAttributes2LevelHetero() {
    String xml="<root>\n"
             + "<a>\n   <b a=\"x0\" b=\"y0\" />\n"
             + "        <b a=\"x1\" b=\"y1\" />\n"
             + "        <b a=\"x2\" b=\"y2\" />\n"
             + "        </a>"
             + "<x>\n   <b a=\"x4\" b=\"y4\" />\n"
             + "        <b a=\"x5\" b=\"y5\" />\n"
             + "        <b a=\"x6\" b=\"y6\" />\n"
             + "        </x>"
             + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/a | /root/x");
    rr.addField("a", "/root/a/b/@a", false);
    rr.addField("b", "/root/a/b/@b", false);
    rr.addField("a", "/root/x/b/@a", false);
    rr.addField("b", "/root/x/b/@b", false);

    final List<Map<String, Object>> a = new ArrayList<>();
    final List<Map<String, Object>> x = new ArrayList<>();
    rr.streamRecords(new StringReader(xml), (record, xpath) -> {
      if (record == null) return;
      if (xpath.equals("/root/a")) a.add(record);
      if (xpath.equals("/root/x")) x.add(record);
    });

    assertEquals(1, a.size());
    assertEquals(1, x.size());
  }

  @Test
  public void testAttributes2LevelMissingAttrVal() {
    String xml="<root>\n"
             + "<a>\n  <b a=\"x0\" b=\"y0\" />\n"
             + "       <b a=\"x1\" b=\"y1\" />\n"
             + "       </a>"
             + "<a>\n  <b a=\"x3\"  />\n"
             + "       <b b=\"y4\" />\n"
             + "       </a>"
             + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/a");
    rr.addField("a", "/root/a/b/@a", true);
    rr.addField("b", "/root/a/b/@b", true);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    assertEquals(2, l.size());
    assertNull(((List) l.get(1).get("a")).get(1));
    assertNull(((List) l.get(1).get("b")).get(0));
  }

  @Test
  public void testElems2LevelMissing() {
    String xml="<root>\n"
             + "\t<a>\n"
             + "\t   <b>\n\t  <x>x0</x>\n"
             + "\t            <y>y0</y>\n"
             + "\t            </b>\n"
             + "\t   <b>\n\t  <x>x1</x>\n"
             + "\t            <y>y1</y>\n"
             + "\t            </b>\n"
             + "\t   </a>\n"
             + "\t<a>\n"
             + "\t   <b>\n\t  <x>x3</x>\n\t   </b>\n"
             + "\t   <b>\n\t  <y>y4</y>\n\t   </b>\n"
             + "\t   </a>\n"
             + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/a");
    rr.addField("a", "/root/a/b/x", true);
    rr.addField("b", "/root/a/b/y", true);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    assertEquals(2, l.size());
    assertNull(((List) l.get(1).get("a")).get(1));
    assertNull(((List) l.get(1).get("b")).get(0));
  }

  @Test
  public void testElems2LevelEmpty() {
    String xml="<root>\n"
             + "\t<a>\n"
             + "\t   <b>\n\t  <x>x0</x>\n"
             + "\t            <y>y0</y>\n"
             + "\t   </b>\n"
             + "\t   <b>\n\t  <x></x>\n"    // empty
             + "\t            <y>y1</y>\n"
             + "\t   </b>\n"
             + "\t</a>\n"
             + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/a");
    rr.addField("a", "/root/a/b/x", true);
    rr.addField("b", "/root/a/b/y", true);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    assertEquals(1, l.size());
    assertEquals("x0",((List) l.get(0).get("a")).get(0));
    assertEquals("y0",((List) l.get(0).get("b")).get(0));
    assertEquals("",((List) l.get(0).get("a")).get(1));
    assertEquals("y1",((List) l.get(0).get("b")).get(1));
  }

  @Test
  public void testMixedContent() {
    String xml = "<xhtml:p xmlns:xhtml=\"http://xhtml.com/\" >This text is \n" +
            "  <xhtml:b>bold</xhtml:b> and this text is \n" +
            "  <xhtml:u>underlined</xhtml:u>!\n" +
            "</xhtml:p>";
    XPathRecordReader rr = new XPathRecordReader("/p");
    rr.addField("p", "/p", true);
    rr.addField("b", "/p/b", true);
    rr.addField("u", "/p/u", true);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    Map<String, Object> row = l.get(0);

    assertEquals("bold", ((List) row.get("b")).get(0));
    assertEquals("underlined", ((List) row.get("u")).get(0));
    String p = (String) ((List) row.get("p")).get(0);
    assertTrue(p.contains("This text is"));
    assertTrue(p.contains("and this text is"));
    assertTrue(p.contains("!"));
    // Should not contain content from child elements
    assertFalse(p.contains("bold"));
  }

  @Test
  public void testMixedContentFlattened() {
    String xml = "<xhtml:p xmlns:xhtml=\"http://xhtml.com/\" >This text is \n" +
            "  <xhtml:b>bold</xhtml:b> and this text is \n" +
            "  <xhtml:u>underlined</xhtml:u>!\n" +
            "</xhtml:p>";
    XPathRecordReader rr = new XPathRecordReader("/p");
    rr.addField("p", "/p", false, XPathRecordReader.FLATTEN);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    Map<String, Object> row = l.get(0);
    assertEquals("This text is \n" +
            "  bold and this text is \n" +
            "  underlined!", ((String)row.get("p")).trim() );
  }

  @Test
  public void testElems2LevelWithAttrib() {
    String xml = "<root>\n\t<a>\n\t   <b k=\"x\">\n"
            + "\t                        <x>x0</x>\n"
            + "\t                        <y></y>\n"  // empty
            + "\t                        </b>\n"
            + "\t                     <b k=\"y\">\n"
            + "\t                        <x></x>\n"  // empty
            + "\t                        <y>y1</y>\n"
            + "\t                        </b>\n"
            + "\t                     <b k=\"z\">\n"
            + "\t                        <x>x2</x>\n"
            + "\t                        <y>y2</y>\n"
            + "\t                        </b>\n"
            + "\t                </a>\n"
            + "\t           <a>\n\t   <b>\n"
            + "\t                        <x>x3</x>\n"
            + "\t                        </b>\n"
            + "\t                     <b>\n"
            + "\t                     <y>y4</y>\n"
            + "\t                        </b>\n"
            + "\t               </a>\n"
            + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/a");
    rr.addField("x", "/root/a/b[@k]/x", true);
    rr.addField("y", "/root/a/b[@k]/y", true);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    assertEquals(2, l.size());
    assertEquals(3, ((List) l.get(0).get("x")).size());
    assertEquals(3, ((List) l.get(0).get("y")).size());
    assertEquals("x0", ((List) l.get(0).get("x")).get(0));
    assertEquals("", ((List) l.get(0).get("y")).get(0));
    assertEquals("", ((List) l.get(0).get("x")).get(1));
    assertEquals("y1", ((List) l.get(0).get("y")).get(1));
    assertEquals("x2", ((List) l.get(0).get("x")).get(2));
    assertEquals("y2", ((List) l.get(0).get("y")).get(2));
    assertEquals(0, l.get(1).size());
  }

  @Test
  public void testElems2LevelWithAttribMultiple() {
    String xml="<root>\n"
             + "\t<a>\n\t   <b k=\"x\" m=\"n\" >\n"
             + "\t             <x>x0</x>\n"
             + "\t             <y>y0</y>\n"
             + "\t             </b>\n"
             + "\t          <b k=\"y\" m=\"p\">\n"
             + "\t             <x>x1</x>\n"
             + "\t             <y>y1</y>\n"
             + "\t             </b>\n"
             + "\t   </a>\n"
             + "\t<a>\n\t   <b k=\"x\">\n"
             + "\t             <x>x3</x>\n"
             + "\t             </b>\n"
             + "\t          <b m=\"n\">\n"
             + "\t             <y>y4</y>\n"
             + "\t             </b>\n"
             + "\t   </a>\n"
             + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/a");
    rr.addField("x", "/root/a/b[@k][@m='n']/x", true);
    rr.addField("y", "/root/a/b[@k][@m='n']/y", true);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    assertEquals(2, l.size());
    assertEquals(1, ((List) l.get(0).get("x")).size());
    assertEquals(1, ((List) l.get(0).get("y")).size());
    assertEquals(0, l.get(1).size());
  }

  @Test
  public void testElems2LevelWithAttribVal() {
    String xml="<root>\n\t<a>\n   <b k=\"x\">\n"
             + "\t                  <x>x0</x>\n"
             + "\t                  <y>y0</y>\n"
             + "\t                  </b>\n"
             + "\t                <b k=\"y\">\n"
             + "\t                  <x>x1</x>\n"
             + "\t                  <y>y1</y>\n"
             + "\t                  </b>\n"
             + "\t                </a>\n"
             + "\t        <a>\n   <b><x>x3</x></b>\n"
             + "\t                <b><y>y4</y></b>\n"
             + "\t</a>\n" + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/a");
    rr.addField("x", "/root/a/b[@k='x']/x", true);
    rr.addField("y", "/root/a/b[@k='x']/y", true);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    assertEquals(2, l.size());
    assertEquals(1, ((List) l.get(0).get("x")).size());
    assertEquals(1, ((List) l.get(0).get("y")).size());
    assertEquals(0, l.get(1).size());
  }

  @Test
  public void testAttribValWithSlash() {
    String xml = "<root><b>\n" +
            "  <a x=\"a/b\" h=\"hello-A\"/>  \n" +
            "</b></root>";
    XPathRecordReader rr = new XPathRecordReader("/root/b");
    rr.addField("x", "/root/b/a[@x='a/b']/@h", false);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    assertEquals(1, l.size());
    Map<String, Object> m = l.get(0);
    assertEquals("hello-A", m.get("x"));    
  }

  @Test
  public void testUnsupportedXPaths() {
    RuntimeException ex = expectThrows(RuntimeException.class, () -> new XPathRecordReader("//b"));
    assertEquals("forEach cannot start with '//': //b", ex.getMessage());

    XPathRecordReader rr = new XPathRecordReader("/anyd/contenido");
    ex = expectThrows(RuntimeException.class, () -> rr.addField("bold", "b", false));
    assertEquals("xpath must start with '/' : b", ex.getMessage());
  }

  @Test
  public void testAny_decendent_from_root() {
    XPathRecordReader rr = new XPathRecordReader("/anyd/contenido");
    rr.addField("descdend", "//boo",                   true);
    rr.addField("inr_descd","//boo/i",                false);
    rr.addField("cont",     "/anyd/contenido",        false);
    rr.addField("id",       "/anyd/contenido/@id",    false);
    rr.addField("status",   "/anyd/status",           false);
    rr.addField("title",    "/anyd/contenido/titulo", false,XPathRecordReader.FLATTEN);
    rr.addField("resume",   "/anyd/contenido/resumen",false);
    rr.addField("text",     "/anyd/contenido/texto",  false);

    String xml="<anyd>\n"
             + "  this <boo>top level</boo> is ignored because it is external to the forEach\n"
             + "  <status>as is <boo>this element</boo></status>\n"
             + "  <contenido id=\"10097\" idioma=\"cat\">\n"
             + "    This one is <boo>not ignored as it's</boo> inside a forEach\n"
             + "    <antetitulo><i> big <boo>antler</boo></i></antetitulo>\n"
             + "    <titulo>  My <i>flattened <boo>title</boo></i> </titulo>\n"
             + "    <resumen> My summary <i>skip this!</i>  </resumen>\n"
             + "    <texto>   <boo>Within the body of</boo>My text</texto>\n"
             + "    <p>Access <boo>inner <i>sub clauses</i> as well</boo></p>\n"
             + "    </contenido>\n"
             + "</anyd>";

    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    assertEquals(1, l.size());
    Map<String, Object> m = l.get(0);
    assertEquals("This one is  inside a forEach", m.get("cont").toString().trim());
    assertEquals("10097"              ,m.get("id"));
    assertEquals("My flattened title" ,m.get("title").toString().trim());
    assertEquals("My summary"         ,m.get("resume").toString().trim());
    assertEquals("My text"            ,m.get("text").toString().trim());
    assertEquals("not ignored as it's",(String) ((List) m.get("descdend")).get(0) );
    assertEquals("antler"             ,(String) ((List) m.get("descdend")).get(1) );
    assertEquals("Within the body of" ,(String) ((List) m.get("descdend")).get(2) );
    assertEquals("inner  as well"     ,(String) ((List) m.get("descdend")).get(3) );
    assertEquals("sub clauses"        ,m.get("inr_descd").toString().trim());
  }

  @Test
  public void testAny_decendent_of_a_child1() {
    XPathRecordReader rr = new XPathRecordReader("/anycd");
    rr.addField("descdend", "/anycd//boo",         true);

    // same test string as above but checking to see if *all* //boo's are collected
    String xml="<anycd>\n"
             + "  this <boo>top level</boo> is ignored because it is external to the forEach\n"
             + "  <status>as is <boo>this element</boo></status>\n"
             + "  <contenido id=\"10097\" idioma=\"cat\">\n"
             + "    This one is <boo>not ignored as it's</boo> inside a forEach\n"
             + "    <antetitulo><i> big <boo>antler</boo></i></antetitulo>\n"
             + "    <titulo>  My <i>flattened <boo>title</boo></i> </titulo>\n"
             + "    <resumen> My summary <i>skip this!</i>  </resumen>\n"
             + "    <texto>   <boo>Within the body of</boo>My text</texto>\n"
             + "    <p>Access <boo>inner <i>sub clauses</i> as well</boo></p>\n"
             + "    </contenido>\n"
             + "</anycd>";

    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    assertEquals(1, l.size());
    Map<String, Object> m = l.get(0);
    assertEquals("top level"          ,(String) ((List) m.get("descdend")).get(0) );
    assertEquals("this element"       ,(String) ((List) m.get("descdend")).get(1) );
    assertEquals("not ignored as it's",(String) ((List) m.get("descdend")).get(2) );
    assertEquals("antler"             ,(String) ((List) m.get("descdend")).get(3) );
    assertEquals("title"              ,(String) ((List) m.get("descdend")).get(4) );
    assertEquals("Within the body of" ,(String) ((List) m.get("descdend")).get(5) );
    assertEquals("inner  as well"     ,(String) ((List) m.get("descdend")).get(6) );
  }

  @Test
  public void testAny_decendent_of_a_child2() {
    XPathRecordReader rr = new XPathRecordReader("/anycd");
    rr.addField("descdend", "/anycd/contenido//boo",         true);

    // same test string as above but checking to see if *some* //boo's are collected
    String xml="<anycd>\n"
             + "  this <boo>top level</boo> is ignored because it is external to the forEach\n"
             + "  <status>as is <boo>this element</boo></status>\n"
             + "  <contenido id=\"10097\" idioma=\"cat\">\n"
             + "    This one is <boo>not ignored as it's</boo> inside a forEach\n"
             + "    <antetitulo><i> big <boo>antler</boo></i></antetitulo>\n"
             + "    <titulo>  My <i>flattened <boo>title</boo></i> </titulo>\n"
             + "    <resumen> My summary <i>skip this!</i>  </resumen>\n"
             + "    <texto>   <boo>Within the body of</boo>My text</texto>\n"
             + "    <p>Access <boo>inner <i>sub clauses</i> as well</boo></p>\n"
             + "    </contenido>\n"
             + "</anycd>";

    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    assertEquals(1, l.size());
    Map<String, Object> m = l.get(0);
    assertEquals("not ignored as it's",((List) m.get("descdend")).get(0) );
    assertEquals("antler"             ,((List) m.get("descdend")).get(1) );
    assertEquals("title"              ,((List) m.get("descdend")).get(2) );
    assertEquals("Within the body of" ,((List) m.get("descdend")).get(3) );
    assertEquals("inner  as well"     ,((List) m.get("descdend")).get(4) );
  }
  
  @Test
  public void testAnother() {
    String xml="<root>\n"
            + "       <contenido id=\"10097\" idioma=\"cat\">\n"
             + "    <antetitulo></antetitulo>\n"
             + "    <titulo>    This is my title             </titulo>\n"
             + "    <resumen>   This is my summary           </resumen>\n"
             + "    <texto>     This is the body of my text  </texto>\n"
             + "    </contenido>\n"
             + "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/contenido");
    rr.addField("id", "/root/contenido/@id", false);
    rr.addField("title", "/root/contenido/titulo", false);
    rr.addField("resume","/root/contenido/resumen",false);
    rr.addField("text", "/root/contenido/texto", false);

    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    assertEquals(1, l.size());
    Map<String, Object> m = l.get(0);
    assertEquals("10097", m.get("id"));
    assertEquals("This is my title", m.get("title").toString().trim());
    assertEquals("This is my summary", m.get("resume").toString().trim());
    assertEquals("This is the body of my text", m.get("text").toString()
            .trim());
  }

  @Test
  public void testSameForEachAndXpath(){
    String xml="<root>\n" +
            "   <cat>\n" +
            "     <name>hello</name>\n" +
            "   </cat>\n" +
            "   <item name=\"item name\"/>\n" +
            "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/cat/name");
    rr.addField("catName", "/root/cat/name",false);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    assertEquals("hello",l.get(0).get("catName"));
  }

  @Test
  public void testPutNullTest(){
    String xml = "<root>\n" +
            "  <i>\n" +
            "    <x>\n" +
            "      <a>A.1.1</a>\n" +
            "      <b>B.1.1</b>\n" +
            "    </x>\n" +
            "    <x>\n" +
            "      <b>B.1.2</b>\n" +
            "      <c>C.1.2</c>\n" +
            "    </x>\n" +
            "  </i>\n" +
            "  <i>\n" +
            "    <x>\n" +
            "      <a>A.2.1</a>\n" +
            "      <c>C.2.1</c>\n" +
            "    </x>\n" +
            "    <x>\n" +
            "      <b>B.2.2</b>\n" +
            "      <c>C.2.2</c>\n" +
            "    </x>\n" +
            "  </i>\n" +
            "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/i");
    rr.addField("a", "/root/i/x/a", true);
    rr.addField("b", "/root/i/x/b", true);
    rr.addField("c", "/root/i/x/c", true);
    List<Map<String, Object>> l = rr.getAllRecords(new StringReader(xml));
    Map<String, Object> map = l.get(0);
    List<String> a = (List<String>) map.get("a");
    List<String> b = (List<String>) map.get("b");
    List<String> c = (List<String>) map.get("c");

    assertEquals("A.1.1",a.get(0));
    assertEquals("B.1.1",b.get(0));
    assertNull(c.get(0));

    assertNull(a.get(1));
    assertEquals("B.1.2",b.get(1));
    assertEquals("C.1.2",c.get(1));

    map = l.get(1);
    a = (List<String>) map.get("a");
    b = (List<String>) map.get("b");
    c = (List<String>) map.get("c");
    assertEquals("A.2.1",a.get(0));
    assertNull(b.get(0));
    assertEquals("C.2.1",c.get(0));

    assertNull(a.get(1));
    assertEquals("B.2.2",b.get(1));
    assertEquals("C.2.2",c.get(1));
  }


  @Test
  public void testError(){
    String malformedXml = "<root>\n" +
          "    <node>\n" +
          "        <id>1</id>\n" +
          "        <desc>test1</desc>\n" +
          "    </node>\n" +
          "    <node>\n" +
          "        <id>2</id>\n" +
          "        <desc>test2</desc>\n" +
          "    </node>\n" +
          "    <node>\n" +
          "        <id/>3</id>\n" +   // invalid XML
          "        <desc>test3</desc>\n" +
          "    </node>\n" +
          "</root>";
    XPathRecordReader rr = new XPathRecordReader("/root/node");
    rr.addField("id", "/root/node/id", true);
    rr.addField("desc", "/root/node/desc", true);
    RuntimeException e = expectThrows(RuntimeException.class, () -> rr.getAllRecords(new StringReader(malformedXml)));
    assertTrue(e.getMessage().contains("Unexpected close tag </id>"));
 }
}
