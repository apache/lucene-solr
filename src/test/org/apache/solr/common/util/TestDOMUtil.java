package org.apache.solr.common.util;

import org.junit.Test;
import org.junit.Assert;
import org.xml.sax.InputSource;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;

public class TestDOMUtil {
  @Test
  public void readFlexiSchemaNodes() throws Exception {
    String xml ="<root>\n" +
            "  <a>val a</a>\n" +
            "  <b>val b</b>\n" +
            "  <c>\n" +
            "     <d>val d</d>\n" +
            "     <e>val e</e>\n" +
            "  </c>\n" +
            "</root>";
    NamedList nl = DOMUtil.childNodesToNamedList(getRootNode(xml));
    Assert.assertEquals("val a",nl.get("a"));
    Assert.assertEquals("val b",nl.get("b"));
    NamedList c = (NamedList) nl.get("c");
    Assert.assertEquals("val d",c.get("d"));
    Assert.assertEquals("val e",c.get("e"));
  }

  @Test
  public void readFlexiSchemaNodesWithAttributes() throws Exception {
    String xml ="<root>\n" +
            "  <defaults a=\"A\" b=\"B\" c=\"C\">\n" +
            "    <x>X1</x>\n" +
            "    <x>X2</x>\n" +
            "  </defaults>\n" +
            "</root>";
    NamedList nl = DOMUtil.childNodesToNamedList(getRootNode(xml));
    NamedList defaults = (NamedList) nl.get("defaults");
    Assert.assertEquals("A",defaults.get("@a"));
    Assert.assertEquals("B",defaults.get("@b"));
    Assert.assertEquals("C",defaults.get("@c"));
    Assert.assertEquals("X1",defaults.getVal(3));
    Assert.assertEquals("X2",defaults.getVal(4));
  }
  @Test
  public void readFlexiSchemaNodesWithAttributesWithOldFormat() throws Exception {
    String xml ="<root>\n" +
            "  <defaults a=\"A\" b=\"B\" c=\"C\">\n" +
            "    <str name=\"x\">X1</str>\n" +
            "    <str name=\"x\">X2</str>\n" +
            "    <bool name=\"boo\">true</bool>\n" +
            "  </defaults>\n" +
            "</root>";
    NamedList nl = DOMUtil.childNodesToNamedList(getRootNode(xml));
    NamedList defaults = (NamedList) nl.get("defaults");
    Assert.assertEquals("A",defaults.get("@a"));
    Assert.assertEquals("B",defaults.get("@b"));
    Assert.assertEquals("C",defaults.get("@c"));
    Assert.assertEquals("X1",defaults.getVal(3));
    Assert.assertEquals("X2",defaults.getVal(4));
    Assert.assertEquals(Boolean.TRUE,defaults.get("boo"));
  }

  private Node getRootNode(String xml) throws Exception {
    javax.xml.parsers.DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    Document doc = builder.parse(new InputSource(new StringReader(xml)));
    return doc.getDocumentElement();
  }
}
