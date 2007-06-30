package org.apache.solr.handler;

import java.io.StringReader;
import java.util.Collection;

import javanet.staxutils.BaseXMLInputFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import junit.framework.TestCase;

import org.apache.solr.common.SolrInputDocument;

public class XmlUpdateRequestHandlerTest extends TestCase 
{
  private XMLInputFactory inputFactory = BaseXMLInputFactory.newInstance();
  protected XmlUpdateRequestHandler handler = new XmlUpdateRequestHandler();

  @Override 
  public void setUp() throws Exception {
    super.setUp();
  }
  
  @Override 
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  public void testReadDoc() throws Exception
  {
    String xml = 
      "<doc boost=\"5.5\">" +
      "  <field name=\"id\" boost=\"2.2\">12345</field>" +
      "  <field name=\"name\">kitten</field>" +
      "  <field name=\"cat\" boost=\"3\">aaa</field>" +
      "  <field name=\"cat\" boost=\"4\">bbb</field>" +
      "  <field name=\"cat\" boost=\"5\">bbb</field>" +
      "  <field name=\"ab\">a&amp;b</field>" +
      "</doc>";

    XMLStreamReader parser = 
      inputFactory.createXMLStreamReader( new StringReader( xml ) );
    parser.next(); // read the START document...
    
    SolrInputDocument doc = handler.readDoc( parser );
    
    // Read boosts
    assertEquals( new Float(5.5f), doc.getBoost(null) );
    assertEquals( null, doc.getBoost( "name" ) );
    assertEquals( new Float(2.2f), doc.getBoost( "id" ) );
    assertEquals( null, doc.getBoost( "ab" ) );
    // Boost is the product of each value
    assertEquals( new Float(3*4*5), doc.getBoost( "cat" ) );
    
    // Read values
    assertEquals( "12345", doc.getFieldValue( "id") );
    assertEquals( "kitten", doc.getFieldValue( "name") );
    assertEquals( "a&b", doc.getFieldValue( "ab") ); // read something with escaped characters
    
    Collection<Object> out = doc.getFieldValues( "cat" );
    assertEquals( 3, out.size() );
    assertEquals( "[aaa, bbb, bbb]", out.toString() );
  }
}
