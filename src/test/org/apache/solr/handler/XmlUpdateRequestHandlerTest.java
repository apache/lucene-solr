package org.apache.solr.handler;

import org.apache.solr.util.AbstractSolrTestCase;
import java.io.StringReader;
import java.util.Collection;

import javanet.staxutils.BaseXMLInputFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import junit.framework.TestCase;

import org.apache.solr.common.SolrInputDocument;

public class XmlUpdateRequestHandlerTest extends AbstractSolrTestCase 
{
  private XMLInputFactory inputFactory = BaseXMLInputFactory.newInstance();
  protected XmlUpdateRequestHandler handler;

@Override public String getSchemaFile() { return "schema.xml"; }
@Override public String getSolrConfigFile() { return "solrconfig.xml"; }

  @Override 
  public void setUp() throws Exception {
    super.setUp();
    handler = new XmlUpdateRequestHandler();
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
    assertEquals( 5.5f, doc.getDocumentBoost() );
    assertEquals( 1.0f, doc.getField( "name" ).getBoost() );
    assertEquals( 2.2f, doc.getField( "id" ).getBoost() );
    // Boost is the product of each value
    assertEquals( (3*4*5.0f), doc.getField( "cat" ).getBoost() );
    
    // Read values
    assertEquals( "12345", doc.getField( "id" ).getValue() );
    assertEquals( "kitten", doc.getField( "name").getValue() );
    assertEquals( "a&b", doc.getField( "ab").getValue() ); // read something with escaped characters
    
    Collection<Object> out = doc.getField( "cat" ).getValues();
    assertEquals( 3, out.size() );
    assertEquals( "[aaa, bbb, bbb]", out.toString() );
  }
}
