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
package org.apache.solr.handler;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.handler.loader.XMLLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.BufferingRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;

public class XmlUpdateRequestHandlerTest extends SolrTestCaseJ4 {
  private static XMLInputFactory inputFactory;
  protected static UpdateRequestHandler handler;

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml","schema.xml");
    handler = new UpdateRequestHandler();
    inputFactory = XMLInputFactory.newInstance();
  }

  @AfterClass
  public static void afterTests() {
    inputFactory = null;
    handler = null;
  }

  @Test
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
    //null for the processor is all right here
    XMLLoader loader = new XMLLoader();
    SolrInputDocument doc = loader.readDoc( parser );
    
    // Read values
    assertEquals( "12345", doc.getField( "id" ).getValue() );
    assertEquals( "kitten", doc.getField( "name").getValue() );
    assertEquals( "a&b", doc.getField( "ab").getValue() ); // read something with escaped characters
    
    Collection<Object> out = doc.getField( "cat" ).getValues();
    assertEquals( 3, out.size() );
    assertEquals( "[aaa, bbb, bbb]", out.toString() );
  }
  
  @Test
  public void testRequestParams() throws Exception
  {
    String xml = 
      "<add>" +
      "  <doc>" +
      "    <field name=\"id\">12345</field>" +
      "    <field name=\"name\">kitten</field>" +
      "  </doc>" +
      "</add>";

    SolrQueryRequest req = req("commitWithin","100","overwrite","false");
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);

    XMLLoader loader = new XMLLoader().init(null);
    loader.load(req, rsp, new ContentStreamBase.StringStream(xml), p);

    AddUpdateCommand add = p.addCommands.get(0);
    assertEquals(100, add.commitWithin);
    assertEquals(false, add.overwrite);
    req.close();
  }
  
  @Test
  public void testExternalEntities() throws Exception
  {
    String file = getFile("mailing_lists.pdf").toURI().toASCIIString();
    String xml = 
      "<?xml version=\"1.0\"?>" +
      // check that external entities are not resolved!
      "<!DOCTYPE foo [<!ENTITY bar SYSTEM \""+file+"\">]>" +
      "<add>" +
      "  &bar;" +
      "  <doc>" +
      "    <field name=\"id\">12345</field>" +
      "    <field name=\"name\">kitten</field>" +
      "  </doc>" +
      "</add>";
    SolrQueryRequest req = req();
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    XMLLoader loader = new XMLLoader().init(null);
    loader.load(req, rsp, new ContentStreamBase.StringStream(xml), p);

    AddUpdateCommand add = p.addCommands.get(0);
    assertEquals("12345", add.solrDoc.getField("id").getFirstValue());
    req.close();
  }

  public void testNamedEntity() throws Exception {
    assertU("<?xml version=\"1.0\" ?>\n"+
            "<!DOCTYPE add [\n<!ENTITY wacky \"zzz\" >\n]>"+
            "<add><doc>"+
            "<field name=\"id\">1</field>"+
            "<field name=\"foo_s\">&wacky;</field>" + 
            "</doc></add>");
    
    assertU("<commit/>");
    assertQ(req("foo_s:zzz"),
            "//*[@numFound='1']"
            );
  }
  
  @Test
  public void testReadDelete() throws Exception {
      String xml =
        "<update>" +
        " <delete>" +
        "   <query>id:150</query>" +
        "   <id>150</id>" +
        "   <id>200</id>" +
        "   <query>id:200</query>" +
        " </delete>" +
        " <delete commitWithin=\"500\">" +
        "   <query>id:150</query>" +
        " </delete>" +
        " <delete>" +
        "   <id>150</id>" +
        " </delete>" +
        " <delete>" +
        "   <id version=\"42\">300</id>" +
        " </delete>" +
        " <delete>" +
        "   <id _route_=\"shard1\">400</id>" +
        " </delete>" +
        " <delete>" +
        "   <id _route_=\"shard1\" version=\"42\">500</id>" +
        " </delete>" +
        "</update>";

      MockUpdateRequestProcessor p = new MockUpdateRequestProcessor(null);
      p.expectDelete(null, "id:150", -1, 0, null);
      p.expectDelete("150", null, -1, 0, null);
      p.expectDelete("200", null, -1, 0, null);
      p.expectDelete(null, "id:200", -1, 0, null);
      p.expectDelete(null, "id:150", 500, 0, null);
      p.expectDelete("150", null, -1, 0, null);
      p.expectDelete("300", null, -1, 42, null);
      p.expectDelete("400", null, -1, 0, "shard1");
      p.expectDelete("500", null, -1, 42, "shard1");

      XMLLoader loader = new XMLLoader().init(null);
      loader.load(req(), new SolrQueryResponse(), new ContentStreamBase.StringStream(xml), p);

      p.assertNoCommandsPending();
    }

    private static class MockUpdateRequestProcessor extends UpdateRequestProcessor {

      private Queue<DeleteUpdateCommand> deleteCommands = new LinkedList<>();

      public MockUpdateRequestProcessor(UpdateRequestProcessor next) {
        super(next);
      }

      public void expectDelete(String id, String query, int commitWithin, long version, String route) {
        DeleteUpdateCommand cmd = new DeleteUpdateCommand(null);
        cmd.id = id;
        cmd.query = query;
        cmd.commitWithin = commitWithin;
        if (version!=0)
          cmd.setVersion(version);
        if (route!=null)
          cmd.setRoute(route);
        deleteCommands.add(cmd);
      }

      public void assertNoCommandsPending() {
        assertTrue(deleteCommands.isEmpty());
      }

      @Override
      public void processDelete(DeleteUpdateCommand cmd) throws IOException {
        DeleteUpdateCommand expected = deleteCommands.poll();
        assertNotNull("Unexpected delete command: [" + cmd + "]", expected);
        assertTrue("Expected [" + expected + "] but found [" + cmd + "]",
            Objects.equals(expected.id, cmd.id) &&
            Objects.equals(expected.query, cmd.query) &&
            expected.commitWithin==cmd.commitWithin && 
            Objects.equals(expected.getRoute(), cmd.getRoute()));
      }
    }

}
