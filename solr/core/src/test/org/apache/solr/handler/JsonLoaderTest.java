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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.handler.loader.JsonLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.BufferingRequestProcessor;
import org.junit.BeforeClass;
import org.junit.Test;
import org.noggit.ObjectBuilder;
import org.xml.sax.SAXException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class JsonLoaderTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }
  
  static String input = json("{\n" +
      "\n" +
      "'add': {\n" +
      "  'doc': {\n" +
      "    'bool': true,\n" +
      "    'f0': 'v0',\n" +
      "    'f2': {\n" +
      "      'boost': 2.3,\n" +
      "      'value': 'test'\n" +
      "    },\n" +
      "    'array': [ 'aaa', 'bbb' ],\n" +
      "    'boosted': {\n" +
      "      'boost': 6.7,\n" +
      "      'value': [ 'aaa', 'bbb' ]\n" +
      "    }\n" +
      "  }\n" +
      "},\n" +
      "'add': {\n" +
      "  'commitWithin': 1234,\n" +
      "  'overwrite': false,\n" +
      "  'boost': 3.45,\n" +
      "  'doc': {\n" +
      "    'f1': 'v1',\n" +
      "    'f1': 'v2',\n" +
      "    'f2': null\n" +
      "  }\n" +
      "},\n" +
      "\n" +
      "'commit': {},\n" +
      "'optimize': { 'waitSearcher':false, 'openSearcher':false },\n" +
      "\n" +
      "'delete': { 'id':'ID' },\n" +
      "'delete': { 'id':'ID', 'commitWithin':500 },\n" +
      "'delete': { 'query':'QUERY' },\n" +
      "'delete': { 'query':'QUERY', 'commitWithin':500 },\n" +
      "'rollback': {}\n" +
      "\n" +
      "}\n" +
      "");


  public void testParsing() throws Exception
  {
    SolrQueryRequest req = req();
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(input), p);

    assertEquals( 2, p.addCommands.size() );
    
    AddUpdateCommand add = p.addCommands.get(0);
    SolrInputDocument d = add.solrDoc;
    SolrInputField f = d.getField( "boosted" );
    assertEquals(6.7f, f.getBoost(), 0.1);
    assertEquals(2, f.getValues().size());

    // 
    add = p.addCommands.get(1);
    d = add.solrDoc;
    f = d.getField( "f1" );
    assertEquals(2, f.getValues().size());
    assertEquals(3.45f, d.getDocumentBoost(), 0.001);
    assertEquals(false, add.overwrite);

    assertEquals(0, d.getField("f2").getValueCount());

    // parse the commit commands
    assertEquals( 2, p.commitCommands.size() );
    CommitUpdateCommand commit = p.commitCommands.get( 0 );
    assertFalse( commit.optimize );
    assertTrue( commit.waitSearcher );
    assertTrue( commit.openSearcher );

    commit = p.commitCommands.get( 1 );
    assertTrue( commit.optimize );
    assertFalse( commit.waitSearcher );
    assertFalse( commit.openSearcher );


    // DELETE COMMANDS
    assertEquals( 4, p.deleteCommands.size() );
    DeleteUpdateCommand delete = p.deleteCommands.get( 0 );
    assertEquals( delete.id, "ID" );
    assertEquals( delete.query, null );
    assertEquals( delete.commitWithin, -1);
    
    delete = p.deleteCommands.get( 1 );
    assertEquals( delete.id, "ID" );
    assertEquals( delete.query, null );
    assertEquals( delete.commitWithin, 500);
    
    delete = p.deleteCommands.get( 2 );
    assertEquals( delete.id, null );
    assertEquals( delete.query, "QUERY" );
    assertEquals( delete.commitWithin, -1);
    
    delete = p.deleteCommands.get( 3 );
    assertEquals( delete.id, null );
    assertEquals( delete.query, "QUERY" );
    assertEquals( delete.commitWithin, 500);

    // ROLLBACK COMMANDS
    assertEquals( 1, p.rollbackCommands.size() );

    req.close();
  }


  public void testSimpleFormat() throws Exception
  {
    String str = "[{'id':'1'},{'id':'2'}]".replace('\'', '"');
    SolrQueryRequest req = req("commitWithin","100", "overwrite","false");
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(str), p);

    assertEquals( 2, p.addCommands.size() );

    AddUpdateCommand add = p.addCommands.get(0);
    SolrInputDocument d = add.solrDoc;
    SolrInputField f = d.getField( "id" );
    assertEquals("1", f.getValue());
    assertEquals(add.commitWithin, 100);
    assertEquals(add.overwrite, false);

    add = p.addCommands.get(1);
    d = add.solrDoc;
    f = d.getField( "id" );
    assertEquals("2", f.getValue());
    assertEquals(add.commitWithin, 100);
    assertEquals(add.overwrite, false);

    req.close();
  }

  public void testSimpleFormatInAdd() throws Exception
  {
    String str = "{'add':[{'id':'1'},{'id':'2'}]}".replace('\'', '"');
    SolrQueryRequest req = req();
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(str), p);

    assertEquals( 2, p.addCommands.size() );

    AddUpdateCommand add = p.addCommands.get(0);
    SolrInputDocument d = add.solrDoc;
    SolrInputField f = d.getField( "id" );
    assertEquals("1", f.getValue());
    assertEquals(add.commitWithin, -1);
    assertEquals(add.overwrite, true);

    add = p.addCommands.get(1);
    d = add.solrDoc;
    f = d.getField( "id" );
    assertEquals("2", f.getValue());
    assertEquals(add.commitWithin, -1);
    assertEquals(add.overwrite, true);

    req.close();
  }

  public void testFieldValueOrdering() throws Exception {
    final String pre = "{'add':[{'id':'1',";
    final String post = "},{'id':'2'}]}";

    // list
    checkFieldValueOrdering((pre+ "'f':[45,67,89]" +post)
                            .replace('\'', '"'),
                            1.0F);
    // dup fieldname keys
    checkFieldValueOrdering((pre+ "'f':45,'f':67,'f':89" +post)
                            .replace('\'', '"'),
                            1.0F);
    // extended w/boost
    checkFieldValueOrdering((pre+ "'f':{'boost':4.0,'value':[45,67,89]}" +post)
                            .replace('\'', '"'),
                            4.0F);
    // dup keys extended w/ multiplicitive boost
    checkFieldValueOrdering((pre+ 
                             "'f':{'boost':2.0,'value':[45,67]}," +
                             "'f':{'boost':2.0,'value':89}" 
                             +post)
                            .replace('\'', '"'),
                            4.0F);

  }
  private void checkFieldValueOrdering(String rawJson, float fBoost) throws Exception {
    SolrQueryRequest req = req();
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(rawJson), p);
    assertEquals( 2, p.addCommands.size() );

    SolrInputDocument d = p.addCommands.get(0).solrDoc;
    assertEquals(2, d.getFieldNames().size());
    assertEquals("1", d.getFieldValue("id"));
    assertEquals(new Object[] {45L, 67L, 89L} , d.getFieldValues("f").toArray());
    assertEquals(0.0F, fBoost, d.getField("f").getBoost());

    d = p.addCommands.get(1).solrDoc;
    assertEquals(1, d.getFieldNames().size());
    assertEquals("2", d.getFieldValue("id"));

    req.close();
  }

  public void testJsonDocFormat() throws Exception{
    String doc = "\n" +
        "\n" +
        "{\"bool\": true,\n" +
        " \"f0\": \"v0\",\n" +
        " \"f2\": {\n" +
        "    \t  \"boost\": 2.3,\n" +
        "    \t  \"value\": \"test\"\n" +
        "    \t   },\n" +
        "\"array\": [ \"aaa\", \"bbb\" ],\n" +
        "\"boosted\": {\n" +
        "    \t      \"boost\": 6.7,\n" +
        "    \t      \"value\": [ \"aaa\", \"bbb\" ]\n" +
        "    \t    }\n" +
        " }\n" +
        "\n" +
        "\n" +
        " {\"f1\": \"v1\",\n" +
        "  \"f1\": \"v2\",\n" +
        "   \"f2\": null\n" +
        "  }\n";
    SolrQueryRequest req = req("srcField","_src");
    req.getContext().put("path","/update/json/docs");
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(doc), p);
    assertEquals( 2, p.addCommands.size() );
     doc = "\n" +
        "\n" +
        "{\"bool\": true,\n" +
        " \"f0\": \"v0\",\n" +
        " \"f2\": {\n" +
        "    \t  \"boost\": 2.3,\n" +
        "    \t  \"value\": \"test\"\n" +
        "    \t   },\n" +
        "\"array\": [ \"aaa\", \"bbb\" ],\n" +
        "\"boosted\": {\n" +
        "    \t      \"boost\": 6.7,\n" +
        "    \t      \"value\": [ \"aaa\", \"bbb\" ]\n" +
        "    \t    }\n" +
        " }\n" +
        "\n" +
        "\n" +
        " {\"f1\": \"v1\",\n" +
        "  \"f2\": \"v2\",\n" +
        "   \"f3\": null\n" +
        "  }\n";
    req = req("srcField","_src");
    req.getContext().put("path","/update/json/docs");
    rsp = new SolrQueryResponse();
    p = new BufferingRequestProcessor(null);
    loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(doc), p);

    assertEquals( 2, p.addCommands.size() );

    String content = (String) p.addCommands.get(0).solrDoc.getFieldValue("_src");
    assertNotNull(content);
    Map obj = (Map) ObjectBuilder.fromJSON(content);
    assertEquals(Boolean.TRUE, obj.get("bool"));
    assertEquals("v0", obj.get("f0"));
    assertNotNull(obj.get("f0"));
    assertNotNull(obj.get("array"));
    assertNotNull(obj.get("boosted"));

    content = (String) p.addCommands.get(1).solrDoc.getFieldValue("_src");
    assertNotNull(content);
    obj = (Map) ObjectBuilder.fromJSON(content);
    assertEquals("v1", obj.get("f1"));
    assertEquals("v2", obj.get("f2"));
    assertTrue(obj.containsKey("f3"));

    doc = "[{'id':'1'},{'id':'2'}]".replace('\'', '"');
    req = req("srcField","_src");
    req.getContext().put("path","/update/json/docs");
    rsp = new SolrQueryResponse();
    p = new BufferingRequestProcessor(null);
    loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(doc), p);
    assertEquals( 2, p.addCommands.size() );

    content = (String) p.addCommands.get(0).solrDoc.getFieldValue("_src");
    assertNotNull(content);
    obj = (Map) ObjectBuilder.fromJSON(content);
    assertEquals("1", obj.get("id"));
    content = (String) p.addCommands.get(1).solrDoc.getFieldValue("_src");
    assertNotNull(content);
    obj = (Map) ObjectBuilder.fromJSON(content);
    assertEquals("2", obj.get("id"));


  }



  public void testExtendedFieldValues() throws Exception {
    String str = "[{'id':'1', 'val_s':{'add':'foo'}}]".replace('\'', '"');
    SolrQueryRequest req = req();
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(str), p);

    assertEquals( 1, p.addCommands.size() );

    AddUpdateCommand add = p.addCommands.get(0);
    assertEquals(add.commitWithin, -1);
    assertEquals(add.overwrite, true);
    SolrInputDocument d = add.solrDoc;

    SolrInputField f = d.getField( "id" );
    assertEquals("1", f.getValue());

    f = d.getField( "val_s" );
    Map<String,Object> map = (Map<String,Object>)f.getValue();
    assertEquals("foo",map.get("add"));

    req.close();
  }

  @Test
  public void testNullValues() throws Exception {
    updateJ( json( "[{'id':'10','foo_s':null,'foo2_s':['hi',null,'there']}]" ), params("commit","true"));
    assertJQ(req("q","id:10", "fl","foo_s,foo2_s")
        ,"/response/docs/[0]=={'foo2_s':['hi','there']}"
    );
  }
  
  @Test
  public void testBooleanValuesInAdd() throws Exception {
    String str = "{'add':[{'id':'1','b1':true,'b2':false,'b3':[false,true]}]}".replace('\'', '"');
    SolrQueryRequest req = req();
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(str), p);

    assertEquals(1, p.addCommands.size());

    AddUpdateCommand add = p.addCommands.get(0);
    SolrInputDocument d = add.solrDoc;
    SolrInputField f = d.getField("b1");
    assertEquals(Boolean.TRUE, f.getValue());
    f = d.getField("b2");
    assertEquals(Boolean.FALSE, f.getValue());
    f = d.getField("b3");
    assertEquals(2, ((List)f.getValue()).size());
    assertEquals(Boolean.FALSE, ((List)f.getValue()).get(0));
    assertEquals(Boolean.TRUE, ((List)f.getValue()).get(1));

    req.close();
  }

  @Test
  public void testIntegerValuesInAdd() throws Exception {
    String str = "{'add':[{'id':'1','i1':256,'i2':-5123456789,'i3':[0,1]}]}".replace('\'', '"');
    SolrQueryRequest req = req();
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(str), p);

    assertEquals(1, p.addCommands.size());

    AddUpdateCommand add = p.addCommands.get(0);
    SolrInputDocument d = add.solrDoc;
    SolrInputField f = d.getField("i1");
    assertEquals(256L, f.getValue());
    f = d.getField("i2");
    assertEquals(-5123456789L, f.getValue());
    f = d.getField("i3");
    assertEquals(2, ((List)f.getValue()).size());
    assertEquals(0L, ((List)f.getValue()).get(0));
    assertEquals(1L, ((List)f.getValue()).get(1));

    req.close();
  }


  @Test
  public void testDecimalValuesInAdd() throws Exception {
    String str = "{'add':[{'id':'1','d1':256.78,'d2':-5123456789.0,'d3':0.0,'d3':1.0,'d4':1.7E-10}]}".replace('\'', '"');
    SolrQueryRequest req = req();
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(str), p);

    assertEquals(1, p.addCommands.size());

    AddUpdateCommand add = p.addCommands.get(0);
    SolrInputDocument d = add.solrDoc;
    SolrInputField f = d.getField("d1");
    assertEquals(256.78, f.getValue());
    f = d.getField("d2");
    assertEquals(-5123456789.0, f.getValue());
    f = d.getField("d3");
    assertEquals(2, ((List)f.getValue()).size());
    assertTrue(((List)f.getValue()).contains(0.0));
    assertTrue(((List) f.getValue()).contains(1.0));
    f = d.getField("d4");
    assertEquals(1.7E-10, f.getValue());

    req.close();
  }

  @Test
  public void testBigDecimalValuesInAdd() throws Exception {
    String str = ("{'add':[{'id':'1','bd1':0.12345678901234567890123456789012345,"
                 + "'bd2':12345678901234567890.12345678901234567890,'bd3':0.012345678901234567890123456789012345,"
                 + "'bd3':123456789012345678900.012345678901234567890}]}").replace('\'', '"');
    SolrQueryRequest req = req();
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(str), p);

    assertEquals(1, p.addCommands.size());

    AddUpdateCommand add = p.addCommands.get(0);
    SolrInputDocument d = add.solrDoc;
    SolrInputField f = d.getField("bd1");                        
    assertTrue(f.getValue() instanceof String);
    assertEquals("0.12345678901234567890123456789012345", f.getValue());
    f = d.getField("bd2");
    assertTrue(f.getValue() instanceof String);
    assertEquals("12345678901234567890.12345678901234567890", f.getValue());
    f = d.getField("bd3");
    assertEquals(2, ((List)f.getValue()).size());
    assertTrue(((List)f.getValue()).contains("0.012345678901234567890123456789012345"));
    assertTrue(((List)f.getValue()).contains("123456789012345678900.012345678901234567890"));

    req.close();
  }

  @Test
  public void testBigIntegerValuesInAdd() throws Exception {
    String str = ("{'add':[{'id':'1','bi1':123456789012345678901,'bi2':1098765432109876543210,"
                 + "'bi3':[1234567890123456789012,10987654321098765432109]}]}").replace('\'', '"');
    SolrQueryRequest req = req();
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(str), p);

    assertEquals(1, p.addCommands.size());

    AddUpdateCommand add = p.addCommands.get(0);
    SolrInputDocument d = add.solrDoc;
    SolrInputField f = d.getField("bi1");
    assertTrue(f.getValue() instanceof String);
    assertEquals("123456789012345678901", f.getValue());
    f = d.getField("bi2");
    assertTrue(f.getValue() instanceof String);
    assertEquals("1098765432109876543210", f.getValue());
    f = d.getField("bi3");
    assertEquals(2, ((List)f.getValue()).size());
    assertTrue(((List)f.getValue()).contains("1234567890123456789012"));
    assertTrue(((List)f.getValue()).contains("10987654321098765432109"));

    req.close();
  }


  @Test
  public void testAddNonStringValues() throws Exception {
    // BigInteger and BigDecimal should be typed as strings, since there is no direct support for them
    updateJ(json( "[{'id':'1','boolean_b':false,'long_l':19,'double_d':18.6,'big_integer_s':12345678901234567890,"
        +"      'big_decimal_s':0.1234567890123456789012345}]" ), params("commit","true"));
    assertJQ(req("q","id:1", "fl","boolean_b,long_l,double_d,big_integer_s,big_decimal_s")
        ,"/response/docs/[0]=={'boolean_b':[false],'long_l':[19],'double_d':[18.6],"
                             +"'big_integer_s':['12345678901234567890'],"
                             +"'big_decimal_s':['0.1234567890123456789012345']}]}"
    );
  }


  @Test
  public void testAddBigIntegerValueToTrieField() throws Exception {
    // Adding a BigInteger to a long field should fail
    // BigInteger.longValue() returns only the low-order 64 bits.

    ignoreException("big_integer_t");

    try {
      updateJ(json( "[{'id':'1','big_integer_tl':12345678901234567890}]" ), null);
      fail("A BigInteger value should overflow a long field");
    } catch (SolrException e) {
      if ( ! (e.getCause() instanceof NumberFormatException)) {
        throw e;
      }
    }

    // Adding a BigInteger to an integer field should fail
    // BigInteger.intValue() returns only the low-order 32 bits.
    try {
      updateJ(json( "[{'id':'1','big_integer_ti':12345678901234567890}]" ), null);
      fail("A BigInteger value should overflow an integer field");
    } catch (SolrException e) {
      if ( ! (e.getCause() instanceof NumberFormatException)) {
        throw e;
      }
    }

    unIgnoreException("big_integer_t");
  }

  @Test
  public void testAddBigDecimalValueToTrieField() throws Exception {
    // Adding a BigDecimal to a double field should succeed by reducing precision
    updateJ(json( "[{'id':'1','big_decimal_td':100000000000000000000000000001234567890.0987654321}]" ),
            params("commit", "true"));
    assertJQ(req("q","id:1", "fl","big_decimal_td"), 
             "/response/docs/[0]=={'big_decimal_td':[1.0E38]}"
    );

    // Adding a BigDecimal to a float field should succeed by reducing precision
    updateJ(json( "[{'id':'2','big_decimal_tf':100000000000000000000000000001234567890.0987654321}]" ),
            params("commit", "true"));
    assertJQ(req("q","id:2", "fl","big_decimal_tf"),
             "/response/docs/[0]=={'big_decimal_tf':[1.0E38]}"
    );
  }

  // The delete syntax was both extended for simplification in 4.0
  @Test
  public void testDeleteSyntax() throws Exception {
    String str = "{'delete':10"
        +"\n ,'delete':'20'"
        +"\n ,'delete':['30','40']"
        +"\n ,'delete':{'id':50, '_version_':12345}"
        +"\n ,'delete':[{'id':60, '_version_':67890}, {'id':70, '_version_':77777}, {'query':'id:80', '_version_':88888}]"
        + "\n}\n";
    str = str.replace('\'', '"');
    SolrQueryRequest req = req();
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(str), p);

    // DELETE COMMANDS
    assertEquals( 8, p.deleteCommands.size() );
    DeleteUpdateCommand delete = p.deleteCommands.get( 0 );
    assertEquals( delete.id, "10" );
    assertEquals( delete.query, null );
    assertEquals( delete.commitWithin, -1);

    delete = p.deleteCommands.get( 1 );
    assertEquals( delete.id, "20" );
    assertEquals( delete.query, null );
    assertEquals( delete.commitWithin, -1);

    delete = p.deleteCommands.get( 2 );
    assertEquals( delete.id, "30" );
    assertEquals( delete.query, null );
    assertEquals( delete.commitWithin, -1);

    delete = p.deleteCommands.get( 3 );
    assertEquals( delete.id, "40" );
    assertEquals( delete.query, null );
    assertEquals( delete.commitWithin, -1);

    delete = p.deleteCommands.get( 4 );
    assertEquals( delete.id, "50" );
    assertEquals( delete.query, null );
    assertEquals( delete.getVersion(), 12345L);

    delete = p.deleteCommands.get( 5 );
    assertEquals( delete.id, "60" );
    assertEquals( delete.query, null );
    assertEquals( delete.getVersion(), 67890L);

    delete = p.deleteCommands.get( 6 );
    assertEquals( delete.id, "70" );
    assertEquals( delete.query, null );
    assertEquals( delete.getVersion(), 77777L);

    delete = p.deleteCommands.get( 7 );
    assertEquals( delete.id, null );
    assertEquals( delete.query, "id:80" );
    assertEquals( delete.getVersion(), 88888L);

    req.close();
  }

  @Test
  public void testSimpleChildDocs() throws Exception {
    String str = "{\n" +
        "    \"add\": {\n" +
        "        \"doc\": {\n" +
        "            \"id\": \"1\",\n" +
        "            \"_childDocuments_\": [\n" +
        "                {\n" +
        "                    \"id\": \"2\"\n" +
        "                },\n" +
        "                {\n" +
        "                    \"id\": \"3\",\n" +
        "                    \"foo_i\": [666,777]\n" +
        "                }\n" +
        "            ]\n" +
        "        }\n" +
        "    }\n" +
        "}";
    checkTwoChildDocs(str);
  }

  @Test
  public void testDupKeysChildDocs() throws Exception {
    String str = "{\n" +
        "    \"add\": {\n" +
        "        \"doc\": {\n" +
        "            \"_childDocuments_\": [\n" +
        "                {\n" +
        "                    \"id\": \"2\"\n" +
        "                }\n" +
        "            ],\n" +
        "            \"id\": \"1\",\n" +
        "            \"_childDocuments_\": [\n" +
        "                {\n" +
        "                    \"id\": \"3\",\n" +
        "                    \"foo_i\": 666,\n" +
        "                    \"foo_i\": 777\n" +
        "                }\n" +
        "            ]\n" +
        "        }\n" +
        "    }\n" +
        "}";
    checkTwoChildDocs(str);
  }

  private void checkTwoChildDocs(String rawJsonStr) throws Exception {
    SolrQueryRequest req = req("commit","true");
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(rawJsonStr), p);

    assertEquals( 1, p.addCommands.size() );

    AddUpdateCommand add = p.addCommands.get(0);
    SolrInputDocument d = add.solrDoc;
    SolrInputField f = d.getField( "id" );
    assertEquals("1", f.getValue());

    SolrInputDocument cd = d.getChildDocuments().get(0);
    SolrInputField cf = cd.getField( "id" );
    assertEquals("2", cf.getValue());

    cd = d.getChildDocuments().get(1);
    cf = cd.getField( "id" );
    assertEquals("3", cf.getValue());
    cf = cd.getField( "foo_i" );
    assertEquals(2, cf.getValueCount());

    assertEquals(new Object[] {666L,777L}, cf.getValues().toArray());

    req.close();
  }

  @Test
  public void testEmptyChildDocs() throws Exception {
    String str = "{\n" +
        "    \"add\": {\n" +
        "        \"doc\": {\n" +
        "            \"id\": \"1\",\n" +
        "            \"_childDocuments_\": []\n" +
        "        }\n" +
        "    }\n" +
        "}";
    SolrQueryRequest req = req("commit","true");
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(str), p);

    assertEquals( 1, p.addCommands.size() );

    AddUpdateCommand add = p.addCommands.get(0);
    SolrInputDocument d = add.solrDoc;
    SolrInputField f = d.getField( "id" );
    assertEquals("1", f.getValue());
    List<SolrInputDocument> cd = d.getChildDocuments();
    assertNull(cd);

    req.close();
  }

  @Test
  public void testGrandChildDocs() throws Exception {
    String str = "{\n" +
        "    \"add\": {\n" +
        "        \"doc\": {\n" +
        "            \"id\": \"1\",\n" +
        "            \"_childDocuments_\": [\n" +
        "                {\n" +
        "                    \"id\": \"2\",\n" +
        "                    \"_childDocuments_\": [\n" +
        "                        {\n" +
        "                           \"id\": \"4\",\n" +
        "                           \"foo_s\": \"Baz\"\n" +
        "                        }\n" +
        "                    ],\n" +
        "                    \"foo_s\": \"Yaz\"\n" +
        "                },\n" +
        "                {\n" +
        "                    \"id\": \"3\",\n" +
        "                    \"foo_s\": \"Bar\"\n" +
        "                }\n" +
        "            ]\n" +
        "        }\n" +
        "    }\n" +
        "}";

    SolrQueryRequest req = req("commit","true");
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(str), p);

    assertEquals( 1, p.addCommands.size() );

    AddUpdateCommand add = p.addCommands.get(0);
    SolrInputDocument one = add.solrDoc;
    assertEquals("1", one.getFieldValue("id"));

    SolrInputDocument two = one.getChildDocuments().get(0);
    assertEquals("2", two.getFieldValue("id"));
    assertEquals("Yaz", two.getFieldValue("foo_s"));

    SolrInputDocument four = two.getChildDocuments().get(0);
    assertEquals("4", four.getFieldValue("id"));
    assertEquals("Baz", four.getFieldValue("foo_s"));

    SolrInputDocument three = one.getChildDocuments().get(1);
    assertEquals("3", three.getFieldValue("id"));
    assertEquals("Bar", three.getFieldValue("foo_s"));

    req.close();

  }


}
