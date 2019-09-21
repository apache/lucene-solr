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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.loader.JsonLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.BufferingRequestProcessor;
import org.junit.BeforeClass;
import org.junit.Test;

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
      "    'array': [ 'aaa', 'bbb' ]\n" +
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
    assertEquals("SolrInputDocument(fields: [bool=true, f0=v0, array=[aaa, bbb]])", add.solrDoc.toString());

    // 
    add = p.addCommands.get(1);
    assertEquals("SolrInputDocument(fields: [f1=[v1, v2], f2=null])", add.solrDoc.toString());
    assertFalse(add.overwrite);

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
    assertNull( delete.query );
    assertEquals( delete.commitWithin, -1);
    
    delete = p.deleteCommands.get( 1 );
    assertEquals( delete.id, "ID" );
    assertNull( delete.query );
    assertEquals( delete.commitWithin, 500);
    
    delete = p.deleteCommands.get( 2 );
    assertNull( delete.id );
    assertEquals( delete.query, "QUERY" );
    assertEquals( delete.commitWithin, -1);
    
    delete = p.deleteCommands.get( 3 );
    assertNull( delete.id );
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
    assertFalse(add.overwrite);

    add = p.addCommands.get(1);
    d = add.solrDoc;
    f = d.getField( "id" );
    assertEquals("2", f.getValue());
    assertEquals(add.commitWithin, 100);
    assertFalse(add.overwrite);

    req.close();
  }

  @Test
  public void testInvalidJsonProducesBadRequestSolrException() throws Exception {
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    String invalidJsonString = "}{";

    SolrException ex = expectThrows(SolrException.class, () -> {
      loader.load(req(), rsp, new ContentStreamBase.StringStream(invalidJsonString), p);
    });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("Cannot parse"));
    assertTrue(ex.getMessage().contains("JSON"));
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
    assertTrue(add.overwrite);

    add = p.addCommands.get(1);
    d = add.solrDoc;
    f = d.getField( "id" );
    assertEquals("2", f.getValue());
    assertEquals(add.commitWithin, -1);
    assertTrue(add.overwrite);

    req.close();
  }

  public void testFieldValueOrdering() throws Exception {
    final String pre = "{'add':[{'id':'1',";
    final String post = "},{'id':'2'}]}";

    // list
    checkFieldValueOrdering((pre+ "'f':[45,67,89]" +post)
                            .replace('\'', '"')
    );
    // dup fieldname keys
    checkFieldValueOrdering((pre+ "'f':45,'f':67,'f':89" +post)
                            .replace('\'', '"')
    );
  }
  private void checkFieldValueOrdering(String rawJson) throws Exception {
    SolrQueryRequest req = req();
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(rawJson), p);
    assertEquals( 2, p.addCommands.size() );

    SolrInputDocument d = p.addCommands.get(0).solrDoc;
    assertEquals(2, d.getFieldNames().size());
    assertEquals("1", d.getFieldValue("id"));
    assertArrayEquals(new Object[] {45L, 67L, 89L} , d.getFieldValues("f").toArray());

    d = p.addCommands.get(1).solrDoc;
    assertEquals(1, d.getFieldNames().size());
    assertEquals("2", d.getFieldValue("id"));

    req.close();
  }

  public void testMultipleDocsWithoutArray() throws Exception {
    String doc = "\n" +
        "\n" +
        "{\"f1\": 1111 }\n" +
        "\n" +
        "{\"f1\": 2222 }\n";
    SolrQueryRequest req = req("srcField","_src_");
    req.getContext().put("path","/update/json/docs");
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(doc), p);
    assertEquals( 2, p.addCommands.size() );
  }

  public void testJsonDocFormat() throws Exception{
    String doc;
    SolrQueryRequest req;
    SolrQueryResponse rsp;
    BufferingRequestProcessor p;
    JsonLoader loader;

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
    req = req("srcField","_src_");
    req.getContext().put("path","/update/json/docs");
    rsp = new SolrQueryResponse();
    p = new BufferingRequestProcessor(null);
    loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(doc), p);

    assertEquals( 2, p.addCommands.size() );

    String content = (String) p.addCommands.get(0).solrDoc.getFieldValue("_src_");
    assertNotNull(content);
    Map obj = (Map) Utils.fromJSONString(content);
    assertEquals(Boolean.TRUE, obj.get("bool"));
    assertEquals("v0", obj.get("f0"));
    assertNotNull(obj.get("f0"));
    assertNotNull(obj.get("array"));
    assertNotNull(obj.get("boosted"));

    content = (String) p.addCommands.get(1).solrDoc.getFieldValue("_src_");
    assertNotNull(content);
    obj = (Map) Utils.fromJSONString(content);
    assertEquals("v1", obj.get("f1"));
    assertEquals("v2", obj.get("f2"));
    assertTrue(obj.containsKey("f3"));

    //TODO new test method
    doc = "[{'id':'1'},{'id':'2'}]".replace('\'', '"');
    req = req("srcField","_src_");
    req.getContext().put("path","/update/json/docs");
    rsp = new SolrQueryResponse();
    p = new BufferingRequestProcessor(null);
    loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(doc), p);
    assertEquals( 2, p.addCommands.size() );

    content = (String) p.addCommands.get(0).solrDoc.getFieldValue("_src_");
    assertNotNull(content);
    obj = (Map) Utils.fromJSONString(content);
    assertEquals("1", obj.get("id"));
    content = (String) p.addCommands.get(1).solrDoc.getFieldValue("_src_");
    assertNotNull(content);
    obj = (Map) Utils.fromJSONString(content);
    assertEquals("2", obj.get("id"));

    //TODO new test method
    String json = "{a:{" +
        "b:[{c:c1, e:e1},{c:c2, e :e2, d:{p:q}}]," +
        "x:y" +
        "}}";
    req = req("split", "/|/a/b"   );
    req.getContext().put("path","/update/json/docs");
    rsp = new SolrQueryResponse();
    p = new BufferingRequestProcessor(null);
    loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(json), p);
    assertEquals( 1, p.addCommands.size() );
    assertEquals("SolrInputDocument(fields: [" +
        "b=[" +
          "SolrInputDocument(fields: [c=c1, e=e1]), " +
          "SolrInputDocument(fields: [c=c2, e=e2, d.p=q])], " +
        "a.x=y" +
        "])",  p.addCommands.get(0).solrDoc.toString());
  }

  private static final String PARENT_TWO_CHILDREN_JSON = "{\n" +
      "  \"id\": \"1\",\n" +
      "  \"name\": \"i am the parent\",\n" +
      "  \"cat\": \"parent\",\n" +
      "  \"children\": [\n" +
      "    {\n" +
      "      \"id\": \"1.1\",\n" +
      "      \"name\": \"i am the 1st child\",\n" +
      "      \"cat\": \"child\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"id\": \"1.2\",\n" +
      "      \"name\": \"i am the 2nd child\",\n" +
      "      \"cat\": \"child\",\n" +
      "      \"test_s\": \"test-new-label\",\n" +
      "      \"grandchildren\": [\n" +
      "        {\n" +
      "          \"id\": \"1.2.1\",\n" +
      "          \"name\": \"i am the grandchild\",\n" +
      "          \"cat\": \"grandchild\"\n" +
      "        }\n" +
      "      ]\n" +
      "    }\n" +
      "  ]\n" +
      "}";
  
  private static final String[] PARENT_TWO_CHILDREN_PARAMS = new String[] {    "split", "/|/children|/children/grandchildren",
      "f","$FQN:/**",
      "f", "id:/children/id",
      "f", "/name",
      "f", "/children/name",
      "f", "cat:/children/cat",
      "f", "id:/children/grandchildren/id",
      "f", "name:/children/grandchildren/name",
      "f", "cat:/children/grandchildren/cat"};

  @Test
  public void testFewParentsJsonDoc() throws Exception {
    String json = PARENT_TWO_CHILDREN_JSON;

    SolrQueryRequest req;
    SolrQueryResponse rsp;
    BufferingRequestProcessor p;
    JsonLoader loader;//multichild test case
    final boolean array = random().nextBoolean();
    StringBuilder b = new StringBuilder();
    if (array) {
      b.append("[");
    }
    final int passes = atLeast(2);
    for (int i=1;i<=passes;i++){
      b.append(json.replace("1",""+i));
      if (array) {
        b.append(i<passes ? "," :"]");
      }
    }

    req = req(PARENT_TWO_CHILDREN_PARAMS);
    req.getContext().put("path", "/update/json/docs");
    rsp = new SolrQueryResponse();
    p = new BufferingRequestProcessor(null);
    loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(b.toString()), p);
    for (int i=1; i<=passes; i++){
      final int ii = i;
      UnaryOperator<String> s = (v)-> v.replace("1",""+ii);
      final SolrInputDocument parent = p.addCommands.get(i-1).solrDoc;
      assertOnlyValue(s.apply("1"), parent,"id");
      assertOnlyValue("i am the parent", parent, "name");
      assertOnlyValue("parent", parent, "cat");

      List<SolrInputDocument> childDocs1 = (List) ((parent.getField("children")).getValue());

      assertEquals(2, childDocs1.size());
      {
        final SolrInputDocument child1 = childDocs1.get(0);
        assertOnlyValue(s.apply("1.1"), child1, "id");
        assertOnlyValue(s.apply("i am the 1st child"), child1, "name");
        assertOnlyValue("child", child1,"cat");
      }
      {
        final SolrInputDocument child2 = childDocs1.get(1);
        assertOnlyValue(s.apply("1.2"), child2, "id");
        assertOnlyValue("i am the 2nd child", child2, "name");
        assertOnlyValue("test-new-label", child2, "test_s");
        assertOnlyValue("child", child2, "cat");

        List<SolrInputDocument> childDocs2 = (List) ((child2.getField("grandchildren")).getValue());

        assertEquals(1, childDocs2.size());
        final SolrInputDocument grandChild = childDocs2.get(0);
        assertOnlyValue(s.apply("1.2.1"), grandChild,"id");
        assertOnlyValue("i am the grandchild", grandChild, "name");
        assertOnlyValue("grandchild", grandChild, "cat");
      }
    }
  }

  private static void assertOnlyValue(String expected, SolrInputDocument doc, String field) {
    assertEquals(Collections.singletonList(expected), doc.getFieldValues(field));
  }

  public void testAtomicUpdateFieldValue() throws Exception {
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
    assertEquals("SolrInputDocument(fields: [id=1, val_s={add=foo}])", add.solrDoc.toString());

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

    SolrException ex = expectThrows(SolrException.class, () -> {
      updateJ(json( "[{'id':'1','big_integer_tl':12345678901234567890}]" ), null);
    });
    assertTrue(ex.getCause() instanceof NumberFormatException);

    // Adding a BigInteger to an integer field should fail
    // BigInteger.intValue() returns only the low-order 32 bits.
    ex = expectThrows(SolrException.class, () -> {
      updateJ(json( "[{'id':'1','big_integer_ti':12345678901234567890}]" ), null);
    });
    assertTrue(ex.getCause() instanceof NumberFormatException);

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
        +"\n ,'delete':{'id':90, '_route_':'shard1', '_version_':88888}"
        + "\n}\n";
    str = str.replace('\'', '"');
    SolrQueryRequest req = req();
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    JsonLoader loader = new JsonLoader();
    loader.load(req, rsp, new ContentStreamBase.StringStream(str), p);

    // DELETE COMMANDS
    assertEquals( 9, p.deleteCommands.size() );
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
    assertEquals(delete.getVersion(), 88888L);

    delete = p.deleteCommands.get(8);
    assertEquals(delete.id, "90");
    assertEquals(delete.query, null);
    assertEquals(delete.getRoute(), "shard1");
    assertEquals(delete.getVersion(), 88888L);

    req.close();
  }

  private static final String SIMPLE_ANON_CHILD_DOCS_JSON = "{\n" +
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

  @Test
  public void testSimpleAnonymousChildDocs() throws Exception {
    checkTwoAnonymousChildDocs(SIMPLE_ANON_CHILD_DOCS_JSON, true);
  }

  @Test
  public void testSimpleChildDocs() throws Exception {
    checkTwoAnonymousChildDocs(SIMPLE_ANON_CHILD_DOCS_JSON, false);
  }

  private static final String DUP_KEYS_ANON_CHILD_DOCS_JSON = "{\n" +
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

  @Test
  public void testDupKeysAnonymousChildDocs() throws Exception {
    checkTwoAnonymousChildDocs(DUP_KEYS_ANON_CHILD_DOCS_JSON, true);
  }

  @Test
  public void testDupKeysChildDocs() throws Exception {
    checkTwoAnonymousChildDocs(DUP_KEYS_ANON_CHILD_DOCS_JSON, false);
  }

  // rawJsonStr has "_childDocuments_" key.  if anonChildDocs then we want to test with something else.
  private void checkTwoAnonymousChildDocs(String rawJsonStr, boolean anonChildDocs) throws Exception {
    if (!anonChildDocs) {
      rawJsonStr = rawJsonStr.replaceAll("_childDocuments_", "childLabel");
    }
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

    SolrInputDocument cd;
    if (anonChildDocs) {
      cd = d.getChildDocuments().get(0);
    } else {
      cd = (SolrInputDocument) (d.getField("childLabel")).getFirstValue();
    }
    SolrInputField cf = cd.getField( "id" );
    assertEquals("2", cf.getValue());

    if (anonChildDocs) {
      cd = d.getChildDocuments().get(1);
    } else {
      cd = (SolrInputDocument)((List)(d.getField("childLabel")).getValue()).get(1);
    }
    cf = cd.getField( "id" );
    assertEquals("3", cf.getValue());
    cf = cd.getField( "foo_i" );
    assertEquals(2, cf.getValueCount());

    assertEquals(new Object[] {666L,777L}, cf.getValues().toArray());

    req.close();
  }

  @Test
  public void testEmptyAnonymousChildDocs() throws Exception {
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
  public void testAnonymousGrandChildDocs() throws Exception {
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

  @Test
  public void testChildDocs() throws Exception {
    String str = "{\n" +
        "    \"add\": {\n" +
        "        \"doc\": {\n" +
        "            \"id\": \"1\",\n" +
        "            \"children\": [\n" +
        "                {\n" +
        "                    \"id\": \"2\",\n" +
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

    List<SolrInputDocument> children = (List) one.getFieldValues("children");
    SolrInputDocument two = children.get(0);
    assertEquals("2", two.getFieldValue("id"));
    assertEquals("Yaz", two.getFieldValue("foo_s"));

    SolrInputDocument three = children.get(1);
    assertEquals("3", three.getFieldValue("id"));
    assertEquals("Bar", three.getFieldValue("foo_s"));

    req.close();

  }

  @Test
  public void testSingleRelationalChildDoc() throws Exception {
    String str = "{\n" +
        "    \"add\": {\n" +
        "        \"doc\": {\n" +
        "            \"id\": \"1\",\n" +
        "            \"child1\": \n" +
        "                {\n" +
        "                    \"id\": \"2\",\n" +
        "                    \"foo_s\": \"Yaz\"\n" +
        "                },\n" +
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

    assertTrue(one.keySet().contains("child1"));

    SolrInputDocument two = (SolrInputDocument) one.getField("child1").getValue();
    assertEquals("2", two.getFieldValue("id"));
    assertEquals("Yaz", two.getFieldValue("foo_s"));

    req.close();

  }

}
