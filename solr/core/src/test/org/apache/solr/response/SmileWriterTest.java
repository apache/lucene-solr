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
package org.apache.solr.response;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrReturnFields;
import org.junit.BeforeClass;
import org.junit.Test;
import org.noggit.CharArr;
import org.noggit.JSONParser;
import org.noggit.JSONWriter;
import org.noggit.ObjectBuilder;

public class SmileWriterTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }

  @Test
  public void testTypes() throws IOException {
    SolrQueryRequest req = req("dummy");
    SolrQueryResponse rsp = new SolrQueryResponse();
    rsp.add("data1", Float.NaN);
    rsp.add("data2", Double.NEGATIVE_INFINITY);
    rsp.add("data3", Float.POSITIVE_INFINITY);
    SmileResponseWriter smileResponseWriter = new SmileResponseWriter();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    smileResponseWriter.write(baos,req,rsp);
    @SuppressWarnings({"rawtypes"})
    Map m = (Map) decodeSmile(new ByteArrayInputStream(baos.toByteArray()));
    CharArr out = new CharArr();
    JSONWriter jsonWriter = new JSONWriter(out, 2);
    jsonWriter.setIndentSize(-1); // indentation by default
    jsonWriter.write(m);
    String s = new String(Utils.toUTF8(out), StandardCharsets.UTF_8);
    assertEquals(s , "{\"data1\":NaN,\"data2\":-Infinity,\"data3\":Infinity}");

    req.close();
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testJSON() throws IOException {
    SolrQueryRequest req = req("wt","json","json.nl","arrarr");
    SolrQueryResponse rsp = new SolrQueryResponse();
    SmileResponseWriter w = new SmileResponseWriter();

    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    @SuppressWarnings({"rawtypes"})
    NamedList nl = new NamedList();
    nl.add("data1", "he\u2028llo\u2029!");       // make sure that 2028 and 2029 are both escaped (they are illegal in javascript)
    nl.add(null, 42);
    rsp.add("nl", nl);

    rsp.add("byte", Byte.valueOf((byte)-3));
    rsp.add("short", Short.valueOf((short)-4));
    String expected = "{\"nl\":[[\"data1\",\"he\\u2028llo\\u2029!\"],[null,42]],byte:-3,short:-4}";
    w.write(buf, req, rsp);
    @SuppressWarnings({"rawtypes"})
    Map m = (Map) decodeSmile(new ByteArrayInputStream(buf.toByteArray()));
    @SuppressWarnings({"rawtypes"})
    Map o2 = (Map) new ObjectBuilder(new JSONParser(new StringReader(expected))).getObject();
    assertEquals(Utils.toJSONString(m), Utils.toJSONString(o2));
    req.close();
  }

  @Test
  public void testJSONSolrDocument() throws IOException {
    SolrQueryRequest req = req(CommonParams.WT,"json",
        CommonParams.FL,"id,score");
    SolrQueryResponse rsp = new SolrQueryResponse();
    SmileResponseWriter w = new SmileResponseWriter();

    ReturnFields returnFields = new SolrReturnFields(req);
    rsp.setReturnFields(returnFields);

    ByteArrayOutputStream buf = new ByteArrayOutputStream();

    SolrDocument solrDoc = new SolrDocument();
    solrDoc.addField("id", "1");
    solrDoc.addField("subject", "hello2");
    solrDoc.addField("title", "hello3");
    solrDoc.addField("score", "0.7");

    SolrDocumentList list = new SolrDocumentList();
    list.setNumFound(1);
    list.setStart(0);
    list.setMaxScore(0.7f);
    list.add(solrDoc);

    rsp.addResponse(list);

    w.write(buf, req, rsp);

    byte[] bytes = buf.toByteArray();
    @SuppressWarnings({"rawtypes"})
    Map m = (Map) decodeSmile(new ByteArrayInputStream(bytes));
    m = (Map) m.get("response");
    @SuppressWarnings({"rawtypes"})
    List l = (List) m.get("docs");
    @SuppressWarnings({"rawtypes"})
    Map doc = (Map) l.get(0);
    assertFalse(doc.containsKey("subject"));
    assertFalse(doc.containsKey("title"));
    assertTrue(doc.containsKey("id"));
    assertTrue(doc.containsKey("score"));
    req.close();
  }


  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void test10Docs() throws IOException {
    SolrQueryResponse response = new SolrQueryResponse();
    SolrDocumentList l = constructSolrDocList(response);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new SmileResponseWriter().write(baos, new LocalSolrQueryRequest(null, new ModifiableSolrParams()), response);

    byte[] bytes = baos.toByteArray();
    Map m = (Map) decodeSmile(new ByteArrayInputStream(bytes, 0, bytes.length));
    m = (Map) m.get("results");
    List lst = (List) m.get("docs");
    assertEquals(lst.size(),10);
    for (int i = 0; i < lst.size(); i++) {
      m = (Map) lst.get(i);
      SolrDocument d = new SolrDocument();
      d.putAll(m);
      compareSolrDocument(l.get(i), d);
    }

  }

  @SuppressWarnings({"unchecked"})
  public static SolrDocumentList constructSolrDocList(SolrQueryResponse response) {
    SolrDocumentList l = new SolrDocumentList();
    for(int i=0;i<10; i++){
      l.add(sampleDoc(random(), i));
    }

    response.getValues().add("results", l);
    return l;
  }

  public static SolrDocument sampleDoc(Random r, int bufnum) {
    SolrDocument sdoc = new SolrDocument();
    sdoc.put("id", "my_id_" + bufnum);
    sdoc.put("author", str(r, 10 + r.nextInt(10)));
    sdoc.put("address", str(r, 20 + r.nextInt(20)));
    sdoc.put("license", str(r, 10));
    sdoc.put("title", str(r, 5 + r.nextInt(10)));
    sdoc.put("title_bin", str(r, 5 + r.nextInt(10)).getBytes(StandardCharsets.UTF_8));
    sdoc.put("modified_dt", r.nextInt(1000000));
    sdoc.put("creation_dt", r.nextInt(1000000));
    sdoc.put("birthdate_dt", r.nextInt(1000000));
    sdoc.put("clean", r.nextBoolean());
    sdoc.put("dirty", r.nextBoolean());
    sdoc.put("employed", r.nextBoolean());
    sdoc.put("priority", r.nextInt(100));
    sdoc.put("dependents", r.nextInt(6));
    sdoc.put("level", r.nextInt(101));
    sdoc.put("education_level", r.nextInt(10));
    // higher level of reuse for string values
    sdoc.put("state", "S"+r.nextInt(50));
    sdoc.put("country", "Country"+r.nextInt(20));
    sdoc.put("some_boolean", ""+r.nextBoolean());
    sdoc.put("another_boolean", ""+r.nextBoolean());
    return sdoc;
  }
  // common-case ascii
  static String str(Random r, int sz) {
    StringBuffer sb = new StringBuffer(sz);
    for (int i=0; i<sz; i++) {
      sb.append('\n' + r.nextInt(128-'\n'));
    }
    return sb.toString();
  }


  public static Object decodeSmile( InputStream is) throws IOException {
    final SmileFactory smileFactory = new SmileFactory();
    com.fasterxml.jackson.databind.ObjectMapper mapper = new  com.fasterxml.jackson.databind.ObjectMapper(smileFactory);
    JsonNode jsonNode = mapper.readTree(is);
    return getVal(jsonNode);
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  public static Object getVal(JsonNode value) {
    if (value instanceof NullNode) {
      return null;
    }
    if (value instanceof NumericNode) {
      return ((NumericNode) value).numberValue();
    }
    if (value instanceof BooleanNode) {
      ((BooleanNode) value).booleanValue();
    }
    if(value instanceof ObjectNode){
      Iterator<Map.Entry<String, JsonNode>> it = ((ObjectNode)value).fields();
      Map result = new LinkedHashMap<>();
      while(it.hasNext()){
        Map.Entry<String, JsonNode> e = it.next();
        result.put(e.getKey(),getVal(e.getValue()));
      }
      return result;
    }
    if (value instanceof ArrayNode) {
      ArrayList result =  new ArrayList();
      Iterator<JsonNode> it = ((ArrayNode) value).elements();
      while (it.hasNext()) {
        result.add(getVal(it.next()));
      }
      return result;

    }
    if(value instanceof BinaryNode) {
      return ((BinaryNode) value).binaryValue();
    }

    return value.textValue();
  }


}
