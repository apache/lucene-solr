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

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrReturnFields;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test some aspects of JSON/python writer output (very incomplete)
 *
 */
public class JSONWriterTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }

  private void jsonEq(String expected, String received) {
    expected = expected.trim();
    received = received.trim();
    assertEquals(expected, received);
  }
  
  @Test
  public void testTypes() throws IOException {
    SolrQueryRequest req = req("dummy");
    SolrQueryResponse rsp = new SolrQueryResponse();
    QueryResponseWriter w = new PythonResponseWriter();

    StringWriter buf = new StringWriter();
    rsp.add("data1", Float.NaN);
    rsp.add("data2", Double.NEGATIVE_INFINITY);
    rsp.add("data3", Float.POSITIVE_INFINITY);
    w.write(buf, req, rsp);
    jsonEq(buf.toString(), "{'data1':float('NaN'),'data2':-float('Inf'),'data3':float('Inf')}");

    w = new RubyResponseWriter();
    buf = new StringWriter();
    w.write(buf, req, rsp);
    jsonEq(buf.toString(), "{'data1'=>(0.0/0.0),'data2'=>-(1.0/0.0),'data3'=>(1.0/0.0)}");

    w = new JSONResponseWriter();
    buf = new StringWriter();
    w.write(buf, req, rsp);
    jsonEq(buf.toString(), "{\"data1\":\"NaN\",\"data2\":\"-Infinity\",\"data3\":\"Infinity\"}");
    req.close();
  }

  @Test
  public void testJSON() throws IOException {
    final String[] namedListStyles = new String[] {
        JSONWriter.JSON_NL_FLAT,
        JSONWriter.JSON_NL_MAP,
        JSONWriter.JSON_NL_ARROFARR,
        JSONWriter.JSON_NL_ARROFMAP,
        JSONWriter.JSON_NL_ARROFNVP,
    };
    for (final String namedListStyle : namedListStyles) {
      implTestJSON(namedListStyle);
    }
    assertEquals(JSONWriter.JSON_NL_STYLE_COUNT, namedListStyles.length);
  }

  private void implTestJSON(final String namedListStyle) throws IOException {
    SolrQueryRequest req = req("wt","json","json.nl",namedListStyle);
    SolrQueryResponse rsp = new SolrQueryResponse();
    JSONResponseWriter w = new JSONResponseWriter();

    StringWriter buf = new StringWriter();
    NamedList nl = new NamedList();
    nl.add("data1", "he\u2028llo\u2029!");       // make sure that 2028 and 2029 are both escaped (they are illegal in javascript)
    nl.add(null, 42);
    nl.add(null, null);
    rsp.add("nl", nl);

    rsp.add("byte", Byte.valueOf((byte)-3));
    rsp.add("short", Short.valueOf((short)-4));
    rsp.add("bytes", "abc".getBytes(StandardCharsets.UTF_8));

    w.write(buf, req, rsp);

    final String expectedNLjson;
    if (namedListStyle == JSONWriter.JSON_NL_FLAT) {
      expectedNLjson = "\"nl\":[\"data1\",\"he\\u2028llo\\u2029!\",null,42,null,null]";
    } else if (namedListStyle == JSONWriter.JSON_NL_MAP) {
      expectedNLjson = "\"nl\":{\"data1\":\"he\\u2028llo\\u2029!\",\"\":42,\"\":null}";
    } else if (namedListStyle == JSONWriter.JSON_NL_ARROFARR) {
      expectedNLjson = "\"nl\":[[\"data1\",\"he\\u2028llo\\u2029!\"],[null,42],[null,null]]";
    } else if (namedListStyle == JSONWriter.JSON_NL_ARROFMAP) {
      expectedNLjson = "\"nl\":[{\"data1\":\"he\\u2028llo\\u2029!\"},42,null]";
    } else if (namedListStyle == JSONWriter.JSON_NL_ARROFNVP) {
      expectedNLjson = "\"nl\":[{\"name\":\"data1\",\"str\":\"he\\u2028llo\\u2029!\"},{\"int\":42},{\"null\":null}]";
    } else {
      expectedNLjson = null;
      fail("unexpected namedListStyle="+namedListStyle);
    }

    jsonEq("{"+expectedNLjson+",\"byte\":-3,\"short\":-4,\"bytes\":\"YWJj\"}", buf.toString());
    req.close();
  }

  @Test
  public void testJSONSolrDocument() throws IOException {
    SolrQueryRequest req = req(CommonParams.WT,"json",
                               CommonParams.FL,"id,score");
    SolrQueryResponse rsp = new SolrQueryResponse();
    JSONResponseWriter w = new JSONResponseWriter();

    ReturnFields returnFields = new SolrReturnFields(req);
    rsp.setReturnFields(returnFields);

    StringWriter buf = new StringWriter();

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
    String result = buf.toString();
    assertFalse("response contains unexpected fields: " + result, 
                result.contains("hello") || 
                result.contains("\"subject\"") || 
                result.contains("\"title\""));
    assertTrue("response doesn't contain expected fields: " + result, 
               result.contains("\"id\"") &&
               result.contains("\"score\""));


    req.close();
  }

  @Test
  public void testArrnvpWriterOverridesAllWrites() {
    // List rather than Set because two not-overridden methods could share name but not signature
    final List<String> methodsExpectedNotOverriden = new ArrayList<>(14);
    methodsExpectedNotOverriden.add("writeResponse");
    methodsExpectedNotOverriden.add("writeKey");
    methodsExpectedNotOverriden.add("writeNamedListAsMapMangled");
    methodsExpectedNotOverriden.add("writeNamedListAsMapWithDups");
    methodsExpectedNotOverriden.add("writeNamedListAsArrMap");
    methodsExpectedNotOverriden.add("writeNamedListAsArrArr");
    methodsExpectedNotOverriden.add("writeNamedListAsFlat");
    methodsExpectedNotOverriden.add("writeEndDocumentList");
    methodsExpectedNotOverriden.add("writeMapOpener");
    methodsExpectedNotOverriden.add("writeMapSeparator");
    methodsExpectedNotOverriden.add("writeMapCloser");
    methodsExpectedNotOverriden.add("public void org.apache.solr.response.JSONWriter.writeArray(java.lang.String,java.util.List) throws java.io.IOException");
    methodsExpectedNotOverriden.add("writeArrayOpener");
    methodsExpectedNotOverriden.add("writeArraySeparator");
    methodsExpectedNotOverriden.add("writeArrayCloser");
    methodsExpectedNotOverriden.add("public void org.apache.solr.response.JSONWriter.writeMap(org.apache.solr.common.MapWriter) throws java.io.IOException");
    methodsExpectedNotOverriden.add("public void org.apache.solr.response.JSONWriter.writeIterator(org.apache.solr.common.IteratorWriter) throws java.io.IOException");

    final Class<?> subClass = ArrayOfNamedValuePairJSONWriter.class;
    final Class<?> superClass = subClass.getSuperclass();

    for (final Method superClassMethod : superClass.getDeclaredMethods()) {
      final String methodName = superClassMethod.getName();
      final String methodFullName = superClassMethod.toString();
      if (!methodName.startsWith("write")) continue;

      final int modifiers = superClassMethod.getModifiers();
      if (Modifier.isFinal(modifiers)) continue;
      if (Modifier.isStatic(modifiers)) continue;
      if (Modifier.isPrivate(modifiers)) continue;

      final boolean expectOverriden = !methodsExpectedNotOverriden.contains(methodName)
          && !methodsExpectedNotOverriden.contains(methodFullName);

      try {
        final Method subClassMethod = subClass.getDeclaredMethod(
            superClassMethod.getName(),
            superClassMethod.getParameterTypes());

        if (expectOverriden) {
          assertEquals("getReturnType() difference",
              superClassMethod.getReturnType(),
              subClassMethod.getReturnType());
        } else {
          fail(subClass + " must not override '" + superClassMethod + "'");
        }
      } catch (NoSuchMethodException e) {
        if (expectOverriden) {
          fail(subClass + " needs to override '" + superClassMethod + "'");
        } else {
          assertTrue(methodName+" not found in remaining "+methodsExpectedNotOverriden, methodsExpectedNotOverriden.remove(methodName)|| methodsExpectedNotOverriden.remove(methodFullName));
        }
      }
    }

    assertTrue("methodsExpected NotOverriden but NotFound instead: "+methodsExpectedNotOverriden,
        methodsExpectedNotOverriden.isEmpty());
  }

  @Test
  public void testArrnvpWriterLacksMethodsOfItsOwn() {
    final Class<?> subClass = ArrayOfNamedValuePairJSONWriter.class;
    final Class<?> superClass = subClass.getSuperclass();
    // ArrayOfNamedValuePairJSONWriter is a simple sub-class
    // which should have (almost) no methods of its own
    for (final Method subClassMethod : subClass.getDeclaredMethods()) {
      // only own private method of its own
      if (subClassMethod.getName().equals("ifNeededWriteTypeAsKey")) continue;
      try {
        final Method superClassMethod = superClass.getDeclaredMethod(
            subClassMethod.getName(),
            subClassMethod.getParameterTypes());

          assertEquals("getReturnType() difference",
              subClassMethod.getReturnType(),
              superClassMethod.getReturnType());
      } catch (NoSuchMethodException e) {
          fail(subClass + " should not have '" + subClassMethod + "' method of its own");
      }
    }
  }
  
  @Test
  public void testConstantsUnchanged() {
    assertEquals("json.nl", JSONWriter.JSON_NL_STYLE);
    assertEquals("map", JSONWriter.JSON_NL_MAP);
    assertEquals("flat", JSONWriter.JSON_NL_FLAT);
    assertEquals("arrarr", JSONWriter.JSON_NL_ARROFARR);
    assertEquals("arrmap", JSONWriter.JSON_NL_ARROFMAP);
    assertEquals("arrnvp", JSONWriter.JSON_NL_ARROFNVP);
    assertEquals("json.wrf", JSONWriter.JSON_WRAPPER_FUNCTION);
  }

}
