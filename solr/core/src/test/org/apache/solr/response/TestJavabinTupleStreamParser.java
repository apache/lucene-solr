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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.JavabinTupleStreamParser;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;

import static org.apache.solr.response.SmileWriterTest.constructSolrDocList;

public class TestJavabinTupleStreamParser extends SolrTestCaseJ4 {

  public void testKnown() throws IOException {
    String payload = "{\n" +
        "  \"responseHeader\":{\n" +
        "    \"zkConnected\":true,\n" +
        "    \"status\":0,\n" +
        "    \"QTime\":46},\n" +
        "  \"response\":{\n" +
        "    \"numFound\":2,\n" +
        "    \"start\":0,\n" +
        "    \"docs\":[\n" +
        "      {\n" +
        "        \"id\":\"2\",\n" +
        "        \"a_s\":\"hello2\",\n" +
        "        \"a_i\":2,\n" +
        "        \"a_f\":0.0},\n" +
        "      {\n" +
        "        \"id\":\"3\",\n" +
        "        \"a_s\":\"hello3\",\n" +
        "        \"a_i\":3,\n" +
        "        \"a_f\":3.0}]}}";
    @SuppressWarnings({"rawtypes"})
    SimpleOrderedMap nl = convert2OrderedMap((Map) Utils.fromJSONString(payload));

    byte[] bytes = serialize(nl);

    try (JavabinTupleStreamParser parser = new JavabinTupleStreamParser(new ByteArrayInputStream(bytes), true)) {
      Map<String, Object> map = parser.next();
      assertEquals("2", map.get("id"));
      map = parser.next();
      assertEquals("3", map.get("id"));
      System.out.println();
      map = parser.next();
      assertNull(map);
    }

  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public SimpleOrderedMap convert2OrderedMap(Map m) {
    SimpleOrderedMap result = new SimpleOrderedMap<>();
    m.forEach((k, v) -> {
      if (v instanceof List) v = ((List) v).iterator();
      if (v instanceof Map) v = convert2OrderedMap((Map) v);
      result.add((String) k, v);
    });
    return result;

  }

  public void testSimple() throws IOException {
    List<Map<String, Object>> l = new ArrayList<>();
    l.add(Utils.makeMap("id", 1, "f", 1.0f, "s", "Some str 1"));
    l.add(Utils.makeMap("id", 2, "f", 2.0f, "s", "Some str 2"));
    l.add(Utils.makeMap("id", 3, "f", 1.0f, "s", "Some str 3"));
    l.add(Utils.makeMap("EOF", true, "RESPONSE_TIME", 206, "sleepMillis", 1000));
    Iterator<Map<String, Object>> iterator = l.iterator();
    TupleStream tupleStream = new TupleStream() {
      @Override
      public void setStreamContext(StreamContext context) {

      }

      @Override
      public List<TupleStream> children() {
        return null;
      }

      @Override
      public void open() throws IOException {
      }

      @Override
      public void close() throws IOException {
      }

      @Override
      public Tuple read() throws IOException {
        if (iterator.hasNext()) return new Tuple(iterator.next());
        else return null;
      }

      @Override
      public StreamComparator getStreamSort() {
        return null;
      }

      @Override
      public Explanation toExplanation(StreamFactory factory) throws IOException {
        return new StreamExplanation(getStreamNodeId().toString())
            .withFunctionName("Dummy")
            .withImplementingClass(this.getClass().getName())
            .withExpressionType(Explanation.ExpressionType.STREAM_SOURCE)
            .withExpression("--non-expressible--");
      }
    };

    byte[] bytes = serialize(tupleStream);
    JavabinTupleStreamParser parser = new JavabinTupleStreamParser(new ByteArrayInputStream(bytes), true);
    @SuppressWarnings({"rawtypes"})
    Map m = parser.next();
    assertEquals(1L, m.get("id"));
    assertEquals(1.0, (Double) m.get("f"), 0.01);
    m = parser.next();
    assertEquals(2L, m.get("id"));
    assertEquals(2.0, (Double) m.get("f"), 0.01);
    m = parser.next();
    assertEquals(3L, m.get("id"));
    assertEquals(1.0, (Double) m.get("f"), 0.01);
    m = parser.next();
    assertEquals(Boolean.TRUE, m.get("EOF"));

    parser = new JavabinTupleStreamParser(new ByteArrayInputStream(bytes), false);
    m = parser.next();
    assertEquals(1, m.get("id"));
    assertEquals(1.0, (Float) m.get("f"), 0.01);
    m = parser.next();
    assertEquals(2, m.get("id"));
    assertEquals(2.0, (Float) m.get("f"), 0.01);
    m = parser.next();
    assertEquals(3, m.get("id"));
    assertEquals(1.0, (Float) m.get("f"), 0.01);
    m = parser.next();
    assertEquals(Boolean.TRUE, m.get("EOF"));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testSolrDocumentList() throws IOException {
    SolrQueryResponse response = new SolrQueryResponse();
    SolrDocumentList l = constructSolrDocList(response);
    try (JavaBinCodec jbc = new JavaBinCodec(); ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      jbc.marshal(response.getValues(), baos);
    }
    byte[] bytes = serialize(response.getValues());
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      jbc.unmarshal(new ByteArrayInputStream(bytes));
    }
    List list = new ArrayList<>();
    Map m = null;
    try (JavabinTupleStreamParser parser = new JavabinTupleStreamParser(new ByteArrayInputStream(bytes), false)) {
      while ((m = parser.next()) != null) {
        list.add(m);
      }
    }
    assertEquals(l.size(), list.size());
    for(int i =0;i<list.size();i++){
      compareSolrDocument(l.get(i),new SolrDocument((Map<String, Object>) list.get(i)));
    }

  }
  @SuppressWarnings({"unchecked"})
  public static byte[] serialize(Object o) throws IOException {
    SolrQueryResponse response = new SolrQueryResponse();
    response.getValues().add("results", o);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      jbc.marshal(response.getValues(), baos);
    }
    return baos.toByteArray();
  }
}
