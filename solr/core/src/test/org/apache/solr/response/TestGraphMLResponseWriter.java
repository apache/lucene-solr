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

import java.io.StringWriter;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.graph.Traversal;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.BaseTestHarness;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestGraphMLResponseWriter extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml","schema12.xml");
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testGraphMLOutput() throws Exception {
    SolrQueryRequest request = req("blah", "blah"); // Just need a request to attach the stream and traversal to.
    @SuppressWarnings({"rawtypes"})
    SolrQueryResponse response = new SolrQueryResponse();
    @SuppressWarnings({"rawtypes"})
    Map context = request.getContext();
    TupleStream stream = new TestStream(); //Simulates a GatherNodesStream
    Traversal traversal = new Traversal();
    context.put("traversal", traversal);
    context.put("stream", stream);
    StringWriter writer = new StringWriter();

    GraphMLResponseWriter graphMLResponseWriter = new GraphMLResponseWriter();
    graphMLResponseWriter.write(writer, request, response);
    String graphML = writer.toString();

    //Validate the nodes
    String error = BaseTestHarness.validateXPath(graphML,
                                   "//graph/node[1][@id ='bill']",
                                   "//graph/node[2][@id ='jim']",
                                   "//graph/node[3][@id ='max']");
    if(error != null) {
      throw new Exception(error);
    }
    //Validate the edges
    error = BaseTestHarness.validateXPath(graphML,
                            "//graph/edge[1][@source ='jim']",
                            "//graph/edge[1][@target ='bill']",
                            "//graph/edge[2][@source ='max']",
                            "//graph/edge[2][@target ='bill']",
                            "//graph/edge[3][@source ='max']",
                            "//graph/edge[3][@target ='jim']",
                            "//graph/edge[4][@source ='jim']",
                            "//graph/edge[4][@target ='max']"
        );

    if(error != null) {
      throw new Exception(error);
    }

  }

  @SuppressWarnings({"unchecked"})
  private static class TestStream extends TupleStream {

    private Iterator<Tuple> tuples;

    public TestStream() {
        //Create some nodes
      List<Tuple> testTuples = new ArrayList<>();
      @SuppressWarnings({"rawtypes"})
      Map m1 = new HashMap();

      List<String> an1 = new ArrayList<>();
      an1.add("jim");
      an1.add("max");
      m1.put("node", "bill");
      m1.put("ancestors", an1);
      testTuples.add(new Tuple(m1));

      @SuppressWarnings({"rawtypes"})
      Map m2 = new HashMap();
      List<String> an2 = new ArrayList<>();
      an2.add("max");
      m2.put("node", "jim");
      m2.put("ancestors", an2);
      testTuples.add(new Tuple(m2));

      @SuppressWarnings({"rawtypes"})
      Map m3 = new HashMap();
      List<String> an3 = new ArrayList<>();
      an3.add("jim");
      m3.put("node", "max");
      m3.put("ancestors", an3);
      testTuples.add(new Tuple(m3));

      tuples = testTuples.iterator();
    }

    public StreamComparator getStreamSort() {
      return null;
    }

    public void close() {

    }

    public void open() {

    }

    public List<TupleStream> children() {
      return null;
    }

    @SuppressWarnings({"unchecked"})
    public Tuple read() {
      if(tuples.hasNext()) {
        return tuples.next();
      } else {
        @SuppressWarnings({"rawtypes"})
        Map map = new HashMap();
        map.put("EOF", true);
        return new Tuple(map);
      }
    }

    public void setStreamContext(StreamContext streamContext) {

    }

    public Explanation toExplanation(StreamFactory factory) {
      return null;
    }

  }
}
