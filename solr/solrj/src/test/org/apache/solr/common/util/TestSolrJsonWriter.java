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

package org.apache.solr.common.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.annotation.JsonProperty;

public class TestSolrJsonWriter  extends SolrTestCaseJ4 {
  public void test() throws IOException {

    Map<String, Object> map = new HashMap<>();
    map.put("k1","v1");
    map.put("k2",1);
    map.put("k3",false);
    map.put("k4",Utils.makeMap("k41", "v41", "k42","v42"));
    map.put("k5", (MapWriter) ew -> {
      ew.put("k61","v61");
      ew.put("k62","v62");
      ew.put("k63", (IteratorWriter) iw -> iw.add("v631")
          .add("v632"));
    });

    StringWriter writer = new StringWriter();
    try (SolrJSONWriter jsonWriter = new SolrJSONWriter(writer).setIndent(false)) {
      jsonWriter.writeObj(map);
    }
    String json = writer.toString();
    Object o = Utils.fromJSONString(json);
    assertEquals("v1", Utils.getObjectByPath(o, true, "k1"));
    assertEquals(1l, Utils.getObjectByPath(o, true, "k2"));
    assertEquals(Boolean.FALSE, Utils.getObjectByPath(o, true, "k3"));
    assertEquals("v41", Utils.getObjectByPath(o, true, "k4/k41"));
    assertEquals("v42", Utils.getObjectByPath(o, true, "k4/k42"));
    assertEquals("v61", Utils.getObjectByPath(o, true, "k5/k61"));
    assertEquals("v62", Utils.getObjectByPath(o, true, "k5/k62"));
    assertEquals("v631", Utils.getObjectByPath(o, true, "k5/k63[0]"));
    assertEquals("v632", Utils.getObjectByPath(o, true, "k5/k63[1]"));
    C1 c1 = new C1();

    int iters = 10000;
    writer = new StringWriter();
    try (SolrJSONWriter jsonWriter = new SolrJSONWriter(writer).setIndent(false)) {
      jsonWriter.writeObj(c1);
    }
   assertEquals(json, writer.toString());


   /*Used in perf testing
   System.out.println("JSON REFLECT write time : "+write2String(c1,iters));
    System.out.println("JSON Map write time : "+write2String(map, iters));

    System.out.println("javabin REFLECT write time : "+write2Javabin(c1,iters));
    System.out.println("javabin Map write time : "+write2Javabin(map, iters));
    */

  }

  @SuppressForbidden(reason="used for perf testing numbers only")
  private long write2String(Object o, int iters) throws IOException {
    long start = System.currentTimeMillis() ;
    for
    (int i = 0;i<iters;i++) {
      StringWriter writer = new StringWriter();

      try (SolrJSONWriter jsonWriter = new SolrJSONWriter(writer)) {
        jsonWriter.setIndent(true).writeObj(o);
      }
    }
    return System.currentTimeMillis()-start;

  }
  @SuppressForbidden(reason="used for perf testing numbers only")
  private long write2Javabin(Object o, int iters) throws IOException {
    long start = System.currentTimeMillis() ;
    for (int i = 0;i<iters;i++) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      new JavaBinCodec(null).marshal(o, baos);
    }
    return System.currentTimeMillis()-start;

//    JSON REFLECT write time : 76
//    JSON Map write time : 42
//    javabin REFLECT write time : 50
//    javabin Map write time : 32
//
//    before
//    JSON REFLECT write time : 181
//    JSON Map write time : 38
//    javabin REFLECT write time : 111
//    javabin Map write time : 33
  }


  public static class C1 implements ReflectMapWriter {
    @JsonProperty
    public String k1 = "v1";
    @JsonProperty
    public int k2 = 1;
    @JsonProperty
    public boolean k3 = false;
    @JsonProperty
    public C2 k4 = new C2();

    @JsonProperty
    public C3 k5 = new C3() ;


  }

  public static class C3 implements ReflectMapWriter {
    @JsonProperty
    public String k61 = "v61";

    @JsonProperty
    public String k62 = "v62";

    @JsonProperty
    public List<String> k63= ImmutableList.of("v631","v632");
  }

  public static class C2 implements ReflectMapWriter {
    @JsonProperty
    public String k41 = "v41";

    @JsonProperty
    public String k42 = "v42";

  }
}
