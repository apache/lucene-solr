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

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;

public class TestSolrJsonWriter  extends SolrTestCaseJ4 {
  public void test() throws IOException {
    StringWriter writer = new StringWriter();

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

    new SolrJSONWriter(writer)
        .setIndent(true)
        .writeObj(map)
        .close();
    Object o = Utils.fromJSONString(writer.toString());
    assertEquals("v1", Utils.getObjectByPath(o, true, "k1"));
    assertEquals(1l, Utils.getObjectByPath(o, true, "k2"));
    assertEquals(Boolean.FALSE, Utils.getObjectByPath(o, true, "k3"));
    assertEquals("v41", Utils.getObjectByPath(o, true, "k4/k41"));
    assertEquals("v42", Utils.getObjectByPath(o, true, "k4/k42"));
    assertEquals("v61", Utils.getObjectByPath(o, true, "k5/k61"));
    assertEquals("v62", Utils.getObjectByPath(o, true, "k5/k62"));
    assertEquals("v631", Utils.getObjectByPath(o, true, "k5/k63[0]"));
    assertEquals("v632", Utils.getObjectByPath(o, true, "k5/k63[1]"));
  }
}
