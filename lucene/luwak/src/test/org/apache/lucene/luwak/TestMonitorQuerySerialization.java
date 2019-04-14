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

package org.apache.lucene.luwak;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;

public class TestMonitorQuerySerialization extends LuceneTestCase {

  private static void assertSerializes(MonitorQuery mq) {
    MonitorQuery sds = MonitorQuery.deserialize(MonitorQuery.serialize(mq));
    assertEquals(mq, sds);
    assertEquals(mq.hash(), sds.hash());
  }

  public void testSimpleQuery() {
    MonitorQuery mq = new MonitorQuery("1", "test");
    assertSerializes(mq);
  }

  public void testQueryWithMetadata() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("lang", "en");
    metadata.put("wibble", "quack");
    MonitorQuery mq = new MonitorQuery("1", "test", metadata);
    assertSerializes(mq);
  }

  public void testMonitorQueryToString() {
    MonitorQuery mq = new MonitorQuery("1", "test");
    assertEquals("1: test", mq.toString());

    Map<String, String> metadata = new HashMap<>();
    metadata.put("lang", "en");
    metadata.put("foo", "bar");
    assertEquals("1: test { foo: bar, lang: en }", new MonitorQuery("1", "test", metadata).toString());
  }

}
