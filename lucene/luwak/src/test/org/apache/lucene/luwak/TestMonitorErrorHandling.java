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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.luwak.matchers.SimpleMatcher;
import org.apache.lucene.search.MatchAllDocsQuery;

public class TestMonitorErrorHandling extends MonitorTestBase {

  private static final Analyzer ANALYZER = new WhitespaceAnalyzer();

  public void testMonitorErrors() throws Exception {

    try (Monitor monitor = new Monitor()) {
      monitor.update(
          MonitorTestBase.mq("1", "test"),
          new MonitorQuery("2", MonitorTestBase.parse("test")),
          new MonitorQuery("3", new ThrowOnRewriteQuery()));

      InputDocument doc = InputDocument.builder("doc").addField(FIELD, "test", ANALYZER).build();
      DocumentBatch batch = DocumentBatch.of(doc);
      Matches<QueryMatch> matches = monitor.match(batch, SimpleMatcher.FACTORY);

      assertEquals(1, matches.getErrors().size());
      assertEquals(2, matches.getMatchCount("doc"));
      assertEquals(3, matches.getQueriesRun());
    }
  }

  public void testMonitorQueryNullValues() {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
      Map<String, String> metadata2 = new HashMap<>();
      metadata2.put("key", null);
      new MonitorQuery("id", new MatchAllDocsQuery(), metadata2);
    });
    assertEquals("Null value for key key in metadata map", e.getMessage());
  }

}
