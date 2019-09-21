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

package org.apache.lucene.monitor;

import java.io.IOException;
import java.util.Collections;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.MatchAllDocsQuery;

import static org.hamcrest.CoreMatchers.containsString;

public abstract class FieldFilterPresearcherComponentTestBase extends PresearcherTestBase {

  public void testBatchFiltering() throws IOException {
    try (Monitor monitor = newMonitor()) {
      monitor.register(
          mq("1", "test", "language", "en"),
          mq("2", "wahl", "language", "de"),
          mq("3", "wibble", "language", "en"),
          mq("4", "*:*", "language", "de"),
          mq("5", "*:*", "language", "es"));

      Document doc1 = new Document();
      doc1.add(newTextField(TEXTFIELD, "this is a test", Field.Store.NO));
      doc1.add(newTextField("language", "en", Field.Store.NO));
      Document doc2 = new Document();
      doc2.add(newTextField(TEXTFIELD, "this is a wibble", Field.Store.NO));
      doc2.add(newTextField("language", "en", Field.Store.NO));
      Document doc3 = new Document();
      doc3.add(newTextField(TEXTFIELD, "wahl is a misspelling of whale", Field.Store.NO));
      doc3.add(newTextField("language", "en", Field.Store.NO));

      MultiMatchingQueries<QueryMatch> matches = monitor.match(new Document[]{ doc1, doc2, doc3 }, QueryMatch.SIMPLE_MATCHER);
      assertEquals(1, matches.getMatchCount(0));
      assertNotNull(matches.matches("1", 0));
      assertEquals(1, matches.getMatchCount(1));
      assertNotNull(matches.matches("3", 1));
      assertEquals(0, matches.getMatchCount(2));
      assertEquals(2, matches.getQueriesRun());
    }
  }

  public void testBatchesWithDissimilarFieldValuesThrowExceptions() throws IOException {

    Document doc1 = new Document();
    doc1.add(newTextField(TEXTFIELD, "test", Field.Store.NO));
    doc1.add(newTextField("language", "en", Field.Store.NO));
    Document doc2 = new Document();
    doc2.add(newTextField(TEXTFIELD, "test", Field.Store.NO));
    doc2.add(newTextField("language", "de", Field.Store.NO));

    try (Monitor monitor = newMonitor()) {
      IllegalArgumentException e
          = expectThrows(IllegalArgumentException.class, () -> monitor.match(new Document[]{ doc1, doc2 }, QueryMatch.SIMPLE_MATCHER));
      assertThat(e.getMessage(), containsString("language:"));
    }
  }

  public void testFieldFiltering() throws IOException {

    try (Monitor monitor = newMonitor()) {
      monitor.register(
          new MonitorQuery("1", parse("test"), null, Collections.singletonMap("language", "en")),
          new MonitorQuery("2", parse("test"), null, Collections.singletonMap("language", "de")),
          new MonitorQuery("3", parse("wibble"), null, Collections.singletonMap("language", "en")),
          new MonitorQuery("4", parse("*:*"), null, Collections.singletonMap("language", "de")));

      Document enDoc = new Document();
      enDoc.add(newTextField(TEXTFIELD, "this is a test", Field.Store.NO));
      enDoc.add(newTextField("language", "en", Field.Store.NO));

      MatchingQueries<QueryMatch> en = monitor.match(enDoc, QueryMatch.SIMPLE_MATCHER);
      assertEquals(1, en.getMatchCount());
      assertNotNull(en.matches("1"));
      assertEquals(1, en.getQueriesRun());

      Document deDoc = new Document();
      deDoc.add(newTextField(TEXTFIELD, "das ist ein test", Field.Store.NO));
      deDoc.add(newTextField("language", "de", Field.Store.NO));

      MatchingQueries<QueryMatch> de = monitor.match(deDoc, QueryMatch.SIMPLE_MATCHER);
      assertEquals(2, de.getMatchCount());
      assertEquals(2, de.getQueriesRun());
      assertNotNull(de.matches("2"));
      assertNotNull(de.matches("4"));

      Document bothDoc = new Document();
      bothDoc.add(newTextField(TEXTFIELD, "this is ein test", Field.Store.NO));
      bothDoc.add(newTextField("language", "en", Field.Store.NO));
      bothDoc.add(newTextField("language", "de", Field.Store.NO));

      MatchingQueries<QueryMatch> both = monitor.match(bothDoc, QueryMatch.SIMPLE_MATCHER);
      assertEquals(3, both.getMatchCount());
      assertEquals(3, both.getQueriesRun());
    }
  }

  public void testFilteringOnMatchAllQueries() throws IOException {
    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", new MatchAllDocsQuery(), null, Collections.singletonMap("language", "de")));

      Document enDoc = new Document();
      enDoc.add(newTextField(TEXTFIELD, "this is a test", Field.Store.NO));
      enDoc.add(newTextField("language", "en", Field.Store.NO));
      MatchingQueries<QueryMatch> matches = monitor.match(enDoc, QueryMatch.SIMPLE_MATCHER);
      assertEquals(0, matches.getMatchCount());
      assertEquals(0, matches.getQueriesRun());
    }
  }

  public void testDebugQueries() throws Exception {
    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", parse("test"), null, Collections.singletonMap("language", "en")));

      Document enDoc = new Document();
      enDoc.add(newTextField(TEXTFIELD, "this is a test", Field.Store.NO));
      enDoc.add(newTextField("language", "en", Field.Store.NO));

      PresearcherMatches<QueryMatch> matches = monitor.debug(enDoc, QueryMatch.SIMPLE_MATCHER);
      assertFalse(matches.match("1", 0).presearcherMatches.isEmpty());
    }
  }

}
