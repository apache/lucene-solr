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

package org.apache.lucene.luwak.matchers;

import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.luwak.InputDocument;
import org.apache.lucene.luwak.Matches;
import org.apache.lucene.luwak.Monitor;
import org.apache.lucene.luwak.MonitorQuery;
import org.apache.lucene.luwak.UpdateException;
import org.apache.lucene.luwak.presearcher.MatchAllPresearcher;
import org.apache.lucene.luwak.queryparsers.LuceneQueryParser;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.util.LuceneTestCase;

public class TestExplainingMatcher extends LuceneTestCase {

  public void testExplainingMatcher() throws IOException, UpdateException {

    try (Monitor monitor = new Monitor(new LuceneQueryParser("field"), MatchAllPresearcher.INSTANCE)) {
      monitor.update(new MonitorQuery("1", "test"), new MonitorQuery("2", "wibble"));

      InputDocument doc1 = InputDocument.builder("doc1").addField("field", "test", new StandardAnalyzer()).build();

      Matches<ExplainingMatch> matches = monitor.match(doc1, ExplainingMatcher.FACTORY);
      assertNotNull(matches.matches("1", "doc1"));
      assertNotNull(matches.matches("1", "doc1").getExplanation());
    }
  }

  public void testHashcodeAndEquals() {

    ExplainingMatch m1 = new ExplainingMatch("1", "1", Explanation.match(0.1f, "an explanation"));
    ExplainingMatch m2 = new ExplainingMatch("1", "2", Explanation.match(0.1f, "an explanation"));
    ExplainingMatch m3 = new ExplainingMatch("1", "1", Explanation.match(0.1f, "another explanation"));
    ExplainingMatch m4 = new ExplainingMatch("1", "1", Explanation.match(0.1f, "an explanation"));

    assertEquals(m1, m4);
    assertEquals(m1.hashCode(), m4.hashCode());
    assertNotEquals(m1, m2);
    assertNotEquals(m1, m3);
    assertNotEquals(m3, m4);
  }
}
