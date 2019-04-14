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

package org.apache.lucene.luwak.presearcher;

import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.luwak.InputDocument;
import org.apache.lucene.luwak.Monitor;
import org.apache.lucene.luwak.MonitorQuery;
import org.apache.lucene.luwak.QueryMatch;
import org.apache.lucene.luwak.UpdateException;
import org.apache.lucene.luwak.matchers.SimpleMatcher;
import org.apache.lucene.luwak.queryparsers.LuceneQueryParser;
import org.apache.lucene.util.LuceneTestCase;

import static org.hamcrest.CoreMatchers.containsString;

public class TestPresearcherMatchCollector extends LuceneTestCase {

  public static final String TEXTFIELD = "f";

  public void testMatchCollectorShowMatches() throws IOException, UpdateException {

    try (Monitor monitor = new Monitor(new LuceneQueryParser(TEXTFIELD), new TermFilteredPresearcher())) {
      monitor.update(new MonitorQuery("1", "test"));
      monitor.update(new MonitorQuery("2", "foo bar -baz f2:quuz"));
      monitor.update(new MonitorQuery("3", "foo -test"));
      monitor.update(new MonitorQuery("4", "baz"));
      assertEquals(4, monitor.getQueryCount());

      InputDocument doc = InputDocument.builder("doc1")
          .addField(TEXTFIELD, "this is a foo test", new StandardAnalyzer())
          .addField("f2", "quuz", new StandardAnalyzer())
          .build();

      PresearcherMatches<QueryMatch> matches = monitor.debug(doc, SimpleMatcher.FACTORY);

      assertNotNull(matches.match("1", "doc1"));
      assertEquals(" f:test", matches.match("1", "doc1").presearcherMatches);
      assertNotNull(matches.match("1", "doc1").queryMatch);

      assertNotNull(matches.match("2", "doc1"));
      String pm = matches.match("2", "doc1").presearcherMatches;
      assertThat(pm, containsString("f:foo"));
      assertThat(pm, containsString("f2:quuz"));

      assertNotNull(matches.match("3", "doc1"));
      assertEquals(" f:foo", matches.match("3", "doc1").presearcherMatches);
      assertNull(matches.match("3", "doc1").queryMatch);

      assertNull(matches.match("4", "doc1"));
    }
  }

}
