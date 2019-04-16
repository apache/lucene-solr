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

import java.io.IOException;
import java.text.NumberFormat;
import java.util.Locale;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.luwak.matchers.SimpleMatcher;
import org.apache.lucene.luwak.presearcher.MatchAllPresearcher;
import org.apache.lucene.luwak.queryparsers.FlexibleLuceneQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.util.LuceneTestCase;

public class TestMonitorFlexibleLuceneQueryParser extends LuceneTestCase {

  private static final String TEXTFIELD = "TEXTFIELD";

  private static final Analyzer ANALYZER = new WhitespaceAnalyzer();

  private final FlexibleLuceneQueryParser parser = new FlexibleLuceneQueryParser(TEXTFIELD, ANALYZER);

  public void testMatchIntPointfields() throws IOException, UpdateException {

    parser.getPointsConfig().put("age", new PointsConfig(NumberFormat.getIntegerInstance(Locale.ROOT), Integer.class));

    InputDocument doc = InputDocument.builder("doc1")
        .addField(new IntPoint("age", 1))
        .addField(new IntPoint("somethingelse", 2))
        .build();
    MonitorQuery mq = new MonitorQuery("query1", "(age:1 somethingelse:2)");

    try (Monitor monitor = new Monitor(parser, MatchAllPresearcher.INSTANCE)) {
      monitor.update(mq);
      Matches<QueryMatch> matches = monitor.match(doc, SimpleMatcher.FACTORY);
      assertEquals(1, matches.getMatchCount("doc1"));

      mq = new MonitorQuery("query1", "age:1");
      monitor.update(mq);
      matches = monitor.match(doc, SimpleMatcher.FACTORY);
      assertEquals(1, matches.getMatchCount("doc1"));
    }

  }

  public void testMatchDoublePointfields() throws IOException, UpdateException {

    parser.getPointsConfig().put("money", new PointsConfig(NumberFormat.getNumberInstance(Locale.ROOT), Double.class));
    parser.getPointsConfig().put("somethingelse", new PointsConfig(NumberFormat.getIntegerInstance(Locale.ROOT), Integer.class));

    InputDocument doc = InputDocument.builder("doc1")
        .addField(new DoublePoint("money", 1.0))
        .addField(new IntPoint("somethingelse", 2))
        .build();
    MonitorQuery mq = new MonitorQuery("query1", "(money:1.0 somethingelse:2)");

    try (Monitor monitor = new Monitor(parser, MatchAllPresearcher.INSTANCE)) {
      monitor.update(mq);
      Matches<QueryMatch> matches = monitor.match(doc, SimpleMatcher.FACTORY);
      assertEquals(1, matches.getMatchCount("doc1"));

      mq = new MonitorQuery("query1", "money:1");
      monitor.update(mq);
      matches = monitor.match(doc, SimpleMatcher.FACTORY);
      assertEquals(1, matches.getMatchCount("doc1"));
    }

  }

}
