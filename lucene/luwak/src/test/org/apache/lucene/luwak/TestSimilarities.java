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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.luwak.matchers.ScoringMatch;
import org.apache.lucene.luwak.matchers.ScoringMatcher;
import org.apache.lucene.luwak.presearcher.MatchAllPresearcher;
import org.apache.lucene.luwak.queryparsers.LuceneQueryParser;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.LuceneTestCase;

public class TestSimilarities extends LuceneTestCase {

  public void testNonStandardSimilarity() throws IOException, UpdateException {

    try (Monitor monitor = new Monitor(new LuceneQueryParser("field"), new MatchAllPresearcher())) {
      monitor.update(new MonitorQuery("1", "test"));

      Similarity similarity = new ClassicSimilarity() {
        @Override
        public float tf(float freq) {
          return 1000f;
        }
      };

      InputDocument doc = InputDocument.builder("doc")
          .addField("field", "this is a test", new StandardAnalyzer()).build();

      DocumentBatch batch = new DocumentBatch.Builder()
          .add(doc)
          .setSimilarity(similarity)
          .build();

      DocumentBatch standardBatch = new DocumentBatch.Builder()
          .add(doc)
          .setSimilarity(new ClassicSimilarity())
          .build();

      Matches<ScoringMatch> standard = monitor.match(standardBatch, ScoringMatcher.FACTORY);
      Matches<ScoringMatch> withSim = monitor.match(batch, ScoringMatcher.FACTORY);

      float standScore = standard.getMatches("doc").iterator().next().getScore();
      float simScore = withSim.getMatches("doc").iterator().next().getScore();
      assertEquals(standScore, simScore / 1000, 0.1f);
    }
  }
}
