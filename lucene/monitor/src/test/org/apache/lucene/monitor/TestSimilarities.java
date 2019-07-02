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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;

public class TestSimilarities extends MonitorTestBase {

  public void testNonStandardSimilarity() throws Exception {

    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", MonitorTestBase.parse("test")));

      Similarity similarity = new ClassicSimilarity() {
        @Override
        public float tf(float freq) {
          return 1000f;
        }
      };

      Document doc = new Document();
      doc.add(newTextField("field", "this is a test", Field.Store.NO));

      MatchingQueries<ScoringMatch> standard = monitor.match(doc, ScoringMatch.matchWithSimilarity(new ClassicSimilarity()));
      MatchingQueries<ScoringMatch> withSim = monitor.match(doc, ScoringMatch.matchWithSimilarity(similarity));

      float standScore = standard.getMatches().iterator().next().getScore();
      float simScore = withSim.getMatches().iterator().next().getScore();
      assertEquals(standScore, simScore / 1000, 0.1f);
    }
  }
}
