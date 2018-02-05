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
package org.apache.lucene.codecs;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.codecs.CompetitiveFreqNormAccumulator.FreqAndNorm;
import org.apache.lucene.util.LuceneTestCase;

public class TestCompetitiveFreqNormAccumulator extends LuceneTestCase {

  public void testBasics() {
    CompetitiveFreqNormAccumulator acc = new CompetitiveFreqNormAccumulator();
    Set<FreqAndNorm> expected = new HashSet<>();

    acc.add(3, 5);
    expected.add(new FreqAndNorm(3, 5));
    assertEquals(expected, acc.getCompetitiveFreqNormPairs());

    acc.add(6, 11);
    expected.add(new FreqAndNorm(6, 11));
    assertEquals(expected, acc.getCompetitiveFreqNormPairs());

    acc.add(10, 13);
    expected.add(new FreqAndNorm(10, 13));
    assertEquals(expected, acc.getCompetitiveFreqNormPairs());
    
    acc.add(1, 2);
    expected.add(new FreqAndNorm(1, 2));
    assertEquals(expected, acc.getCompetitiveFreqNormPairs());

    acc.add(7, 9);
    expected.remove(new FreqAndNorm(6, 11));
    expected.add(new FreqAndNorm(7, 9));
    assertEquals(expected, acc.getCompetitiveFreqNormPairs());

    acc.add(8, 2);
    expected.clear();
    expected.add(new FreqAndNorm(10, 13));
    expected.add(new FreqAndNorm(8, 2));
    assertEquals(expected, acc.getCompetitiveFreqNormPairs());
  }

  public void testExtremeNorms() {
    CompetitiveFreqNormAccumulator acc = new CompetitiveFreqNormAccumulator();
    Set<FreqAndNorm> expected = new HashSet<>();

    acc.add(3, 5);
    expected.add(new FreqAndNorm(3, 5));
    assertEquals(expected, acc.getCompetitiveFreqNormPairs());

    acc.add(10, 10000);
    expected.add(new FreqAndNorm(10, 10000));
    assertEquals(expected, acc.getCompetitiveFreqNormPairs());

    acc.add(5, 200);
    expected.add(new FreqAndNorm(5, 200));
    assertEquals(expected, acc.getCompetitiveFreqNormPairs());

    acc.add(20, -100);
    expected.add(new FreqAndNorm(20, -100));
    assertEquals(expected, acc.getCompetitiveFreqNormPairs());

    acc.add(30, -3);
    expected.add(new FreqAndNorm(30, -3));
    assertEquals(expected, acc.getCompetitiveFreqNormPairs());
  }

  public void testOmitFreqs() {
    CompetitiveFreqNormAccumulator acc = new CompetitiveFreqNormAccumulator();

    acc.add(1, 5);
    acc.add(1, 7);
    acc.add(1, 4);

    assertEquals(Collections.singleton(new FreqAndNorm(1, 4)), acc.getCompetitiveFreqNormPairs());
  }

  public void testOmitNorms() {
    CompetitiveFreqNormAccumulator acc = new CompetitiveFreqNormAccumulator();

    acc.add(5, 1);
    acc.add(7, 1);
    acc.add(4, 1);

    assertEquals(Collections.singleton(new FreqAndNorm(7, 1)), acc.getCompetitiveFreqNormPairs());
  }
}
