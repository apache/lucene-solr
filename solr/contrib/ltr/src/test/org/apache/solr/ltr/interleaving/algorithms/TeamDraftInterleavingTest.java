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

package org.apache.solr.ltr.interleaving.algorithms;


import java.util.ArrayList;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.search.ScoreDoc;
import org.apache.solr.ltr.interleaving.InterleavingResult;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertTrue;

public class TeamDraftInterleavingTest {
  private TeamDraftInterleaving toTest;
  private ScoreDoc[] rerankedA,rerankedB;
  private ScoreDoc a1,a2,a3,a4,a5;
  private ScoreDoc b1,b2,b3,b4,b5;

  
  @Before
  public void setup() {
    toTest = new TeamDraftInterleaving();
    TeamDraftInterleaving.setRANDOM(new Random(10101010));//Random Boolean Choices Generation from Seed: [0,1,1]
  }

  protected void initDifferentOrderRerankLists() {
    a1 = new ScoreDoc(1,10,1);
    a2 = new ScoreDoc(5,7,1);
    a3 = new ScoreDoc(4,6,1);
    a4 = new ScoreDoc(2,5,1);
    a5 = new ScoreDoc(3,4,1);
    rerankedA = new ScoreDoc[]{a1,a2,a3,a4,a5};

    b1 = new ScoreDoc(1,10,1);
    b2 = new ScoreDoc(4,7,1);
    b3 = new ScoreDoc(5,6,1);
    b4 = new ScoreDoc(3,5,1);
    b5 = new ScoreDoc(2,4,1);
    rerankedB = new ScoreDoc[]{b1,b2,b3,b4,b5};
  }

  /**
   * Random Boolean Choices Generation from Seed: [0,1,1]
   */
  @Test
  public void interleaving_twoDifferentLists_shouldInterleaveTeamDraft() {
    initDifferentOrderRerankLists();

    InterleavingResult interleaved = toTest.interleave(rerankedA, rerankedB);
    ScoreDoc[] interleavedResults = interleaved.getInterleavedResults();
    
    assertThat(interleavedResults.length,is(5));
    
    assertThat(interleavedResults[0],is(a1));
    assertThat(interleavedResults[1],is(b2));
    
    assertThat(interleavedResults[2],is(b3));
    assertThat(interleavedResults[3],is(a4));
    
    assertThat(interleavedResults[4],is(b4));
  }

  /**
   * Random Boolean Choices Generation from Seed: [0,1,1]
   */
  @Test
  public void interleaving_twoDifferentLists_shouldBuildCorrectInterleavingPicks() {
    initDifferentOrderRerankLists();

    InterleavingResult interleaved = toTest.interleave(rerankedA, rerankedB);

    ArrayList<Set<Integer>> interleavingPicks = interleaved.getInterleavingPicks();
    Set<Integer> modelAPicks = interleavingPicks.get(0);
    Set<Integer> modelBPicks = interleavingPicks.get(1);

    assertThat(modelAPicks.size(),is(2));
    assertThat(modelBPicks.size(),is(3));

    assertTrue(modelAPicks.contains(a1.doc));
    assertTrue(modelAPicks.contains(a4.doc));

    assertTrue(modelBPicks.contains(b2.doc));
    assertTrue(modelBPicks.contains(b3.doc));
    assertTrue(modelBPicks.contains(b4.doc));
  }

  protected void initIdenticalOrderRerankLists() {
    a1 = new ScoreDoc(1,10,1);
    a2 = new ScoreDoc(5,7,1);
    a3 = new ScoreDoc(4,6,1);
    a4 = new ScoreDoc(2,5,1);
    a5 = new ScoreDoc(3,4,1);
    rerankedA = new ScoreDoc[]{a1,a2,a3,a4,a5};

    b1 = new ScoreDoc(1,10,1);
    b2 = new ScoreDoc(5,7,1);
    b3 = new ScoreDoc(4,6,1);
    b4 = new ScoreDoc(2,5,1);
    b5 = new ScoreDoc(3,4,1);
    rerankedB = new ScoreDoc[]{b1,b2,b3,b4,b5};
  }

  /**
   * Random Boolean Choices Generation from Seed: [0,1,1]
   */
  @Test
  public void interleaving_identicalRerankLists_shouldInterleaveTeamDraft() {
    initIdenticalOrderRerankLists();

    InterleavingResult interleaved = toTest.interleave(rerankedA, rerankedB);
    ScoreDoc[] interleavedResults = interleaved.getInterleavedResults();

    assertThat(interleavedResults.length,is(5));

    assertThat(interleavedResults[0],is(a1));
    assertThat(interleavedResults[1],is(b2));

    assertThat(interleavedResults[2],is(b3));
    assertThat(interleavedResults[3],is(a4));

    assertThat(interleavedResults[4],is(b5));
  }

  /**
   * Random Boolean Choices Generation from Seed: [0,1,1]
   */
  @Test
  public void interleaving_identicalRerankLists_shouldBuildCorrectInterleavingPicks() {
    initIdenticalOrderRerankLists();

    InterleavingResult interleaved = toTest.interleave(rerankedA, rerankedB);

    ArrayList<Set<Integer>> interleavingPicks = interleaved.getInterleavingPicks();
    Set<Integer> modelAPicks = interleavingPicks.get(0);
    Set<Integer> modelBPicks = interleavingPicks.get(1);

    assertThat(modelAPicks.size(),is(2));
    assertThat(modelBPicks.size(),is(3));

    assertTrue(modelAPicks.contains(a1.doc));
    assertTrue(modelAPicks.contains(a4.doc));

    assertTrue(modelBPicks.contains(b2.doc));
    assertTrue(modelBPicks.contains(b3.doc));
    assertTrue(modelBPicks.contains(b5.doc));
  }

}

