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

package org.apache.solr.ltr.interleaving;


import java.util.Random;

import org.apache.lucene.search.ScoreDoc;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

public class TeamDraftInterleavingTest {
  private TeamDraftInterleaving toTest = new TeamDraftInterleaving();
  
  @Before
  public void setup() throws Exception {
    toTest.setGenerator(new Random(1));
  }


  /**
   * Random Generation from Seed: [1,0,0]
   */
  @Test
  public void interleaving_sameIdScoreDocs_shouldInterleaveThemConsideringShardIndex() {
    ScoreDoc a1 = new ScoreDoc(1,10,1);
    ScoreDoc a2 = new ScoreDoc(5,7,1);
    ScoreDoc a3 = new ScoreDoc(3,6,1);
    ScoreDoc a4 = new ScoreDoc(1,5,2);
    ScoreDoc a5 = new ScoreDoc(3,4,2);
    ScoreDoc[] rerankedA = new ScoreDoc[]{a1,a2,a3,a4,a5};

    ScoreDoc b1 = new ScoreDoc(5,10,1);
    ScoreDoc b2 = new ScoreDoc(1,7,2);
    ScoreDoc b3 = new ScoreDoc(3,6,1);
    ScoreDoc b4 = new ScoreDoc(1,5,1);
    ScoreDoc b5 = new ScoreDoc(3,4,2);
    ScoreDoc[] rerankedB = new ScoreDoc[]{b1,b2,b3,b4,b5};

    ScoreDoc[] interleavedResults = toTest.interleave(rerankedA, rerankedB);
    assertThat(interleavedResults.length,is(5));
    assertThat(interleavedResults[0],is(a1));
    assertThat(interleavedResults[1],is(b1));
    assertThat(interleavedResults[2],is(b2));
    assertThat(interleavedResults[3],is(a3));
    assertThat(interleavedResults[4],is(b5));
    
  }
  

}

