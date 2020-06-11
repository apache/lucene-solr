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

import java.util.LinkedHashSet;
import java.util.Random;

import org.apache.lucene.search.ScoreDoc;

public class TeamDraftInterleaving implements Interleaving{
  private Random generator = new Random();
  
  public ScoreDoc[] interleave(ScoreDoc[] rerankedA, ScoreDoc[] rerankedB){
    LinkedHashSet<ScoreDoc> interleaved = new LinkedHashSet<>();
    ScoreDoc[] interleavedArray = new ScoreDoc[rerankedA.length];
    int topN = rerankedA.length;
    int indexA = 0, indexB = 0;
    int teamA = 0, teamB = 0;
    
    while(interleaved.size()<topN && (indexA<rerankedA.length) && (indexB<rerankedB.length)){
      if(teamA<teamB || (teamA==teamB && generator.nextBoolean())){
        interleaved.add(rerankedA[indexA]);
        indexA++;
        teamA++;
        indexA = updateIndex(interleaved,indexA,rerankedA);
      } else{
        interleaved.add(rerankedB[indexB]);
        indexB++;
        teamB++;
        indexB = updateIndex(interleaved,indexB,rerankedB);
      }
    }
    
    return interleaved.toArray(interleavedArray);
  }

  private int updateIndex(LinkedHashSet<ScoreDoc> interleaved, int index, ScoreDoc[] reranked) {
    boolean foundElementToAdd = false;
    while (index < reranked.length && !foundElementToAdd) {
      ScoreDoc elementToCheck = reranked[index];
      if (interleaved.contains(elementToCheck)) {
        index++;
      } else {
        foundElementToAdd = true;
      }
    }
    return index;
  }

  public void setGenerator(Random generator) {
    this.generator = generator;
  }
}
