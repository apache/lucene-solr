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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.search.ScoreDoc;

public class TeamDraftInterleaving implements Interleaving{
  public static Random RANDOM;

  static {
    // We try to make things reproducible in the context of our tests by initializing the random instance
    // based on the current seed
    String seed = System.getProperty("tests.seed");
    if (seed == null) {
      RANDOM = new Random();
    } else {
      RANDOM = new Random(seed.hashCode());
    }
  }  
  
  public InterleavingResult interleave(ScoreDoc[] rerankedA, ScoreDoc[] rerankedB){
    LinkedHashSet<ScoreDoc> interleavedResults = new LinkedHashSet<>();
    ScoreDoc[] interleavedResultArray = new ScoreDoc[rerankedA.length];
    ArrayList<Set<Integer>> interleavingPicks = new ArrayList<>(rerankedA.length);
    Set<Integer> teamA = new HashSet<>();
    Set<Integer> teamB = new HashSet<>();
    int topN = rerankedA.length;
    int indexA = 0, indexB = 0;
    
    while(interleavedResults.size()<topN && (indexA<rerankedA.length) && (indexB<rerankedB.length)){
      if(teamA.size()<teamB.size() || (teamA.size()==teamB.size() && !RANDOM.nextBoolean())){
        indexA = updateIndex(interleavedResults,indexA,rerankedA);
        interleavedResults.add(rerankedA[indexA]);
        teamA.add(rerankedA[indexA].doc);
        indexA++;
      } else{
        indexB = updateIndex(interleavedResults,indexB,rerankedB);
        interleavedResults.add(rerankedB[indexB]);
        teamB.add(rerankedB[indexB].doc);
        indexB++;
      }
    }
    interleavingPicks.add(teamA);
    interleavingPicks.add(teamB);
    interleavedResultArray = interleavedResults.toArray(interleavedResultArray);
    
    return new InterleavingResult(interleavedResultArray,interleavingPicks);
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

  public static void setRANDOM(Random RANDOM) {
    TeamDraftInterleaving.RANDOM = RANDOM;
  }
}
