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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.search.ScoreDoc;
import org.apache.solr.ltr.interleaving.Interleaving;
import org.apache.solr.ltr.interleaving.InterleavingResult;

/**
 * Interleaving was introduced the first time by Joachims in [1, 2].
 * Team Draft Interleaving is among the most successful and used interleaving approaches[3].
 * Team Draft Interleaving implements a method similar to the way in which captains select their players in team-matches.
 * Team Draft Interleaving produces a fair distribution of ranking models’ elements in the final interleaved list.
 * "Team draft interleaving" has also proved to overcome an issue of the "Balanced interleaving" approach, in determining the winning model[4].
 * <p>
 * [1] T. Joachims. Optimizing search engines using clickthrough data. KDD (2002)
 * [2] T.Joachims.Evaluatingretrievalperformanceusingclickthroughdata.InJ.Franke, G. Nakhaeizadeh, and I. Renz, editors,
 * Text Mining, pages 79–96. Physica/Springer (2003)
 * [3] F. Radlinski, M. Kurup, and T. Joachims. How does clickthrough data reflect re-
 * trieval quality? In CIKM, pages 43–52. ACM Press (2008)
 * [4] O. Chapelle, T. Joachims, F. Radlinski, and Y. Yue.
 * Large-scale validation and analysis of interleaved search evaluation. ACM TOIS, 30(1):1–41, Feb. (2012)
 */
public class TeamDraftInterleaving implements Interleaving {
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

  /**
   * Team Draft Interleaving considers two ranking models: modelA and modelB.
   * For a given query, each model returns its ranked list of documents La = (a1,a2,...) and Lb = (b1, b2, ...).
   * The algorithm creates a unique ranked list I = (i1, i2, ...).
   * This list is created by interleaving elements from the two lists la and lb as described by Chapelle et al.[1].
   * Each element Ij is labelled TeamA if it is selected from La and TeamB if it is selected from Lb.
   * <p>
   * [1] O. Chapelle, T. Joachims, F. Radlinski, and Y. Yue.
   * Large-scale validation and analysis of interleaved search evaluation. ACM TOIS, 30(1):1–41, Feb. (2012)
   * <p>
   * Assumptions:
   * - rerankedA and rerankedB has the same length.
   * They contains the same search results, ranked differently by two ranking models
   * - each reranked list can not contain the same search result more than once.
   * - results are all from the same shard
   *
   * @param rerankedA a ranked list of search results produced by a ranking model A
   * @param rerankedB a ranked list of search results produced by a ranking model B
   * @return the interleaved ranking list
   */
  public InterleavingResult interleave(ScoreDoc[] rerankedA, ScoreDoc[] rerankedB) {
    LinkedList<ScoreDoc> interleavedResults = new LinkedList<>();
    HashSet<Integer> alreadyAdded = new HashSet<>();
    ScoreDoc[] interleavedResultArray = new ScoreDoc[rerankedA.length];
    ArrayList<Set<Integer>> interleavingPicks = new ArrayList<>(2);
    Set<Integer> teamA = new HashSet<>();
    Set<Integer> teamB = new HashSet<>();
    int topN = rerankedA.length;
    int indexA = 0, indexB = 0;

    while (interleavedResults.size() < topN && indexA < rerankedA.length && indexB < rerankedB.length) {
      if(teamA.size()<teamB.size() || (teamA.size()==teamB.size() && !RANDOM.nextBoolean())){
        indexA = updateIndex(alreadyAdded, indexA, rerankedA);
        interleavedResults.add(rerankedA[indexA]);
        alreadyAdded.add(rerankedA[indexA].doc);
        teamA.add(rerankedA[indexA].doc);
        indexA++;
      } else{
        indexB = updateIndex(alreadyAdded,indexB,rerankedB);
        interleavedResults.add(rerankedB[indexB]);
        alreadyAdded.add(rerankedB[indexB].doc);
        teamB.add(rerankedB[indexB].doc);
        indexB++;
      }
    }
    interleavingPicks.add(teamA);
    interleavingPicks.add(teamB);
    interleavedResultArray = interleavedResults.toArray(interleavedResultArray);

    return new InterleavingResult(interleavedResultArray,interleavingPicks);
  }

  private int updateIndex(HashSet<Integer> alreadyAdded, int index, ScoreDoc[] reranked) {
    boolean foundElementToAdd = false;
    while (index < reranked.length && !foundElementToAdd) {
      ScoreDoc elementToCheck = reranked[index];
      if (alreadyAdded.contains(elementToCheck.doc)) {
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
