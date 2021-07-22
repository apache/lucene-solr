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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.solr.ltr.LTRRescorer;
import org.apache.solr.ltr.LTRScoringQuery;

/**
 * Implements the rescoring logic. The top documents returned by solr with their
 * original scores, will be processed by a {@link LTRScoringQuery} that will assign a
 * new score to each document. The top documents will be resorted based on the
 * new score.
 * */
public class LTRInterleavingRescorer extends LTRRescorer {
  
  final private LTRInterleavingScoringQuery[] rerankingQueries;
  private Integer originalRankingIndex = null;
  final private Interleaving interleavingAlgorithm;
  
  public LTRInterleavingRescorer( Interleaving interleavingAlgorithm, LTRInterleavingScoringQuery[] rerankingQueries) {
    this.rerankingQueries = rerankingQueries;
    this.interleavingAlgorithm = interleavingAlgorithm;
    for(int i=0;i<this.rerankingQueries.length;i++){
      if(this.rerankingQueries[i] instanceof OriginalRankingLTRScoringQuery){
        this.originalRankingIndex = i;
      }
    }
  }

  /**
   * rescores the documents:
   *
   * @param searcher
   *          current IndexSearcher
   * @param firstPassTopDocs
   *          documents to rerank;
   * @param topN
   *          documents to return;
   */
  @Override
  public TopDocs rescore(IndexSearcher searcher, TopDocs firstPassTopDocs,
      int topN) throws IOException {
    if ((topN == 0) || (firstPassTopDocs.scoreDocs.length == 0)) {
      return firstPassTopDocs;
    }

    ScoreDoc[] firstPassResults = null;
    if(originalRankingIndex != null) {
      firstPassResults = new ScoreDoc[firstPassTopDocs.scoreDocs.length];
      System.arraycopy(firstPassTopDocs.scoreDocs, 0, firstPassResults, 0, firstPassTopDocs.scoreDocs.length);
    }
    topN = Math.toIntExact(Math.min(topN, firstPassTopDocs.totalHits.value));

    ScoreDoc[][] reRankedPerModel = rerank(searcher,topN,getFirstPassDocsRanked(firstPassTopDocs));
    if (originalRankingIndex != null) {
      reRankedPerModel[originalRankingIndex] = firstPassResults;
    }
    InterleavingResult interleaved = interleavingAlgorithm.interleave(reRankedPerModel[0], reRankedPerModel[1]);
    ScoreDoc[] interleavedResults = interleaved.getInterleavedResults();
    
    ArrayList<Set<Integer>> interleavingPicks = interleaved.getInterleavingPicks();
    rerankingQueries[0].setPickedInterleavingDocIds(interleavingPicks.get(0));
    rerankingQueries[1].setPickedInterleavingDocIds(interleavingPicks.get(1));

    return new TopDocs(firstPassTopDocs.totalHits, interleavedResults);
  }

  private ScoreDoc[][] rerank(IndexSearcher searcher, int topN, ScoreDoc[] firstPassResults) throws IOException {
    ScoreDoc[][] reRankedPerModel = new ScoreDoc[rerankingQueries.length][topN];
    final List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
    LTRScoringQuery.ModelWeight[] modelWeights = new LTRScoringQuery.ModelWeight[rerankingQueries.length];
    for (int i = 0; i < rerankingQueries.length; i++) {
      if (originalRankingIndex == null || originalRankingIndex != i) {
        modelWeights[i] = (LTRScoringQuery.ModelWeight) searcher
            .createWeight(searcher.rewrite(rerankingQueries[i]), ScoreMode.COMPLETE, 1);
      }
    }
    scoreFeatures(searcher, topN, modelWeights, firstPassResults, leaves, reRankedPerModel);

    for (int i = 0; i < rerankingQueries.length; i++) {
      if (originalRankingIndex == null || originalRankingIndex != i) {
        Arrays.sort(reRankedPerModel[i], scoreComparator);
      }
    }

    return reRankedPerModel;
  }

  public void scoreFeatures(IndexSearcher indexSearcher,
                            int topN, LTRScoringQuery.ModelWeight[] modelWeights, ScoreDoc[] hits, List<LeafReaderContext> leaves,
                            ScoreDoc[][] rerankedPerModel) throws IOException {

    int readerUpto = -1;
    int endDoc = 0;
    int docBase = 0;
    int hitUpto = 0;
    LTRScoringQuery.ModelWeight.ModelScorer[] scorers = new LTRScoringQuery.ModelWeight.ModelScorer[rerankingQueries.length];
    while (hitUpto < hits.length) {
      final ScoreDoc hit = hits[hitUpto];
      final int docID = hit.doc;
      LeafReaderContext readerContext = null;
      while (docID >= endDoc) {
        readerUpto++;
        readerContext = leaves.get(readerUpto);
        endDoc = readerContext.docBase + readerContext.reader().maxDoc();
      }

      // We advanced to another segment
      if (readerContext != null) {
        docBase = readerContext.docBase;
        for (int i = 0; i < modelWeights.length; i++) {
          if (modelWeights[i] != null) {
            scorers[i] = modelWeights[i].scorer(readerContext);
          }
        }
      }
      for (int i = 0; i < rerankingQueries.length; i++) {
        if (modelWeights[i] != null) {
          final ScoreDoc hit_i = new ScoreDoc(hit.doc, hit.score, hit.shardIndex);
          if (scoreSingleHit(topN, docBase, hitUpto, hit_i, docID, scorers[i], rerankedPerModel[i])) {
            logSingleHit(indexSearcher, modelWeights[i], hit_i.doc, rerankingQueries[i]);
          }
        }
      }
      hitUpto++;
    }

  }
  
  @Override
  public Explanation explain(IndexSearcher searcher,
                             Explanation firstPassExplanation, int docID) throws IOException {
    LTRScoringQuery pickedRerankModel = rerankingQueries[0];
    if (rerankingQueries[1].getPickedInterleavingDocIds().contains(docID)) {
      pickedRerankModel = rerankingQueries[1];
    }
    return getExplanation(searcher, docID, pickedRerankModel);
  }

}
