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
package org.apache.solr.ltr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;


/**
 * Implements the rescoring logic. The top documents returned by solr with their
 * original scores, will be processed by a {@link LTRScoringQuery} that will assign a
 * new score to each document. The top documents will be resorted based on the
 * new score.
 * */
public class LTRInterleavingRescorer extends LTRRescorer {
  
  LTRScoringQuery[] rarankingModels;
  
  public LTRInterleavingRescorer(LTRScoringQuery[] rarankingModels) {
    this.rarankingModels = rarankingModels;
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
    final ScoreDoc[] firstPassResults = getFirstPassDocsRanked(firstPassTopDocs);
    topN = Math.toIntExact(Math.min(topN, firstPassTopDocs.totalHits.value));
    final List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();

    ScoreDoc[][] reRankedPerModel = new ScoreDoc[rarankingModels.length][topN];
    for(int i = 0; i< rarankingModels.length; i++){
      reRankedPerModel[i] = rerank(searcher, rarankingModels[i], topN, firstPassResults, leaves);
    }
    ScoreDoc[] reRanked = reRankedPerModel[0];
    if(rarankingModels.length>1){//Interleaving
      
    }
    return new TopDocs(firstPassTopDocs.totalHits, reRanked);
  }

  @Override
  public Explanation explain(IndexSearcher searcher,
      Explanation firstPassExplanation, int docID) throws IOException {

    final List<LeafReaderContext> leafContexts = searcher.getTopReaderContext()
        .leaves();
    final int n = ReaderUtil.subIndex(docID, leafContexts);
    final LeafReaderContext context = leafContexts.get(n);
    final int deBasedDoc = docID - context.docBase;
    List<Explanation> modelsExplanations = new ArrayList<>(rarankingModels.length);
    for(LTRScoringQuery reRankingModel: rarankingModels){
      final Weight modelWeight = searcher.createWeight(searcher.rewrite(reRankingModel),
          ScoreMode.COMPLETE, 1);
      modelsExplanations.add(modelWeight.explain(context, deBasedDoc));
    }
    if(rarankingModels.length>1) {
      return Explanation.match(0, toString() + " score from model X has been chosen by Interleaving", modelsExplanations);//0 must be the real score chosen
    } else {
      return modelsExplanations.get(0);
    }
  }

}
