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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.solr.search.SolrIndexSearcher;


/**
 * Implements the rescoring logic. The top documents returned by solr with their
 * original scores, will be processed by a {@link LTRScoringQuery} that will assign a
 * new score to each document. The top documents will be resorted based on the
 * new score.
 * */
public class LTRRescorer extends Rescorer {

  LTRScoringQuery scoringQuery;
  public LTRRescorer(LTRScoringQuery scoringQuery) {
    this.scoringQuery = scoringQuery;
  }

  private void heapAdjust(ScoreDoc[] hits, int size, int root) {
    final ScoreDoc doc = hits[root];
    final float score = doc.score;
    int i = root;
    while (i <= ((size >> 1) - 1)) {
      final int lchild = (i << 1) + 1;
      final ScoreDoc ldoc = hits[lchild];
      final float lscore = ldoc.score;
      float rscore = Float.MAX_VALUE;
      final int rchild = (i << 1) + 2;
      ScoreDoc rdoc = null;
      if (rchild < size) {
        rdoc = hits[rchild];
        rscore = rdoc.score;
      }
      if (lscore < score) {
        if (rscore < lscore) {
          hits[i] = rdoc;
          hits[rchild] = doc;
          i = rchild;
        } else {
          hits[i] = ldoc;
          hits[lchild] = doc;
          i = lchild;
        }
      } else if (rscore < score) {
        hits[i] = rdoc;
        hits[rchild] = doc;
        i = rchild;
      } else {
        return;
      }
    }
  }

  private void heapify(ScoreDoc[] hits, int size) {
    for (int i = (size >> 1) - 1; i >= 0; i--) {
      heapAdjust(hits, size, i);
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
    if ((topN == 0) || (firstPassTopDocs.totalHits == 0)) {
      return firstPassTopDocs;
    }
    final ScoreDoc[] hits = firstPassTopDocs.scoreDocs;
    Arrays.sort(hits, new Comparator<ScoreDoc>() {
      @Override
      public int compare(ScoreDoc a, ScoreDoc b) {
        return a.doc - b.doc;
      }
    });

    topN = Math.min(topN, firstPassTopDocs.totalHits);
    final ScoreDoc[] reranked = new ScoreDoc[topN];
    final List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
    final LTRScoringQuery.ModelWeight modelWeight = (LTRScoringQuery.ModelWeight) searcher
        .createNormalizedWeight(scoringQuery, true);

    final SolrIndexSearcher solrIndexSearch = (SolrIndexSearcher) searcher;
    scoreFeatures(solrIndexSearch, firstPassTopDocs,topN, modelWeight, hits, leaves, reranked);
    // Must sort all documents that we reranked, and then select the top
    Arrays.sort(reranked, new Comparator<ScoreDoc>() {
      @Override
      public int compare(ScoreDoc a, ScoreDoc b) {
        // Sort by score descending, then docID ascending:
        if (a.score > b.score) {
          return -1;
        } else if (a.score < b.score) {
          return 1;
        } else {
          // This subtraction can't overflow int
          // because docIDs are >= 0:
          return a.doc - b.doc;
        }
      }
    });

    return new TopDocs(firstPassTopDocs.totalHits, reranked, reranked[0].score);
  }

  public void scoreFeatures(SolrIndexSearcher solrIndexSearch, TopDocs firstPassTopDocs,
      int topN, LTRScoringQuery.ModelWeight modelWeight, ScoreDoc[] hits, List<LeafReaderContext> leaves,
      ScoreDoc[] reranked) throws IOException {

    int readerUpto = -1;
    int endDoc = 0;
    int docBase = 0;

    LTRScoringQuery.ModelWeight.ModelScorer scorer = null;
    int hitUpto = 0;
    final FeatureLogger featureLogger = scoringQuery.getFeatureLogger();

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
        scorer = modelWeight.scorer(readerContext);
      }
      // Scorer for a LTRScoringQuery.ModelWeight should never be null since we always have to
      // call score
      // even if no feature scorers match, since a model might use that info to
      // return a
      // non-zero score. Same applies for the case of advancing a LTRScoringQuery.ModelWeight.ModelScorer
      // past the target
      // doc since the model algorithm still needs to compute a potentially
      // non-zero score from blank features.
      assert (scorer != null);
      final int targetDoc = docID - docBase;
      scorer.docID();
      scorer.iterator().advance(targetDoc);

      scorer.getDocInfo().setOriginalDocScore(new Float(hit.score));
      hit.score = scorer.score();
      if (hitUpto < topN) {
        reranked[hitUpto] = hit;
        // if the heap is not full, maybe I want to log the features for this
        // document
        if (featureLogger != null) {
          featureLogger.log(hit.doc, scoringQuery, solrIndexSearch,
              modelWeight.getFeaturesInfo());
        }
      } else if (hitUpto == topN) {
        // collected topN document, I create the heap
        heapify(reranked, topN);
      }
      if (hitUpto >= topN) {
        // once that heap is ready, if the score of this document is lower that
        // the minimum
        // i don't want to log the feature. Otherwise I replace it with the
        // minimum and fix the
        // heap.
        if (hit.score > reranked[0].score) {
          reranked[0] = hit;
          heapAdjust(reranked, topN, 0);
          if (featureLogger != null) {
            featureLogger.log(hit.doc, scoringQuery, solrIndexSearch,
                modelWeight.getFeaturesInfo());
          }
        }
      }
      hitUpto++;
    }
  }

  @Override
  public Explanation explain(IndexSearcher searcher,
      Explanation firstPassExplanation, int docID) throws IOException {

    final List<LeafReaderContext> leafContexts = searcher.getTopReaderContext()
        .leaves();
    final int n = ReaderUtil.subIndex(docID, leafContexts);
    final LeafReaderContext context = leafContexts.get(n);
    final int deBasedDoc = docID - context.docBase;
    final Weight modelWeight = searcher.createNormalizedWeight(scoringQuery,
        true);
    return modelWeight.explain(context, deBasedDoc);
  }

  public static LTRScoringQuery.FeatureInfo[] extractFeaturesInfo(LTRScoringQuery.ModelWeight modelWeight,
      int docid,
      Float originalDocScore,
      List<LeafReaderContext> leafContexts)
          throws IOException {
    final int n = ReaderUtil.subIndex(docid, leafContexts);
    final LeafReaderContext atomicContext = leafContexts.get(n);
    final int deBasedDoc = docid - atomicContext.docBase;
    final LTRScoringQuery.ModelWeight.ModelScorer r = modelWeight.scorer(atomicContext);
    if ( (r == null) || (r.iterator().advance(deBasedDoc) != deBasedDoc) ) {
      return new LTRScoringQuery.FeatureInfo[0];
    } else {
      if (originalDocScore != null) {
        // If results have not been reranked, the score passed in is the original query's
        // score, which some features can use instead of recalculating it
        r.getDocInfo().setOriginalDocScore(originalDocScore);
      }
      r.score();
      return modelWeight.getFeaturesInfo();
    }
  }

}
