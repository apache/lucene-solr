package org.apache.solr.ltr.ranking;

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
import org.apache.solr.ltr.log.FeatureLogger;
import org.apache.solr.ltr.ranking.ModelQuery.ModelWeight;
import org.apache.solr.ltr.ranking.ModelQuery.ModelWeight.ModelScorer;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Implements the rescoring logic. The top documents returned by solr with their
 * original scores, will be processed by a {@link ModelQuery} that will assign a
 * new score to each document. The top documents will be resorted based on the
 * new score.
 * */
public class LTRRescorer extends Rescorer {

  ModelQuery reRankModel;
  public static final String ORIGINAL_DOC_NAME = "ORIGINAL_DOC_SCORE";

  public LTRRescorer(ModelQuery reRankModel) {
    this.reRankModel = reRankModel;
  }

  private void heapAdjust(ScoreDoc[] hits, int size, int root) {
    ScoreDoc doc = hits[root];
    float score = doc.score;
    int i = root;
    while (i <= (size >> 1) - 1) {
      int lchild = (i << 1) + 1;
      ScoreDoc ldoc = hits[lchild];
      float lscore = ldoc.score;
      float rscore = Float.MAX_VALUE;
      int rchild = (i << 1) + 2;
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
    if (topN == 0 || firstPassTopDocs.totalHits == 0) {
      return firstPassTopDocs;
    }

    ScoreDoc[] hits = firstPassTopDocs.scoreDocs;

    Arrays.sort(hits, new Comparator<ScoreDoc>() {
      @Override
      public int compare(ScoreDoc a, ScoreDoc b) {
        return a.doc - b.doc;
      }
    });

    topN = Math.min(topN, firstPassTopDocs.totalHits);
    ScoreDoc[] reranked = new ScoreDoc[topN];
    String[] featureNames;
    float[] featureValues;
    boolean[] featuresUsed;

    List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();

    int readerUpto = -1;
    int endDoc = 0;
    int docBase = 0;

    ModelScorer scorer = null;
    int hitUpto = 0;

    ModelWeight modelWeight = (ModelWeight) searcher.createNormalizedWeight(
        reRankModel, true);
    FeatureLogger featureLogger = reRankModel.getFeatureLogger();

    // FIXME: I dislike that we have no gaurentee this is actually a
    // SolrIndexReader.
    // We should do something about that
    SolrIndexSearcher solrIndexSearch = (SolrIndexSearcher) searcher;

    // FIXME
    // All of this heap code is only for logging. Wrap all this code in
    // 1 outer if (fl != null) so we can skip heap stuff if the request doesn't
    // call for a feature vector.
    //
    // that could be done but it would require a new vector of size $rerank,
    // that in the end we would have to sort, while using the heap, also if
    // we do not log, in the end we sort a smaller array of topN results (that
    // is the heap array).
    // The heap is just anticipating the sorting of the array, so I don't think
    // it would
    // save time.

    while (hitUpto < hits.length) {
      ScoreDoc hit = hits[hitUpto];
      int docID = hit.doc;

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

      // Scorer for a ModelWeight should never be null since we always have to
      // call score
      // even if no feature scorers match, since a model might use that info to
      // return a
      // non-zero score. Same applies for the case of advancing a ModelScorer
      // past the target
      // doc since the model algorithm still needs to compute a potentially
      // non-zero score from blank features.
      assert (scorer != null);
      int targetDoc = docID - docBase;
      int actualDoc = scorer.docID();
      actualDoc = scorer.iterator().advance(targetDoc);

      scorer.setDocInfoParam(ORIGINAL_DOC_NAME, new Float(hit.score));
      hit.score = scorer.score();
      featureNames = modelWeight.allFeatureNames;
      featureValues = modelWeight.allFeatureValues;
      featuresUsed = modelWeight.allFeaturesUsed;

      if (hitUpto < topN) {
        reranked[hitUpto] = hit;
        // if the heap is not full, maybe I want to log the features for this
        // document
        if (featureLogger != null) {
          featureLogger.log(hit.doc, reRankModel, solrIndexSearch,
              featureNames, featureValues, featuresUsed);
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
            featureLogger.log(hit.doc, reRankModel, solrIndexSearch,
                featureNames, featureValues, featuresUsed);
          }
        }
      }

      hitUpto++;
    }

    // Must sort all documents that we reranked, and then select the top N

    // ScoreDoc[] reranked = heap.getArray();
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

    // if (topN < hits.length) {
    // ScoreDoc[] subset = new ScoreDoc[topN];
    // System.arraycopy(hits, 0, subset, 0, topN);
    // hits = subset;
    // }

    return new TopDocs(firstPassTopDocs.totalHits, reranked, reranked[0].score);
  }

  @Override
  public Explanation explain(IndexSearcher searcher,
      Explanation firstPassExplanation, int docID) throws IOException {

    List<LeafReaderContext> leafContexts = searcher.getTopReaderContext()
        .leaves();
    int n = ReaderUtil.subIndex(docID, leafContexts);
    final LeafReaderContext context = leafContexts.get(n);
    int deBasedDoc = docID - context.docBase;
    Weight modelWeight = searcher.createNormalizedWeight(reRankModel, true);
    return modelWeight.explain(context, deBasedDoc);
  }

}
