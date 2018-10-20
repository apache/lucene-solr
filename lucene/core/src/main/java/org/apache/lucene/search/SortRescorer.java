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
package org.apache.lucene.search;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.TotalHits.Relation;

/**
 * A {@link Rescorer} that re-sorts according to a provided
 * Sort.
 */

public class SortRescorer extends Rescorer {

  private final Sort sort;

  /** Sole constructor. */
  public SortRescorer(Sort sort) {
    this.sort = sort;
  }

  @Override
  public TopDocs rescore(IndexSearcher searcher, TopDocs firstPassTopDocs, int topN) throws IOException {

    // Copy ScoreDoc[] and sort by ascending docID:
    ScoreDoc[] hits = firstPassTopDocs.scoreDocs.clone();
    Comparator<ScoreDoc> docIdComparator = Comparator.comparingInt(sd -> sd.doc);
    Arrays.sort(hits, docIdComparator);

    List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();

    TopFieldCollector collector = TopFieldCollector.create(sort, topN, Integer.MAX_VALUE);

    // Now merge sort docIDs from hits, with reader's leaves:
    int hitUpto = 0;
    int readerUpto = -1;
    int endDoc = 0;
    int docBase = 0;

    LeafCollector leafCollector = null;
    ScoreAndDoc scoreAndDoc = new ScoreAndDoc();

    while (hitUpto < hits.length) {
      ScoreDoc hit = hits[hitUpto];
      int docID = hit.doc;
      LeafReaderContext readerContext = null;
      while (docID >= endDoc) {
        readerUpto++;
        readerContext = leaves.get(readerUpto);
        endDoc = readerContext.docBase + readerContext.reader().maxDoc();
      }

      if (readerContext != null) {
        // We advanced to another segment:
        leafCollector = collector.getLeafCollector(readerContext);
        leafCollector.setScorer(scoreAndDoc);
        docBase = readerContext.docBase;
      }

      scoreAndDoc.score = hit.score;
      scoreAndDoc.doc = docID - docBase;

      leafCollector.collect(scoreAndDoc.doc);

      hitUpto++;
    }

    TopDocs rescoredDocs = collector.topDocs();
    // set scores from the original score docs
    assert hits.length == rescoredDocs.scoreDocs.length;
    ScoreDoc[] rescoredDocsClone = rescoredDocs.scoreDocs.clone();
    Arrays.sort(rescoredDocsClone, docIdComparator);
    for (int i = 0; i < rescoredDocsClone.length; ++i) {
      rescoredDocsClone[i].score = hits[i].score;
    }
    return rescoredDocs;
  }

  @Override
  public Explanation explain(IndexSearcher searcher, Explanation firstPassExplanation, int docID) throws IOException {
    TopDocs oneHit = new TopDocs(new TotalHits(1, Relation.EQUAL_TO), new ScoreDoc[] {new ScoreDoc(docID, firstPassExplanation.getValue().floatValue())});
    TopDocs hits = rescore(searcher, oneHit, 1);
    assert hits.totalHits.value == 1;

    List<Explanation> subs = new ArrayList<>();

    // Add first pass:
    Explanation first = Explanation.match(firstPassExplanation.getValue(), "first pass score", firstPassExplanation);
    subs.add(first);

    FieldDoc fieldDoc = (FieldDoc) hits.scoreDocs[0];

    // Add sort values:
    SortField[] sortFields = sort.getSort();
    for(int i=0;i<sortFields.length;i++) {
      subs.add(Explanation.match(0.0f, "sort field " + sortFields[i].toString() + " value=" + fieldDoc.fields[i]));
    }

    // TODO: if we could ask the Sort to explain itself then
    // we wouldn't need the separate ExpressionRescorer...
    return Explanation.match(0.0f, "sort field values for sort=" + sort.toString(), subs);
  }
}
