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

package org.apache.lucene.concordance.charoffsets;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;


/**
 * Utility class to crawl spans.
 */
public class SpansCrawler {

  /**
   *
   * @param query span query to use
   * @param filter filter
   * @param searcher searcher
   * @param visitor visitor to call for each span
   * @throws IOException on IOException
   * @throws TargetTokenNotFoundException if the visitor can't find the target token
   */
  public static void crawl(SpanQuery query, Query filter, IndexSearcher searcher,
                           DocTokenOffsetsVisitor visitor) throws IOException, TargetTokenNotFoundException {

    query = (SpanQuery) query.rewrite(searcher.getIndexReader());

    SpanWeight w = query.createWeight(searcher, false, 1.0f);
    if (filter == null) {
      for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {

        Spans spans = w.getSpans(ctx, SpanWeight.Postings.POSITIONS);
        if (spans == null) {
          continue;
        }
        boolean cont = visitLeafReader(ctx, spans, visitor);
        if (!cont) {
          break;
        }
      }
    } else {
      filter = searcher.rewrite(filter);
      Weight searcherWeight = searcher.createWeight(filter, false, 1.0f);
      for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
        Scorer leafReaderContextScorer = searcherWeight.scorer(ctx);
        if (leafReaderContextScorer == null) {
          continue;
        }
        //Can we tell from the scorer that there were no hits?
        //in <= 5.x we could stop here if the filter query had no hits.

        Spans spans = w.getSpans(ctx, SpanWeight.Postings.POSITIONS);
        if (spans == null) {
          continue;
        }
        DocIdSetIterator filterItr = leafReaderContextScorer.iterator();

        if (filterItr == null || filterItr.equals(DocIdSetIterator.empty())) {
          continue;
        }
        boolean cont = visitLeafReader(ctx, spans, filterItr, visitor);
        if (!cont) {
          break;
        }
      }
    }
  }

  static boolean visitLeafReader(LeafReaderContext leafCtx,
                                     Spans spans, DocIdSetIterator filterItr, DocTokenOffsetsVisitor visitor) throws IOException, TargetTokenNotFoundException {
    int filterDoc = -1;
    int spansDoc = spans.nextDoc();
    while (true) {
      if (spansDoc == DocIdSetIterator.NO_MORE_DOCS) {
        break;
      }
      filterDoc = filterItr.advance(spansDoc);
      if (filterDoc == DocIdSetIterator.NO_MORE_DOCS) {
        break;
      } else if (filterDoc > spansDoc) {
        while (spansDoc <= filterDoc) {
          spansDoc = spans.nextDoc();
          if (spansDoc == filterDoc) {
            boolean cont = visit(leafCtx, spans, visitor);
            if (! cont) {
              return false;
            }

          } else {
            continue;
          }
        }
      } else if (filterDoc == spansDoc) {
        boolean cont = visit(leafCtx, spans, visitor);
        if (! cont) {
          return false;
        }
        //then iterate spans
        spansDoc = spans.nextDoc();
      } else if (filterDoc < spansDoc) {
        throw new IllegalArgumentException("FILTER doc is < spansdoc!!!");
      } else {
        throw new IllegalArgumentException("Something horrible happened");
      }
    }
    return true;
  }

  static boolean visitLeafReader(LeafReaderContext leafCtx,
                                        Spans spans,
                                        DocTokenOffsetsVisitor visitor) throws IOException, TargetTokenNotFoundException {
    while (spans.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      boolean cont = visit(leafCtx, spans, visitor);
      if (! cont) {
        return false;
      }
    }
    return true;
  }


  static boolean visit(LeafReaderContext leafCtx, Spans spans, DocTokenOffsetsVisitor visitor) throws IOException, TargetTokenNotFoundException {
    Document document = leafCtx.reader().document(spans.docID(), visitor.getFields());
    DocTokenOffsets offsets = visitor.getDocTokenOffsets();
    offsets.reset(leafCtx.docBase, spans.docID(), document);
    while (spans.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
      offsets.addOffset(spans.startPosition(), spans.endPosition());
    }
    return visitor.visit(offsets);
  }

}
