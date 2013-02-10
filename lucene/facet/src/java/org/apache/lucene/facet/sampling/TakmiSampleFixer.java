package org.apache.lucene.facet.sampling;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;

import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.DrillDownQuery;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.search.ScoredDocIDs;
import org.apache.lucene.facet.search.ScoredDocIDsIterator;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

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

/**
 * Fix sampling results by counting the intersection between two lists: a
 * TermDocs (list of documents in a certain category) and a DocIdSetIterator
 * (list of documents matching the query).
 * 
 * 
 * @lucene.experimental
 */
// TODO (Facet): implement also an estimated fixing by ratio (taking into
// account "translation" of counts!)
class TakmiSampleFixer implements SampleFixer {
  
  private TaxonomyReader taxonomyReader;
  private IndexReader indexReader;
  private FacetSearchParams searchParams;
  
  public TakmiSampleFixer(IndexReader indexReader,
      TaxonomyReader taxonomyReader, FacetSearchParams searchParams) {
    this.indexReader = indexReader;
    this.taxonomyReader = taxonomyReader;
    this.searchParams = searchParams;
  }

  @Override
  public void fixResult(ScoredDocIDs origDocIds, FacetResult fres)
      throws IOException {
    FacetResultNode topRes = fres.getFacetResultNode();
    fixResultNode(topRes, origDocIds);
  }
  
  /**
   * Fix result node count, and, recursively, fix all its children
   * 
   * @param facetResNode
   *          result node to be fixed
   * @param docIds
   *          docids in effect
   * @throws IOException If there is a low-level I/O error.
   */
  private void fixResultNode(FacetResultNode facetResNode, ScoredDocIDs docIds) throws IOException {
    recount(facetResNode, docIds);
    for (FacetResultNode frn : facetResNode.subResults) {
      fixResultNode(frn, docIds);
    }
  }

  /**
   * Internal utility: recount for a facet result node
   * 
   * @param fresNode
   *          result node to be recounted
   * @param docIds
   *          full set of matching documents.
   * @throws IOException If there is a low-level I/O error.
   */
  private void recount(FacetResultNode fresNode, ScoredDocIDs docIds) throws IOException {
    // TODO (Facet): change from void to return the new, smaller docSet, and use
    // that for the children, as this will make their intersection ops faster.
    // can do this only when the new set is "sufficiently" smaller.
    
    /* We need the category's path name in order to do its recounting.
     * If it is missing, because the option to label only part of the
     * facet results was exercise, we need to calculate them anyway, so
     * in essence sampling with recounting spends some extra cycles for
     * labeling results for which labels are not required. */
    if (fresNode.label == null) {
      fresNode.label = taxonomyReader.getPath(fresNode.ordinal);
    }
    CategoryPath catPath = fresNode.label;

    Term drillDownTerm = DrillDownQuery.term(searchParams.indexingParams, catPath);
    // TODO (Facet): avoid Multi*?
    Bits liveDocs = MultiFields.getLiveDocs(indexReader);
    int updatedCount = countIntersection(MultiFields.getTermDocsEnum(indexReader, liveDocs,
                                                                     drillDownTerm.field(), drillDownTerm.bytes(),
                                                                     0), docIds.iterator());
    fresNode.value = updatedCount;
  }

  /**
   * Count the size of the intersection between two lists: a TermDocs (list of
   * documents in a certain category) and a DocIdSetIterator (list of documents
   * matching a query).
   */
  private static int countIntersection(DocsEnum p1, ScoredDocIDsIterator p2)
      throws IOException {
    // The documentation of of both TermDocs and DocIdSetIterator claim
    // that we must do next() before doc(). So we do, and if one of the
    // lists is empty, obviously return 0;
    if (p1 == null || p1.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
      return 0;
    }
    if (!p2.next()) {
      return 0;
    }
    
    int d1 = p1.docID();
    int d2 = p2.getDocID();

    int count = 0;
    for (;;) {
      if (d1 == d2) {
        ++count;
        if (p1.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
          break; // end of list 1, nothing more in intersection
        }
        d1 = p1.docID();
        if (!advance(p2, d1)) {
          break; // end of list 2, nothing more in intersection
        }
        d2 = p2.getDocID();
      } else if (d1 < d2) {
        if (p1.advance(d2) == DocIdSetIterator.NO_MORE_DOCS) {
          break; // end of list 1, nothing more in intersection
        }
        d1 = p1.docID();
      } else /* d1>d2 */ {
        if (!advance(p2, d1)) {
          break; // end of list 2, nothing more in intersection
        }
        d2 = p2.getDocID();
      }
    }
    return count;
  }

  /**
   * utility: advance the iterator until finding (or exceeding) specific
   * document
   * 
   * @param iterator
   *          iterator being advanced
   * @param targetDoc
   *          target of advancing
   * @return false if iterator exhausted, true otherwise.
   */
  private static boolean advance(ScoredDocIDsIterator iterator, int targetDoc) {
    while (iterator.next()) {
      if (iterator.getDocID() >= targetDoc) {
        return true; // target reached
      }
    }
    return false; // exhausted
  }
}