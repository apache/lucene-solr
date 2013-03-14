package org.apache.lucene.facet.util;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.facet.search.ScoredDocIDs;
import org.apache.lucene.facet.search.ScoredDocIDsIterator;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.OpenBitSetDISI;

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
 * Utility methods for Scored Doc IDs.
 * 
 * @lucene.experimental
 */
public class ScoredDocIdsUtils {

  /**
   * Create a complement of the input set. The returned {@link ScoredDocIDs}
   * does not contain any scores, which makes sense given that the complementing
   * documents were not scored.
   * 
   * Note: the complement set does NOT contain doc ids which are noted as deleted by the given reader
   * 
   * @param docids to be complemented.
   * @param reader holding the number of documents & information about deletions.
   */
  public final static ScoredDocIDs getComplementSet(final ScoredDocIDs docids, final IndexReader reader)
      throws IOException {
    final int maxDoc = reader.maxDoc();

    DocIdSet docIdSet = docids.getDocIDs();
    final FixedBitSet complement;
    if (docIdSet instanceof FixedBitSet) {
      // That is the most common case, if ScoredDocIdsCollector was used.
      complement = ((FixedBitSet) docIdSet).clone();
    } else {
      complement = new FixedBitSet(maxDoc);
      DocIdSetIterator iter = docIdSet.iterator();
      int doc;
      while ((doc = iter.nextDoc()) < maxDoc) {
        complement.set(doc);
      }
    }
    complement.flip(0, maxDoc);
    clearDeleted(reader, complement);

    return createScoredDocIds(complement, maxDoc);
  }
  
  /** Clear all deleted documents from a given open-bit-set according to a given reader */
  private static void clearDeleted(final IndexReader reader, final FixedBitSet set) throws IOException {
    // TODO use BitsFilteredDocIdSet?
    
    // If there are no deleted docs
    if (!reader.hasDeletions()) {
      return; // return immediately
    }
    
    DocIdSetIterator it = set.iterator();
    int doc = it.nextDoc(); 
    for (AtomicReaderContext context : reader.leaves()) {
      AtomicReader r = context.reader();
      final int maxDoc = r.maxDoc() + context.docBase;
      if (doc >= maxDoc) { // skip this segment
        continue;
      }
      if (!r.hasDeletions()) { // skip all docs that belong to this reader as it has no deletions
        while ((doc = it.nextDoc()) < maxDoc) {}
        continue;
      }
      Bits liveDocs = r.getLiveDocs();
      do {
        if (!liveDocs.get(doc - context.docBase)) {
          set.clear(doc);
        }
      } while ((doc = it.nextDoc()) < maxDoc);
    }
  }
  
  /**
   * Create a subset of an existing ScoredDocIDs object.
   * 
   * @param allDocIds orginal set
   * @param sampleSet Doc Ids of the subset.
   */
  public static final ScoredDocIDs createScoredDocIDsSubset(final ScoredDocIDs allDocIds,
      final int[] sampleSet) throws IOException {

    // sort so that we can scan docs in order
    final int[] docids = sampleSet;
    Arrays.sort(docids);
    final float[] scores = new float[docids.length];
    // fetch scores and compute size
    ScoredDocIDsIterator it = allDocIds.iterator();
    int n = 0;
    while (it.next() && n < docids.length) {
      int doc = it.getDocID();
      if (doc == docids[n]) {
        scores[n] = it.getScore();
        ++n;
      }
    }
    final int size = n;

    return new ScoredDocIDs() {

      @Override
      public DocIdSet getDocIDs() {
        return new DocIdSet() {

          @Override
          public boolean isCacheable() { return true; }

          @Override
          public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {

              private int next = -1;

              @Override
              public int advance(int target) {
                while (next < size && docids[next++] < target) {
                }
                return next == size ? NO_MORE_DOCS : docids[next];
              }

              @Override
              public int docID() {
                return docids[next];
              }

              @Override
              public int nextDoc() {
                if (++next >= size) {
                  return NO_MORE_DOCS;
                }
                return docids[next];
              }

              @Override
              public long cost() {
                return size;
              }
            };
          }
        };
      }

      @Override
      public ScoredDocIDsIterator iterator() {
        return new ScoredDocIDsIterator() {

          int next = -1;

          @Override
          public boolean next() { return ++next < size; }

          @Override
          public float getScore() { return scores[next]; }

          @Override
          public int getDocID() { return docids[next]; }
        };
      }

      @Override
      public int size() { return size; }

    };
  }

  /**
   * Creates a {@link ScoredDocIDs} which returns document IDs all non-deleted doc ids 
   * according to the given reader. 
   * The returned set contains the range of [0 .. reader.maxDoc ) doc ids
   */
  public static final ScoredDocIDs createAllDocsScoredDocIDs (final IndexReader reader) {
    if (reader.hasDeletions()) {
      return new AllLiveDocsScoredDocIDs(reader);
    }
    return new AllDocsScoredDocIDs(reader);
  }

  /**
   * Create a ScoredDocIDs out of a given docIdSet and the total number of documents in an index  
   */
  public static final ScoredDocIDs createScoredDocIds(final DocIdSet docIdSet, final int maxDoc) {
    return new ScoredDocIDs() {
      private int size = -1;
      @Override
      public DocIdSet getDocIDs() { return docIdSet; }

      @Override
      public ScoredDocIDsIterator iterator() throws IOException {
        final DocIdSetIterator docIterator = docIdSet.iterator();
        return new ScoredDocIDsIterator() {
          @Override
          public boolean next() {
            try {
              return docIterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS;
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public float getScore() { return DEFAULT_SCORE; }

          @Override
          public int getDocID() { return docIterator.docID(); }
        };
      }

      @Override
      public int size() {
        // lazy size computation
        if (size < 0) {
          OpenBitSetDISI openBitSetDISI;
          try {
            openBitSetDISI = new OpenBitSetDISI(docIdSet.iterator(), maxDoc);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          size = (int) openBitSetDISI.cardinality();
        }
        return size;
      }
    };
  }

  /**
   * All docs ScoredDocsIDs - this one is simply an 'all 1' bitset. Used when
   * there are no deletions in the index and we wish to go through each and
   * every document
   */
  private static class AllDocsScoredDocIDs implements ScoredDocIDs {
    final int maxDoc;

    public AllDocsScoredDocIDs(IndexReader reader) {
      this.maxDoc = reader.maxDoc();
    }

    @Override
    public int size() {  
      return maxDoc;
    }

    @Override
    public DocIdSet getDocIDs() {
      return new DocIdSet() {

        @Override
        public boolean isCacheable() {
          return true;
        }

        @Override
        public DocIdSetIterator iterator() {
          return new DocIdSetIterator() {
            private int next = -1;

            @Override
            public int advance(int target) {
              if (target <= next) {
                target = next + 1;
              }
              return next = target >= maxDoc ? NO_MORE_DOCS : target;
            }

            @Override
            public int docID() {
              return next;
            }

            @Override
            public int nextDoc() {
              return ++next < maxDoc ? next : NO_MORE_DOCS;
            }

            @Override
            public long cost() {
              return maxDoc;
            }
          };
        }
      };
    }

    @Override
    public ScoredDocIDsIterator iterator() {
      try {
        final DocIdSetIterator iter = getDocIDs().iterator();
        return new ScoredDocIDsIterator() {
          @Override
          public boolean next() {
            try {
              return iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS;
            } catch (IOException e) {
              // cannot happen
              return false;
            }
          }

          @Override
          public float getScore() {
            return DEFAULT_SCORE;
          }

          @Override
          public int getDocID() {
            return iter.docID();
          }
        };
      } catch (IOException e) {
        // cannot happen
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * An All-docs bitset which has '0' for deleted documents and '1' for the
   * rest. Useful for iterating over all 'live' documents in a given index.
   * <p>
   * NOTE: this class would work for indexes with no deletions at all,
   * although it is recommended to use {@link AllDocsScoredDocIDs} to ease
   * the performance cost of validating isDeleted() on each and every docId
   */
  private static final class AllLiveDocsScoredDocIDs implements ScoredDocIDs {
    final int maxDoc;
    final IndexReader reader;

    AllLiveDocsScoredDocIDs(IndexReader reader) {
      this.maxDoc = reader.maxDoc();
      this.reader = reader;
    }

    @Override
    public int size() {
      return reader.numDocs();
    }

    @Override
    public DocIdSet getDocIDs() {
      return new DocIdSet() {

        @Override
        public boolean isCacheable() {
          return true;
        }

        @Override
        public DocIdSetIterator iterator() {
          return new DocIdSetIterator() {
            final Bits liveDocs = MultiFields.getLiveDocs(reader);
            private int next = -1;

            @Override
            public int advance(int target) {
              if (target > next) {
                next = target - 1;
              }
              return nextDoc();
            }

            @Override
            public int docID() {
              return next;
            }

            @Override
            public int nextDoc() {
              do {
                ++next;
              } while (next < maxDoc && liveDocs != null && !liveDocs.get(next));

              return next < maxDoc ? next : NO_MORE_DOCS;
            }

            @Override
            public long cost() {
              return maxDoc;
            }
          };
        }
      };
    }

    @Override
    public ScoredDocIDsIterator iterator() {
      try {
        final DocIdSetIterator iter = getDocIDs().iterator();
        return new ScoredDocIDsIterator() {
          @Override
          public boolean next() {
            try {
              return iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS;
            } catch (IOException e) {
              // cannot happen
              return false;
            }
          }

          @Override
          public float getScore() {
            return DEFAULT_SCORE;
          }

          @Override
          public int getDocID() {
            return iter.docID();
          }
        };
      } catch (IOException e) {
        // cannot happen
        throw new RuntimeException(e);
      }
    }
  }
  
}