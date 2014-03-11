package org.apache.lucene.facet;

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
import java.util.Collection;
import java.util.Collections;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

class DrillSidewaysScorer extends BulkScorer {

  //private static boolean DEBUG = false;

  private final Collector drillDownCollector;

  private final DocsAndCost[] dims;

  // DrillDown DocsEnums:
  private final Scorer baseScorer;

  private final AtomicReaderContext context;

  final boolean scoreSubDocsAtOnce;

  private static final int CHUNK = 2048;
  private static final int MASK = CHUNK-1;

  private int collectDocID = -1;
  private float collectScore;

  DrillSidewaysScorer(AtomicReaderContext context, Scorer baseScorer, Collector drillDownCollector,
                      DocsAndCost[] dims, boolean scoreSubDocsAtOnce) {
    this.dims = dims;
    this.context = context;
    this.baseScorer = baseScorer;
    this.drillDownCollector = drillDownCollector;
    this.scoreSubDocsAtOnce = scoreSubDocsAtOnce;
  }

  @Override
  public boolean score(Collector collector, int maxDoc) throws IOException {
    if (maxDoc != Integer.MAX_VALUE) {
      throw new IllegalArgumentException("maxDoc must be Integer.MAX_VALUE");
    }
    //if (DEBUG) {
    //  System.out.println("\nscore: reader=" + context.reader());
    //}
    //System.out.println("score r=" + context.reader());
    FakeScorer scorer = new FakeScorer();
    collector.setScorer(scorer);
    if (drillDownCollector != null) {
      drillDownCollector.setScorer(scorer);
      drillDownCollector.setNextReader(context);
    }
    for (DocsAndCost dim : dims) {
      dim.sidewaysCollector.setScorer(scorer);
      dim.sidewaysCollector.setNextReader(context);
    }

    // TODO: if we ever allow null baseScorer ... it will
    // mean we DO score docs out of order ... hmm, or if we
    // change up the order of the conjuntions below
    assert baseScorer != null;

    // Position all scorers to their first matching doc:
    baseScorer.nextDoc();
    int numBits = 0;
    for (DocsAndCost dim : dims) {
      if (dim.disi != null) {
        dim.disi.nextDoc();
      } else if (dim.bits != null) {
        numBits++;
      }
    }

    final int numDims = dims.length;

    Bits[] bits = new Bits[numBits];
    Collector[] bitsSidewaysCollectors = new Collector[numBits];

    DocIdSetIterator[] disis = new DocIdSetIterator[numDims-numBits];
    Collector[] sidewaysCollectors = new Collector[numDims-numBits];
    long drillDownCost = 0;
    int disiUpto = 0;
    int bitsUpto = 0;
    for (int dim=0;dim<numDims;dim++) {
      DocIdSetIterator disi = dims[dim].disi;
      if (dims[dim].bits == null) {
        disis[disiUpto] = disi;
        sidewaysCollectors[disiUpto] = dims[dim].sidewaysCollector;
        disiUpto++;
        if (disi != null) {
          drillDownCost += disi.cost();
        }
      } else {
        bits[bitsUpto] = dims[dim].bits;
        bitsSidewaysCollectors[bitsUpto] = dims[dim].sidewaysCollector;
        bitsUpto++;
      }
    }

    long baseQueryCost = baseScorer.cost();

    /*
    System.out.println("\nbaseDocID=" + baseScorer.docID() + " est=" + estBaseHitCount);
    System.out.println("  maxDoc=" + context.reader().maxDoc());
    System.out.println("  maxCost=" + maxCost);
    System.out.println("  dims[0].freq=" + dims[0].freq);
    if (numDims > 1) {
      System.out.println("  dims[1].freq=" + dims[1].freq);
    }
    */

    if (bitsUpto > 0 || scoreSubDocsAtOnce || baseQueryCost < drillDownCost/10) {
      //System.out.println("queryFirst: baseScorer=" + baseScorer + " disis.length=" + disis.length + " bits.length=" + bits.length);
      doQueryFirstScoring(collector, disis, sidewaysCollectors, bits, bitsSidewaysCollectors);
    } else if (numDims > 1 && (dims[1].disi == null || dims[1].disi.cost() < baseQueryCost/10)) {
      //System.out.println("drillDownAdvance");
      doDrillDownAdvanceScoring(collector, disis, sidewaysCollectors);
    } else {
      //System.out.println("union");
      doUnionScoring(collector, disis, sidewaysCollectors);
    }

    return false;
  }

  /** Used when base query is highly constraining vs the
   *  drilldowns, or when the docs must be scored at once
   *  (i.e., like BooleanScorer2, not BooleanScorer).  In
   *  this case we just .next() on base and .advance() on
   *  the dim filters. */ 
  private void doQueryFirstScoring(Collector collector, DocIdSetIterator[] disis, Collector[] sidewaysCollectors,
                                   Bits[] bits, Collector[] bitsSidewaysCollectors) throws IOException {
    //if (DEBUG) {
    //  System.out.println("  doQueryFirstScoring");
    //}
    int docID = baseScorer.docID();

    nextDoc: while (docID != DocsEnum.NO_MORE_DOCS) {
      Collector failedCollector = null;
      for (int i=0;i<disis.length;i++) {
        // TODO: should we sort this 2nd dimension of
        // docsEnums from most frequent to least?
        DocIdSetIterator disi = disis[i];
        if (disi != null && disi.docID() < docID) {
          disi.advance(docID);
        }
        if (disi == null || disi.docID() > docID) {
          if (failedCollector != null) {
            // More than one dim fails on this document, so
            // it's neither a hit nor a near-miss; move to
            // next doc:
            docID = baseScorer.nextDoc();
            continue nextDoc;
          } else {
            failedCollector = sidewaysCollectors[i];
          }
        }
      }

      // TODO: for the "non-costly Bits" we really should
      // have passed them down as acceptDocs, but
      // unfortunately we cannot distinguish today betwen
      // "bits() is so costly that you should apply it last"
      // from "bits() is so cheap that you should apply it
      // everywhere down low"

      // Fold in Filter Bits last, since they may be costly:
      for(int i=0;i<bits.length;i++) {
        if (bits[i].get(docID) == false) {
          if (failedCollector != null) {
            // More than one dim fails on this document, so
            // it's neither a hit nor a near-miss; move to
            // next doc:
            docID = baseScorer.nextDoc();
            continue nextDoc;
          } else {
            failedCollector = bitsSidewaysCollectors[i];
          }
        }
      }

      collectDocID = docID;

      // TODO: we could score on demand instead since we are
      // daat here:
      collectScore = baseScorer.score();

      if (failedCollector == null) {
        // Hit passed all filters, so it's "real":
        collectHit(collector, sidewaysCollectors, bitsSidewaysCollectors);
      } else {
        // Hit missed exactly one filter:
        collectNearMiss(failedCollector);
      }

      docID = baseScorer.nextDoc();
    }
  }

  /** Used when drill downs are highly constraining vs
   *  baseQuery. */
  private void doDrillDownAdvanceScoring(Collector collector, DocIdSetIterator[] disis, Collector[] sidewaysCollectors) throws IOException {
    final int maxDoc = context.reader().maxDoc();
    final int numDims = dims.length;

    //if (DEBUG) {
    //  System.out.println("  doDrillDownAdvanceScoring");
    //}

    // TODO: maybe a class like BS, instead of parallel arrays
    int[] filledSlots = new int[CHUNK];
    int[] docIDs = new int[CHUNK];
    float[] scores = new float[CHUNK];
    int[] missingDims = new int[CHUNK];
    int[] counts = new int[CHUNK];

    docIDs[0] = -1;
    int nextChunkStart = CHUNK;

    final FixedBitSet seen = new FixedBitSet(CHUNK);

    while (true) {
      //if (DEBUG) {
      //  System.out.println("\ncycle nextChunkStart=" + nextChunkStart + " docIds[0]=" + docIDs[0]);
      //}

      // First dim:
      //if (DEBUG) {
      //  System.out.println("  dim0");
      //}
      DocIdSetIterator disi = disis[0];
      if (disi != null) {
        int docID = disi.docID();
        while (docID < nextChunkStart) {
          int slot = docID & MASK;

          if (docIDs[slot] != docID) {
            seen.set(slot);
            // Mark slot as valid:
            //if (DEBUG) {
            //  System.out.println("    set docID=" + docID + " id=" + context.reader().document(docID).get("id"));
            //}
            docIDs[slot] = docID;
            missingDims[slot] = 1;
            counts[slot] = 1;
          }

          docID = disi.nextDoc();
        }
      }
      
      // Second dim:
      //if (DEBUG) {
      //  System.out.println("  dim1");
      //}
      disi = disis[1];
      if (disi != null) {
        int docID = disi.docID();
        while (docID < nextChunkStart) {
          int slot = docID & MASK;

          if (docIDs[slot] != docID) {
            // Mark slot as valid:
            seen.set(slot);
            //if (DEBUG) {
            //  System.out.println("    set docID=" + docID + " missingDim=0 id=" + context.reader().document(docID).get("id"));
            //}
            docIDs[slot] = docID;
            missingDims[slot] = 0;
            counts[slot] = 1;
          } else {
            // TODO: single-valued dims will always be true
            // below; we could somehow specialize
            if (missingDims[slot] >= 1) {
              missingDims[slot] = 2;
              counts[slot] = 2;
              //if (DEBUG) {
              //  System.out.println("    set docID=" + docID + " missingDim=2 id=" + context.reader().document(docID).get("id"));
              //}
            } else {
              counts[slot] = 1;
              //if (DEBUG) {
              //  System.out.println("    set docID=" + docID + " missingDim=" + missingDims[slot] + " id=" + context.reader().document(docID).get("id"));
              //}
            }
          }

          docID = disi.nextDoc();
        }
      }

      // After this we can "upgrade" to conjunction, because
      // any doc not seen by either dim 0 or dim 1 cannot be
      // a hit or a near miss:

      //if (DEBUG) {
      //  System.out.println("  baseScorer");
      //}

      // Fold in baseScorer, using advance:
      int filledCount = 0;
      int slot0 = 0;
      while (slot0 < CHUNK && (slot0 = seen.nextSetBit(slot0)) != -1) {
        int ddDocID = docIDs[slot0];
        assert ddDocID != -1;

        int baseDocID = baseScorer.docID();
        if (baseDocID < ddDocID) {
          baseDocID = baseScorer.advance(ddDocID);
        }
        if (baseDocID == ddDocID) {
          //if (DEBUG) {
          //  System.out.println("    keep docID=" + ddDocID + " id=" + context.reader().document(ddDocID).get("id"));
          //}
          scores[slot0] = baseScorer.score();
          filledSlots[filledCount++] = slot0;
          counts[slot0]++;
        } else {
          //if (DEBUG) {
          //  System.out.println("    no docID=" + ddDocID + " id=" + context.reader().document(ddDocID).get("id"));
          //}
          docIDs[slot0] = -1;

          // TODO: we could jump slot0 forward to the
          // baseDocID ... but we'd need to set docIDs for
          // intervening slots to -1
        }
        slot0++;
      }
      seen.clear(0, CHUNK);

      if (filledCount == 0) {
        if (nextChunkStart >= maxDoc) {
          break;
        }
        nextChunkStart += CHUNK;
        continue;
      }
      
      // TODO: factor this out & share w/ union scorer,
      // except we start from dim=2 instead:
      for (int dim=2;dim<numDims;dim++) {
        //if (DEBUG) {
        //  System.out.println("  dim=" + dim + " [" + dims[dim].dim + "]");
        //}
        disi = disis[dim];
        if (disi != null) {
          int docID = disi.docID();
          while (docID < nextChunkStart) {
            int slot = docID & MASK;
            if (docIDs[slot] == docID && counts[slot] >= dim) {
              // TODO: single-valued dims will always be true
              // below; we could somehow specialize
              if (missingDims[slot] >= dim) {
                //if (DEBUG) {
                //  System.out.println("    set docID=" + docID + " count=" + (dim+2));
                //}
                missingDims[slot] = dim+1;
                counts[slot] = dim+2;
              } else {
                //if (DEBUG) {
                //  System.out.println("    set docID=" + docID + " missing count=" + (dim+1));
                //}
                counts[slot] = dim+1;
              }
            }

            // TODO: sometimes use advance?
            docID = disi.nextDoc();
          }
        }
      }

      // Collect:
      //if (DEBUG) {
      //  System.out.println("  now collect: " + filledCount + " hits");
      //}
      for (int i=0;i<filledCount;i++) {
        int slot = filledSlots[i];
        collectDocID = docIDs[slot];
        collectScore = scores[slot];
        //if (DEBUG) {
        //  System.out.println("    docID=" + docIDs[slot] + " count=" + counts[slot]);
        //}
        if (counts[slot] == 1+numDims) {
          collectHit(collector, sidewaysCollectors);
        } else if (counts[slot] == numDims) {
          collectNearMiss(sidewaysCollectors[missingDims[slot]]);
        }
      }

      if (nextChunkStart >= maxDoc) {
        break;
      }

      nextChunkStart += CHUNK;
    }
  }

  private void doUnionScoring(Collector collector, DocIdSetIterator[] disis, Collector[] sidewaysCollectors) throws IOException {
    //if (DEBUG) {
    //  System.out.println("  doUnionScoring");
    //}

    final int maxDoc = context.reader().maxDoc();
    final int numDims = dims.length;

    // TODO: maybe a class like BS, instead of parallel arrays
    int[] filledSlots = new int[CHUNK];
    int[] docIDs = new int[CHUNK];
    float[] scores = new float[CHUNK];
    int[] missingDims = new int[CHUNK];
    int[] counts = new int[CHUNK];

    docIDs[0] = -1;

    // NOTE: this is basically a specialized version of
    // BooleanScorer, to the minShouldMatch=N-1 case, but
    // carefully tracking which dimension failed to match

    int nextChunkStart = CHUNK;

    while (true) {
      //if (DEBUG) {
      //  System.out.println("\ncycle nextChunkStart=" + nextChunkStart + " docIds[0]=" + docIDs[0]);
      //}
      int filledCount = 0;
      int docID = baseScorer.docID();
      //if (DEBUG) {
      //  System.out.println("  base docID=" + docID);
      //}
      while (docID < nextChunkStart) {
        int slot = docID & MASK;
        //if (DEBUG) {
        //  System.out.println("    docIDs[slot=" + slot + "]=" + docID + " id=" + context.reader().document(docID).get("id"));
        //}

        // Mark slot as valid:
        assert docIDs[slot] != docID: "slot=" + slot + " docID=" + docID;
        docIDs[slot] = docID;
        scores[slot] = baseScorer.score();
        filledSlots[filledCount++] = slot;
        missingDims[slot] = 0;
        counts[slot] = 1;

        docID = baseScorer.nextDoc();
      }

      if (filledCount == 0) {
        if (nextChunkStart >= maxDoc) {
          break;
        }
        nextChunkStart += CHUNK;
        continue;
      }

      // First drill-down dim, basically adds SHOULD onto
      // the baseQuery:
      //if (DEBUG) {
      //  System.out.println("  dim=0 [" + dims[0].dim + "]");
      //}
      DocIdSetIterator disi = disis[0];
      if (disi != null) {
        docID = disi.docID();
        //if (DEBUG) {
        //  System.out.println("    start docID=" + docID);
        //}
        while (docID < nextChunkStart) {
          int slot = docID & MASK;
          if (docIDs[slot] == docID) {
            //if (DEBUG) {
            //  System.out.println("      set docID=" + docID + " count=2");
            //}
            missingDims[slot] = 1;
            counts[slot] = 2;
          }
          docID = disi.nextDoc();
        }
      }

      for (int dim=1;dim<numDims;dim++) {
        //if (DEBUG) {
        //  System.out.println("  dim=" + dim + " [" + dims[dim].dim + "]");
        //}

        disi = disis[dim];
        if (disi != null) {
          docID = disi.docID();
          //if (DEBUG) {
          //  System.out.println("    start docID=" + docID);
          //}
          while (docID < nextChunkStart) {
            int slot = docID & MASK;
            if (docIDs[slot] == docID && counts[slot] >= dim) {
              // This doc is still in the running...
              // TODO: single-valued dims will always be true
              // below; we could somehow specialize
              if (missingDims[slot] >= dim) {
                //if (DEBUG) {
                //  System.out.println("      set docID=" + docID + " count=" + (dim+2));
                //}
                missingDims[slot] = dim+1;
                counts[slot] = dim+2;
              } else {
                //if (DEBUG) {
                //  System.out.println("      set docID=" + docID + " missing count=" + (dim+1));
                //}
                counts[slot] = dim+1;
              }
            }
            docID = disi.nextDoc();
          }
        }
      }

      // Collect:
      //System.out.println("  now collect: " + filledCount + " hits");
      for (int i=0;i<filledCount;i++) {
        // NOTE: This is actually in-order collection,
        // because we only accept docs originally returned by
        // the baseScorer (ie that Scorer is AND'd)
        int slot = filledSlots[i];
        collectDocID = docIDs[slot];
        collectScore = scores[slot];
        //if (DEBUG) {
        //  System.out.println("    docID=" + docIDs[slot] + " count=" + counts[slot]);
        //}
        //System.out.println("  collect doc=" + collectDocID + " main.freq=" + (counts[slot]-1) + " main.doc=" + collectDocID + " exactCount=" + numDims);
        if (counts[slot] == 1+numDims) {
          //System.out.println("    hit");
          collectHit(collector, sidewaysCollectors);
        } else if (counts[slot] == numDims) {
          //System.out.println("    sw");
          collectNearMiss(sidewaysCollectors[missingDims[slot]]);
        }
      }

      if (nextChunkStart >= maxDoc) {
        break;
      }

      nextChunkStart += CHUNK;
    }
  }

  private void collectHit(Collector collector, Collector[] sidewaysCollectors) throws IOException {
    //if (DEBUG) {
    //  System.out.println("      hit");
    //}

    collector.collect(collectDocID);
    if (drillDownCollector != null) {
      drillDownCollector.collect(collectDocID);
    }

    // TODO: we could "fix" faceting of the sideways counts
    // to do this "union" (of the drill down hits) in the
    // end instead:

    // Tally sideways counts:
    for (int dim=0;dim<sidewaysCollectors.length;dim++) {
      sidewaysCollectors[dim].collect(collectDocID);
    }
  }

  private void collectHit(Collector collector, Collector[] sidewaysCollectors, Collector[] sidewaysCollectors2) throws IOException {
    //if (DEBUG) {
    //  System.out.println("      hit");
    //}

    collector.collect(collectDocID);
    if (drillDownCollector != null) {
      drillDownCollector.collect(collectDocID);
    }

    // TODO: we could "fix" faceting of the sideways counts
    // to do this "union" (of the drill down hits) in the
    // end instead:

    // Tally sideways counts:
    for (int i=0;i<sidewaysCollectors.length;i++) {
      sidewaysCollectors[i].collect(collectDocID);
    }
    for (int i=0;i<sidewaysCollectors2.length;i++) {
      sidewaysCollectors2[i].collect(collectDocID);
    }
  }

  private void collectNearMiss(Collector sidewaysCollector) throws IOException {
    //if (DEBUG) {
    //  System.out.println("      missingDim=" + dim);
    //}
    sidewaysCollector.collect(collectDocID);
  }

  private final class FakeScorer extends Scorer {
    float score;
    int doc;

    public FakeScorer() {
      super(null);
    }
    
    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException("FakeScorer doesn't support advance(int)");
    }

    @Override
    public int docID() {
      return collectDocID;
    }

    @Override
    public int freq() {
      return 1+dims.length;
    }

    @Override
    public int nextDoc() {
      throw new UnsupportedOperationException("FakeScorer doesn't support nextDoc()");
    }
    
    @Override
    public float score() {
      return collectScore;
    }

    @Override
    public long cost() {
      return baseScorer.cost();
    }

    @Override
    public Collection<ChildScorer> getChildren() {
      return Collections.singletonList(new ChildScorer(baseScorer, "MUST"));
    }

    @Override
    public Weight getWeight() {
      throw new UnsupportedOperationException();
    }
  }

  static class DocsAndCost implements Comparable<DocsAndCost> {
    // Iterator for docs matching this dim's filter, or ...
    DocIdSetIterator disi;
    // Random access bits:
    Bits bits;
    Collector sidewaysCollector;
    String dim;

    @Override
    public int compareTo(DocsAndCost other) {
      if (disi == null) {
        if (other.disi == null) {
          return 0;
        } else {
          return 1;
        }
      } else if (other.disi == null) {
        return -1;
      } else if (disi.cost() < other.disi.cost()) {
        return -1;
      } else if (disi.cost() > other.disi.cost()) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}
