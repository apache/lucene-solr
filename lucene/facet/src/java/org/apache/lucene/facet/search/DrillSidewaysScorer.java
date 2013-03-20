package org.apache.lucene.facet.search;

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
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.FixedBitSet;

class DrillSidewaysScorer extends Scorer {

  //private static boolean DEBUG = false;

  private final Collector drillDownCollector;

  private final DocsEnumsAndFreq[] dims;

  // DrillDown DocsEnums:
  private final Scorer baseScorer;

  private final AtomicReaderContext context;

  private static final int CHUNK = 2048;
  private static final int MASK = CHUNK-1;

  private int collectDocID = -1;
  private float collectScore;

  DrillSidewaysScorer(Weight w, AtomicReaderContext context, Scorer baseScorer, Collector drillDownCollector,
                      DocsEnumsAndFreq[] dims) {
    super(w);
    this.dims = dims;
    this.context = context;
    this.baseScorer = baseScorer;
    this.drillDownCollector = drillDownCollector;
  }

  @Override
  public void score(Collector collector) throws IOException {
    //if (DEBUG) {
    //  System.out.println("\nscore: reader=" + context.reader());
    //}
    //System.out.println("score r=" + context.reader());
    collector.setScorer(this);
    drillDownCollector.setScorer(this);
    drillDownCollector.setNextReader(context);
    for(DocsEnumsAndFreq dim : dims) {
      dim.sidewaysCollector.setScorer(this);
      dim.sidewaysCollector.setNextReader(context);
    }

    // TODO: if we ever allow null baseScorer ... it will
    // mean we DO score docs out of order ... hmm, or if we
    // change up the order of the conjuntions below
    assert baseScorer != null;

    // Position all scorers to their first matching doc:
    int baseDocID = baseScorer.nextDoc();

    for(DocsEnumsAndFreq dim : dims) {
      for(DocsEnum docsEnum : dim.docsEnums) {
        if (docsEnum != null) {
          docsEnum.nextDoc();
        }
      }
    }

    final int numDims = dims.length;

    DocsEnum[][] docsEnums = new DocsEnum[numDims][];
    Collector[] sidewaysCollectors = new Collector[numDims];
    int maxFreq = 0;
    for(int dim=0;dim<numDims;dim++) {
      docsEnums[dim] = dims[dim].docsEnums;
      sidewaysCollectors[dim] = dims[dim].sidewaysCollector;
      maxFreq = Math.max(maxFreq, dims[dim].freq);
    }

    // TODO: if we add cost API to Scorer, switch to that!
    int estBaseHitCount = context.reader().maxDoc() / (1+baseDocID);

    /*
    System.out.println("\nbaseDocID=" + baseDocID + " est=" + estBaseHitCount);
    System.out.println("  maxDoc=" + context.reader().maxDoc());
    System.out.println("  maxFreq=" + maxFreq);
    System.out.println("  dims[0].freq=" + dims[0].freq);
    if (numDims > 1) {
      System.out.println("  dims[1].freq=" + dims[1].freq);
    }
    */

    if (estBaseHitCount < maxFreq/10) {
      //System.out.println("baseAdvance");
      doBaseAdvanceScoring(collector, docsEnums, sidewaysCollectors);
    } else if (numDims > 1 && (dims[1].freq < estBaseHitCount/10)) {
      //System.out.println("drillDownAdvance");
      doDrillDownAdvanceScoring(collector, docsEnums, sidewaysCollectors);
    } else {
      //System.out.println("union");
      doUnionScoring(collector, docsEnums, sidewaysCollectors);
    }
  }

  /** Used when drill downs are highly constraining vs
   *  baseQuery. */
  private void doDrillDownAdvanceScoring(Collector collector, DocsEnum[][] docsEnums, Collector[] sidewaysCollectors) throws IOException {
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
      for(DocsEnum docsEnum : docsEnums[0]) {
        if (docsEnum == null) {
          continue;
        }
        int docID = docsEnum.docID();
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

          docID = docsEnum.nextDoc();
        }
      }

      // Second dim:
      //if (DEBUG) {
      //  System.out.println("  dim1");
      //}
      for(DocsEnum docsEnum : docsEnums[1]) {
        if (docsEnum == null) {
          continue;
        }
        int docID = docsEnum.docID();
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

          docID = docsEnum.nextDoc();
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
      for(int dim=2;dim<numDims;dim++) {
        //if (DEBUG) {
        //  System.out.println("  dim=" + dim + " [" + dims[dim].dim + "]");
        //}
        for(DocsEnum docsEnum : docsEnums[dim]) {
          if (docsEnum == null) {
            continue;
          }
          int docID = docsEnum.docID();
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
            docID = docsEnum.nextDoc();
          }
        }
      }

      // Collect:
      //if (DEBUG) {
      //  System.out.println("  now collect: " + filledCount + " hits");
      //}
      for(int i=0;i<filledCount;i++) {
        int slot = filledSlots[i];
        collectDocID = docIDs[slot];
        collectScore = scores[slot];
        //if (DEBUG) {
        //  System.out.println("    docID=" + docIDs[slot] + " count=" + counts[slot]);
        //}
        if (counts[slot] == 1+numDims) {
          collectHit(collector, sidewaysCollectors);
        } else if (counts[slot] == numDims) {
          collectNearMiss(sidewaysCollectors, missingDims[slot]);
        }
      }

      if (nextChunkStart >= maxDoc) {
        break;
      }

      nextChunkStart += CHUNK;
    }
  }

  /** Used when base query is highly constraining vs the
   *  drilldowns; in this case we just .next() on base and
   *  .advance() on the dims. */
  private void doBaseAdvanceScoring(Collector collector, DocsEnum[][] docsEnums, Collector[] sidewaysCollectors) throws IOException {
    //if (DEBUG) {
    //  System.out.println("  doBaseAdvanceScoring");
    //}
    int docID = baseScorer.docID();

    final int numDims = dims.length;

    nextDoc: while (docID != NO_MORE_DOCS) {
      int failedDim = -1;
      for(int dim=0;dim<numDims;dim++) {
        // TODO: should we sort this 2nd dimension of
        // docsEnums from most frequent to least?
        boolean found = false;
        for(DocsEnum docsEnum : docsEnums[dim]) {
          if (docsEnum == null) {
            continue;
          }
          if (docsEnum.docID() < docID) {
            docsEnum.advance(docID);
          }
          if (docsEnum.docID() == docID) {
            found = true;
            break;
          }
        }
        if (!found) {
          if (failedDim != -1) {
            // More than one dim fails on this document, so
            // it's neither a hit nor a near-miss; move to
            // next doc:
            docID = baseScorer.nextDoc();
            continue nextDoc;
          } else {
            failedDim = dim;
          }
        }
      }

      collectDocID = docID;

      // TODO: we could score on demand instead since we are
      // daat here:
      collectScore = baseScorer.score();

      if (failedDim == -1) {
        collectHit(collector, sidewaysCollectors);
      } else {
        collectNearMiss(sidewaysCollectors, failedDim);
      }

      docID = baseScorer.nextDoc();
    }
  }

  private void collectHit(Collector collector, Collector[] sidewaysCollectors) throws IOException {
    //if (DEBUG) {
    //  System.out.println("      hit");
    //}

    collector.collect(collectDocID);
    drillDownCollector.collect(collectDocID);

    // TODO: we could "fix" faceting of the sideways counts
    // to do this "union" (of the drill down hits) in the
    // end instead:

    // Tally sideways counts:
    for(int dim=0;dim<sidewaysCollectors.length;dim++) {
      sidewaysCollectors[dim].collect(collectDocID);
    }
  }

  private void collectNearMiss(Collector[] sidewaysCollectors, int dim) throws IOException {
    //if (DEBUG) {
    //  System.out.println("      missingDim=" + dim);
    //}
    sidewaysCollectors[dim].collect(collectDocID);
  }

  private void doUnionScoring(Collector collector, DocsEnum[][] docsEnums, Collector[] sidewaysCollectors) throws IOException {
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
      for(DocsEnum docsEnum : docsEnums[0]) {
        if (docsEnum == null) {
          continue;
        }
        docID = docsEnum.docID();
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
          docID = docsEnum.nextDoc();
        }
      }

      for(int dim=1;dim<numDims;dim++) {
        //if (DEBUG) {
        //  System.out.println("  dim=" + dim + " [" + dims[dim].dim + "]");
        //}
        for(DocsEnum docsEnum : docsEnums[dim]) {
          if (docsEnum == null) {
            continue;
          }
          docID = docsEnum.docID();
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
            docID = docsEnum.nextDoc();
          }

          // TODO: sometimes use advance?

          /*
            int docBase = nextChunkStart - CHUNK;
            for(int i=0;i<filledCount;i++) {
              int slot = filledSlots[i];
              docID = docBase + filledSlots[i];
              if (docIDs[slot] == docID && counts[slot] >= dim) {
                // This doc is still in the running...
                int ddDocID = docsEnum.docID();
                if (ddDocID < docID) {
                  ddDocID = docsEnum.advance(docID);
                }
                if (ddDocID == docID) {
                  if (missingDims[slot] >= dim && counts[slot] == allMatchCount) {
                  //if (DEBUG) {
                  //    System.out.println("    set docID=" + docID + " count=" + (dim+2));
                   // }
                    missingDims[slot] = dim+1;
                    counts[slot] = dim+2;
                  } else {
                  //if (DEBUG) {
                  //    System.out.println("    set docID=" + docID + " missing count=" + (dim+1));
                   // }
                    counts[slot] = dim+1;
                  }
                }
              }
            }            
          */
        }
      }

      // Collect:
      //if (DEBUG) {
      //  System.out.println("  now collect: " + filledCount + " hits");
      //}
      for(int i=0;i<filledCount;i++) {
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
          collectNearMiss(sidewaysCollectors, missingDims[slot]);
        }
      }

      if (nextChunkStart >= maxDoc) {
        break;
      }

      nextChunkStart += CHUNK;
    }
  }

  @Override
  public int docID() {
    return collectDocID;
  }

  @Override
  public float score() {
    return collectScore;
  }

  @Override
  public int freq() {
    return 1+dims.length;
  }

  @Override
  public int nextDoc() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int advance(int target) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long cost() {
    return baseScorer.cost();
  }

  @Override
  public Collection<ChildScorer> getChildren() {
    return Collections.singletonList(new ChildScorer(baseScorer, "MUST"));
  }

  static class DocsEnumsAndFreq implements Comparable<DocsEnumsAndFreq> {
    DocsEnum[] docsEnums;
    // Max docFreq for all docsEnums for this dim:
    int freq;
    Collector sidewaysCollector;
    String dim;

    @Override
    public int compareTo(DocsEnumsAndFreq other) {
      return freq - other.freq;
    }
  }
}
