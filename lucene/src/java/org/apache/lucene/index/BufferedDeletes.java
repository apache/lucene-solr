package org.apache.lucene.index;

/**
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
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Date;
import java.util.Map.Entry;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/** Holds a {@link SegmentDeletes} for each segment in the
 *  index. */

class BufferedDeletes {

  // Deletes for all flushed/merged segments:
  private final Map<SegmentInfo,SegmentDeletes> deletesMap = new HashMap<SegmentInfo,SegmentDeletes>();

  // used only by assert
  private Term lastDeleteTerm;
  
  private PrintStream infoStream;
  private final AtomicLong bytesUsed = new AtomicLong();
  private final AtomicInteger numTerms = new AtomicInteger();
  private final int messageID;

  public BufferedDeletes(int messageID) {
    this.messageID = messageID;
  }

  private synchronized void message(String message) {
    if (infoStream != null) {
      infoStream.println("BD " + messageID + " [" + new Date() + "; " + Thread.currentThread().getName() + "]: BD " + message);
    }
  }
  
  public synchronized void setInfoStream(PrintStream infoStream) {
    this.infoStream = infoStream;
  }

  public synchronized void pushDeletes(SegmentDeletes newDeletes, SegmentInfo info) {
    pushDeletes(newDeletes, info, false);
  }

  // Moves all pending deletes onto the provided segment,
  // then clears the pending deletes
  public synchronized void pushDeletes(SegmentDeletes newDeletes, SegmentInfo info, boolean noLimit) {
    assert newDeletes.any();
    numTerms.addAndGet(newDeletes.numTermDeletes.get());

    if (!noLimit) {
      assert !deletesMap.containsKey(info);
      assert info != null;
      deletesMap.put(info, newDeletes);
      bytesUsed.addAndGet(newDeletes.bytesUsed.get());
    } else {
      final SegmentDeletes deletes = getDeletes(info);
      bytesUsed.addAndGet(-deletes.bytesUsed.get());
      deletes.update(newDeletes, noLimit);
      bytesUsed.addAndGet(deletes.bytesUsed.get());
    }    
    if (infoStream != null) {
      message("push deletes seg=" + info + " dels=" + getDeletes(info));
    }
    assert checkDeleteStats();    
  }

  public synchronized void clear() {
    deletesMap.clear();
    numTerms.set(0);
    bytesUsed.set(0);
  }

  synchronized boolean any() {
    return bytesUsed.get() != 0;
  }

  public int numTerms() {
    return numTerms.get();
  }

  public long bytesUsed() {
    return bytesUsed.get();
  }

  // IW calls this on finishing a merge.  While the merge
  // was running, it's possible new deletes were pushed onto
  // our last (and only our last) segment.  In this case we
  // must carry forward those deletes onto the merged
  // segment.
  synchronized void commitMerge(MergePolicy.OneMerge merge) {
    assert checkDeleteStats();
    if (infoStream != null) {
      message("commitMerge merge.info=" + merge.info + " merge.segments=" + merge.segments);
    }
    final SegmentInfo lastInfo = merge.segments.lastElement();
    final SegmentDeletes lastDeletes = deletesMap.get(lastInfo);
    if (lastDeletes != null) {
      deletesMap.remove(lastInfo);
      assert !deletesMap.containsKey(merge.info);
      deletesMap.put(merge.info, lastDeletes);
      // don't need to update numTerms/bytesUsed since we
      // are just moving the deletes from one info to
      // another
      if (infoStream != null) {
        message("commitMerge done: new deletions=" + lastDeletes);
      }
    } else if (infoStream != null) {
      message("commitMerge done: no new deletions");
    }
    assert !anyDeletes(merge.segments.range(0, merge.segments.size()-1));
    assert checkDeleteStats();
  }

  synchronized void clear(SegmentDeletes deletes) {
    deletes.clear();
  }
  
  public synchronized boolean applyDeletes(IndexWriter.ReaderPool readerPool, SegmentInfos segmentInfos, SegmentInfos applyInfos) throws IOException {
    if (!any()) {
      return false;
    }
    final long t0 = System.currentTimeMillis();

    if (infoStream != null) {
      message("applyDeletes: applyInfos=" + applyInfos + "; index=" + segmentInfos);
    }

    assert checkDeleteStats();

    assert applyInfos.size() > 0;

    boolean any = false;
    
    final SegmentInfo lastApplyInfo = applyInfos.lastElement();
    final int lastIdx = segmentInfos.indexOf(lastApplyInfo);
    
    final SegmentInfo firstInfo = applyInfos.firstElement();
    final int firstIdx = segmentInfos.indexOf(firstInfo);

    // applyInfos must be a slice of segmentInfos
    assert lastIdx - firstIdx + 1 == applyInfos.size();
    
    // iterate over all segment infos backwards
    // coalesceing deletes along the way 
    // when we're at or below the last of the 
    // segments to apply to, start applying the deletes
    // we traverse up to the first apply infos
    SegmentDeletes coalescedDeletes = null;
    boolean hasDeletes = false;
    for (int segIdx=segmentInfos.size()-1; segIdx >= firstIdx; segIdx--) {
      final SegmentInfo info = segmentInfos.info(segIdx);
      final SegmentDeletes deletes = deletesMap.get(info);
      assert deletes == null || deletes.any();

      if (deletes == null && coalescedDeletes == null) {
        continue;
      }

      if (infoStream != null) {
        message("applyDeletes: seg=" + info + " segment's deletes=[" + (deletes == null ? "null" : deletes) + "]; coalesced deletes=[" + (coalescedDeletes == null ? "null" : coalescedDeletes) + "]");
      }

      hasDeletes |= deletes != null;

      if (segIdx <= lastIdx && hasDeletes) {

        final long delCountInc = applyDeletes(readerPool, info, coalescedDeletes, deletes);

        if (delCountInc != 0) {
          any = true;
        }
        if (infoStream != null) {
          message("deletes touched " + delCountInc + " docIDs");
        }
      
        if (deletes != null) {
          // we've applied doc ids, and they're only applied
          // on the current segment
          bytesUsed.addAndGet(-deletes.docIDs.size() * SegmentDeletes.BYTES_PER_DEL_DOCID);
          deletes.clearDocIDs();
        }
      }
      
      // now coalesce at the max limit
      if (deletes != null) {
        if (coalescedDeletes == null) {
          coalescedDeletes = new SegmentDeletes();
        }
        // TODO: we could make this single pass (coalesce as
        // we apply the deletes
        coalescedDeletes.update(deletes, true);
      }
    }

    // move all deletes to segment just before our merge.
    if (firstIdx > 0) {

      SegmentDeletes mergedDeletes = null;
      // TODO: we could also make this single pass
      for (SegmentInfo info : applyInfos) {
        final SegmentDeletes deletes = deletesMap.get(info);
        if (deletes != null) {
          assert deletes.any();
          if (mergedDeletes == null) {
            mergedDeletes = getDeletes(segmentInfos.info(firstIdx-1));
            numTerms.addAndGet(-mergedDeletes.numTermDeletes.get());
            assert numTerms.get() >= 0;
            bytesUsed.addAndGet(-mergedDeletes.bytesUsed.get());
            assert bytesUsed.get() >= 0;
          }

          mergedDeletes.update(deletes, true);
        }
      }

      if (mergedDeletes != null) {
        numTerms.addAndGet(mergedDeletes.numTermDeletes.get());
        bytesUsed.addAndGet(mergedDeletes.bytesUsed.get());
      }

      if (infoStream != null) {
        if (mergedDeletes != null) {
          message("applyDeletes: merge all deletes into seg=" + segmentInfos.info(firstIdx-1) + ": " + mergedDeletes);
        } else {
          message("applyDeletes: no deletes to merge");
        }
      }
    } else {
      // We drop the deletes in this case, because we've
      // applied them to segment infos starting w/ the first
      // segment.  There are no prior segments so there's no
      // reason to keep them around.  When the applyInfos ==
      // segmentInfos this means all deletes have been
      // removed:
    }
    remove(applyInfos);

    assert checkDeleteStats();
    assert applyInfos != segmentInfos || !any();
    
    if (infoStream != null) {
      message("applyDeletes took " + (System.currentTimeMillis()-t0) + " msec");
    }
    return any;
  }
  
  private synchronized long applyDeletes(IndexWriter.ReaderPool readerPool,
                                            SegmentInfo info, 
                                            SegmentDeletes coalescedDeletes,
                                            SegmentDeletes segmentDeletes) throws IOException {    
    assert readerPool.infoIsLive(info);
    
    assert coalescedDeletes == null || coalescedDeletes.docIDs.size() == 0;
    
    long delCount = 0;

    // Lock order: IW -> BD -> RP
    SegmentReader reader = readerPool.get(info, false);
    try {
      if (coalescedDeletes != null) {
        delCount += applyDeletes(coalescedDeletes, reader);
      }
      if (segmentDeletes != null) {
        delCount += applyDeletes(segmentDeletes, reader);
      }
    } finally {
      readerPool.release(reader);
    }
    return delCount;
  }
  
  private synchronized long applyDeletes(SegmentDeletes deletes, SegmentReader reader) throws IOException {

    long delCount = 0;

    assert checkDeleteTerm(null);
    
    if (deletes.terms.size() > 0) {
      Fields fields = reader.fields();
      if (fields == null) {
        // This reader has no postings
        return 0;
      }

      TermsEnum termsEnum = null;
        
      String currentField = null;
      DocsEnum docs = null;
        
      for (Entry<Term,Integer> entry: deletes.terms.entrySet()) {
        Term term = entry.getKey();
        // Since we visit terms sorted, we gain performance
        // by re-using the same TermsEnum and seeking only
        // forwards
        if (term.field() != currentField) {
          assert currentField == null || currentField.compareTo(term.field()) < 0;
          currentField = term.field();
          Terms terms = fields.terms(currentField);
          if (terms != null) {
            termsEnum = terms.iterator();
          } else {
            termsEnum = null;
          }
        }
          
        if (termsEnum == null) {
          continue;
        }
        assert checkDeleteTerm(term);
          
        if (termsEnum.seek(term.bytes(), false) == TermsEnum.SeekStatus.FOUND) {
          DocsEnum docsEnum = termsEnum.docs(reader.getDeletedDocs(), docs);
            
          if (docsEnum != null) {
            docs = docsEnum;
            final int limit = entry.getValue();
            while (true) {
              final int docID = docs.nextDoc();
              if (docID == DocsEnum.NO_MORE_DOCS || docID >= limit) {
                break;
              }
              reader.deleteDocument(docID);
              // TODO: we could/should change
              // reader.deleteDocument to return boolean
              // true if it did in fact delete, because here
              // we could be deleting an already-deleted doc
              // which makes this an upper bound:
              delCount++;
            }
          }
        }
      }
    }

    // Delete by docID
    for (Integer docIdInt : deletes.docIDs) {
      int docID = docIdInt.intValue();
      reader.deleteDocument(docID);
      delCount++;
    }

    // Delete by query
    if (deletes.queries.size() > 0) {
      IndexSearcher searcher = new IndexSearcher(reader);
      assert searcher.getTopReaderContext().isAtomic;
      final AtomicReaderContext readerContext = (AtomicReaderContext) searcher.getTopReaderContext();
      try {
        for (Entry<Query, Integer> entry : deletes.queries.entrySet()) {
          Query query = entry.getKey();
          int limit = entry.getValue().intValue();
          Weight weight = query.weight(searcher);
          Scorer scorer = weight.scorer(readerContext, Weight.ScorerContext.def());
          if (scorer != null) {
            while(true)  {
              int doc = scorer.nextDoc();
              if (doc >= limit)
                break;

              reader.deleteDocument(doc);
              // TODO: we could/should change
              // reader.deleteDocument to return boolean
              // true if it did in fact delete, because here
              // we could be deleting an already-deleted doc
              // which makes this an upper bound:
              delCount++;
            }
          }
        }
      } finally {
        searcher.close();
      }
    }

    return delCount;
  }
  
  public synchronized SegmentDeletes getDeletes(SegmentInfo info) {
    SegmentDeletes deletes = deletesMap.get(info);
    if (deletes == null) {
      deletes = new SegmentDeletes();
      deletesMap.put(info, deletes);
    }
    return deletes;
  }
  
  public synchronized void remove(SegmentInfos infos) {
    assert infos.size() > 0;
    for (SegmentInfo info : infos) {
      SegmentDeletes deletes = deletesMap.get(info);
      if (deletes != null) {
        bytesUsed.addAndGet(-deletes.bytesUsed.get());
        assert bytesUsed.get() >= 0: "bytesUsed=" + bytesUsed;
        numTerms.addAndGet(-deletes.numTermDeletes.get());
        assert numTerms.get() >= 0: "numTerms=" + numTerms;
        deletesMap.remove(info);
      }
    }
  }

  // used only by assert
  private boolean anyDeletes(SegmentInfos infos) {
    for(SegmentInfo info : infos) {
      if (deletesMap.containsKey(info)) {
        return true;
      }
    }
    return false;
  }

  // used only by assert
  private boolean checkDeleteTerm(Term term) {
    if (term != null) {
      assert lastDeleteTerm == null || term.compareTo(lastDeleteTerm) > 0: "lastTerm=" + lastDeleteTerm + " vs term=" + term;
    }
    lastDeleteTerm = term;
    return true;
  }
  
  // only for assert
  private boolean checkDeleteStats() {
    int numTerms2 = 0;
    long bytesUsed2 = 0;
    for(SegmentDeletes deletes : deletesMap.values()) {
      numTerms2 += deletes.numTermDeletes.get();
      bytesUsed2 += deletes.bytesUsed.get();
    }
    assert numTerms2 == numTerms.get(): "numTerms2=" + numTerms2 + " vs " + numTerms.get();
    assert bytesUsed2 == bytesUsed.get(): "bytesUsed2=" + bytesUsed2 + " vs " + bytesUsed;
    return true;
  }
}
