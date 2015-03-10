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

package org.apache.solr.request;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.uninverting.DocTermOrds;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;

/**
 *
 * Final form of the un-inverted field:
 *   Each document points to a list of term numbers that are contained in that document.
 *
 *   Term numbers are in sorted order, and are encoded as variable-length deltas from the
 *   previous term number.  Real term numbers start at 2 since 0 and 1 are reserved.  A
 *   term number of 0 signals the end of the termNumber list.
 *
 *   There is a single int[maxDoc()] which either contains a pointer into a byte[] for
 *   the termNumber lists, or directly contains the termNumber list if it fits in the 4
 *   bytes of an integer.  If the first byte in the integer is 1, the next 3 bytes
 *   are a pointer into a byte[] where the termNumber list starts.
 *
 *   There are actually 256 byte arrays, to compensate for the fact that the pointers
 *   into the byte arrays are only 3 bytes long.  The correct byte array for a document
 *   is a function of its id.
 *
 *   To save space and speed up faceting, any term that matches enough documents will
 *   not be un-inverted... it will be skipped while building the un-inverted field structure,
 *   and will use a set intersection method during faceting.
 *
 *   To further save memory, the terms (the actual string values) are not all stored in
 *   memory, but a TermIndex is used to convert term numbers to term values only
 *   for the terms needed after faceting has completed.  Only every 128th term value
 *   is stored, along with its corresponding term number, and this is used as an
 *   index to find the closest term and iterate until the desired number is hit (very
 *   much like Lucene's own internal term index).
 *
 */
public class UnInvertedField extends DocTermOrds {

  static class TopTerm {
    BytesRef term;
    int termNum;

    long memSize() {
      return 8 +   // obj header
             8 + 8 +term.length +  //term
             4;    // int
    }
  }

  long memsz;
  final AtomicLong use = new AtomicLong(); // number of uses

  int[] maxTermCounts = new int[1024];

  final Map<Integer,TopTerm> bigTerms = new LinkedHashMap<>();

  private SolrIndexSearcher.DocsEnumState deState;
  private final SolrIndexSearcher searcher;

  private static UnInvertedField uifPlaceholder = new UnInvertedField();

  private UnInvertedField() { // Dummy for synchronization.
    super("fake", 0, 0); // cheapest initialization I can find.
    searcher = null;
   }

  @Override
  protected void visitTerm(TermsEnum te, int termNum) throws IOException {

    if (termNum >= maxTermCounts.length) {
      // resize by doubling - for very large number of unique terms, expanding
      // by 4K and resultant GC will dominate uninvert times.  Resize at end if material
      int[] newMaxTermCounts = new int[maxTermCounts.length*2];
      System.arraycopy(maxTermCounts, 0, newMaxTermCounts, 0, termNum);
      maxTermCounts = newMaxTermCounts;
    }

    final BytesRef term = te.term();

    if (te.docFreq() > maxTermDocFreq) {
      TopTerm topTerm = new TopTerm();
      topTerm.term = BytesRef.deepCopyOf(term);
      topTerm.termNum = termNum;
      bigTerms.put(topTerm.termNum, topTerm);

      if (deState == null) {
        deState = new SolrIndexSearcher.DocsEnumState();
        deState.fieldName = field;
        deState.liveDocs = searcher.getLeafReader().getLiveDocs();
        deState.termsEnum = te;  // TODO: check for MultiTermsEnum in SolrIndexSearcher could now fail?
        deState.postingsEnum = postingsEnum;
        deState.minSetSizeCached = maxTermDocFreq;
      }

      postingsEnum = deState.postingsEnum;
      DocSet set = searcher.getDocSet(deState);
      maxTermCounts[termNum] = set.size();
    }
  }

  @Override
  protected void setActualDocFreq(int termNum, int docFreq) {
    maxTermCounts[termNum] = docFreq;
  }

  public long memSize() {
    // can cache the mem size since it shouldn't change
    if (memsz!=0) return memsz;
    long sz = super.ramBytesUsed();
    sz += 8*8 + 32; // local fields
    sz += bigTerms.size() * 64;
    for (TopTerm tt : bigTerms.values()) {
      sz += tt.memSize();
    }
    if (maxTermCounts != null)
      sz += maxTermCounts.length * 4;
    if (indexedTermsArray != null) {
      // assume 8 byte references?
      sz += 8+8+8+8+(indexedTermsArray.length<<3)+sizeOfIndexedStrings;
    }
    memsz = sz;
    return sz;
  }

  public UnInvertedField(String field, SolrIndexSearcher searcher) throws IOException {
    super(field,
          // threshold, over which we use set intersections instead of counting
          // to (1) save memory, and (2) speed up faceting.
          // Add 2 for testing purposes so that there will always be some terms under
          // the threshold even when the index is very
          // small.
          searcher.maxDoc()/20 + 2,
          DEFAULT_INDEX_INTERVAL_BITS);
    //System.out.println("maxTermDocFreq=" + maxTermDocFreq + " maxDoc=" + searcher.maxDoc());

    final String prefix = TrieField.getMainValuePrefix(searcher.getSchema().getFieldType(field));
    this.searcher = searcher;
    try {
      LeafReader r = searcher.getLeafReader();
      uninvert(r, r.getLiveDocs(), prefix == null ? null : new BytesRef(prefix));
    } catch (IllegalStateException ise) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ise.getMessage());
    }
    if (tnums != null) {
      for(byte[] target : tnums) {
        if (target != null && target.length > (1<<24)*.9) {
          SolrCore.log.warn("Approaching too many values for UnInvertedField faceting on field '"+field+"' : bucket size=" + target.length);
        }
      }
    }

    // free space if outrageously wasteful (tradeoff memory/cpu) 
    if ((maxTermCounts.length - numTermsInField) > 1024) { // too much waste!
      int[] newMaxTermCounts = new int[numTermsInField];
      System.arraycopy(maxTermCounts, 0, newMaxTermCounts, 0, numTermsInField);
      maxTermCounts = newMaxTermCounts;
    }

    SolrCore.log.info("UnInverted multi-valued field " + toString());
    //System.out.println("CREATED: " + toString() + " ti.index=" + ti.index);
  }

  public int getNumTerms() {
    return numTermsInField;
  }

  String getReadableValue(BytesRef termval, FieldType ft, CharsRefBuilder charsRef) {
    return ft.indexedToReadable(termval, charsRef).toString();
  }

  /** may return a reused BytesRef */
  BytesRef getTermValue(TermsEnum te, int termNum) throws IOException {
    //System.out.println("getTermValue termNum=" + termNum + " this=" + this + " numTerms=" + numTermsInField);
    if (bigTerms.size() > 0) {
      // see if the term is one of our big terms.
      TopTerm tt = bigTerms.get(termNum);
      if (tt != null) {
        //System.out.println("  return big " + tt.term);
        return tt.term;
      }
    }

    return lookupTerm(te, termNum);
  }

  @Override
  public String toString() {
    final long indexSize = indexedTermsArray == null ? 0 : (8+8+8+8+(indexedTermsArray.length<<3)+sizeOfIndexedStrings); // assume 8 byte references?
    return "{field=" + field
            + ",memSize="+memSize()
            + ",tindexSize="+indexSize
            + ",time="+total_time
            + ",phase1="+phase1_time
            + ",nTerms="+numTermsInField
            + ",bigTerms="+bigTerms.size()
            + ",termInstances="+termInstances
            + ",uses="+use.get()
            + "}";
  }

  //////////////////////////////////////////////////////////////////
  //////////////////////////// caching /////////////////////////////
  //////////////////////////////////////////////////////////////////

  public static UnInvertedField getUnInvertedField(String field, SolrIndexSearcher searcher) throws IOException {
    SolrCache<String,UnInvertedField> cache = searcher.getFieldValueCache();
    if (cache == null) {
      return new UnInvertedField(field, searcher);
    }
    UnInvertedField uif = null;
    Boolean doWait = false;
    synchronized (cache) {
      uif = cache.get(field);
      if (uif == null) {
        /**
         * We use this place holder object to pull the UninvertedField construction out of the sync
         * so that if many fields are accessed in a short time, the UninvertedField can be
         * built for these fields in parallel rather than sequentially.
         */
        cache.put(field, uifPlaceholder);
      } else {
        if (uif != uifPlaceholder) {
          return uif;
        }
        doWait = true; // Someone else has put the place holder in, wait for that to complete.
      }
    }
    while (doWait) {
      try {
        synchronized (cache) {
          uif = cache.get(field); // Should at least return the placeholder, NPE if not is OK.
          if (uif != uifPlaceholder) { // OK, another thread put this in the cache we should be good.
            return uif;
          }
          cache.wait();
        }
      } catch (InterruptedException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Thread interrupted in getUninvertedField.");
      }
    }

    uif = new UnInvertedField(field, searcher);
    synchronized (cache) {
      cache.put(field, uif); // Note, this cleverly replaces the placeholder.
      cache.notifyAll();
    }

    return uif;
  }
}
