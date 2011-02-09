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

package org.apache.solr.request;

import org.apache.lucene.search.FieldCache;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.PagedBytes;
import org.apache.noggit.CharArr;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;

import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.*;
import org.apache.solr.util.ByteUtils;
import org.apache.solr.util.LongPriorityQueue;
import org.apache.solr.util.PrimUtils;
import org.apache.solr.handler.component.StatsValues;
import org.apache.solr.handler.component.FieldFacetStats;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Comparator;

import java.util.concurrent.atomic.AtomicLong;

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
 *   is a function of it's id.
 *
 *   To save space and speed up faceting, any term that matches enough documents will
 *   not be un-inverted... it will be skipped while building the un-inverted field structure,
 *   and will use a set intersection method during faceting.
 *
 *   To further save memory, the terms (the actual string values) are not all stored in
 *   memory, but a TermIndex is used to convert term numbers to term values only
 *   for the terms needed after faceting has completed.  Only every 128th term value
 *   is stored, along with it's corresponding term number, and this is used as an
 *   index to find the closest term and iterate until the desired number is hit (very
 *   much like Lucene's own internal term index).
 *
 */
public class UnInvertedField {
  private static int TNUM_OFFSET=2;

  static class TopTerm {
    BytesRef term;
    int termNum;

    long memSize() {
      return 8 +   // obj header
             8 + 8 +term.length +  //term
             4;    // int
    }
  }

  String field;
  int numTermsInField;
  int termsInverted;  // number of unique terms that were un-inverted
  long termInstances; // total number of references to term numbers
  final TermIndex ti;
  long memsz;
  int total_time;  // total time to uninvert the field
  int phase1_time;  // time for phase1 of the uninvert process
  final AtomicLong use = new AtomicLong(); // number of uses

  int[] index;
  byte[][] tnums = new byte[256][];
  int[] maxTermCounts;
  final Map<Integer,TopTerm> bigTerms = new LinkedHashMap<Integer,TopTerm>();


  public long memSize() {
    // can cache the mem size since it shouldn't change
    if (memsz!=0) return memsz;
    long sz = 8*8 + 32; // local fields
    sz += bigTerms.size() * 64;
    for (TopTerm tt : bigTerms.values()) {
      sz += tt.memSize();
    }
    if (index != null) sz += index.length * 4;
    if (tnums!=null) {
      for (byte[] arr : tnums)
        if (arr != null) sz += arr.length;
    }
    if (maxTermCounts != null)
      sz += maxTermCounts.length * 4;
    sz += ti.memSize();
    memsz = sz;
    return sz;
  }


  /** Number of bytes to represent an unsigned int as a vint. */
  static int vIntSize(int x) {
    if ((x & (0xffffffff << (7*1))) == 0 ) {
      return 1;
    }
    if ((x & (0xffffffff << (7*2))) == 0 ) {
      return 2;
    }
    if ((x & (0xffffffff << (7*3))) == 0 ) {
      return 3;
    }
    if ((x & (0xffffffff << (7*4))) == 0 ) {
      return 4;
    }
    return 5;
  }


  // todo: if we know the size of the vInt already, we could do
  // a single switch on the size
  static int writeInt(int x, byte[] arr, int pos) {
    int a;
    a = (x >>> (7*4));
    if (a != 0) {
      arr[pos++] = (byte)(a | 0x80);
    }
    a = (x >>> (7*3));
    if (a != 0) {
      arr[pos++] = (byte)(a | 0x80);
    }
    a = (x >>> (7*2));
    if (a != 0) {
      arr[pos++] = (byte)(a | 0x80);
    }
    a = (x >>> (7*1));
    if (a != 0) {
      arr[pos++] = (byte)(a | 0x80);
    }
    arr[pos++] = (byte)(x & 0x7f);
    return pos;
  }



  public UnInvertedField(String field, SolrIndexSearcher searcher) throws IOException {
    this.field = field;
    this.ti = new TermIndex(field,
            TrieField.getMainValuePrefix(searcher.getSchema().getFieldType(field)));
    uninvert(searcher);
  }


  private void uninvert(SolrIndexSearcher searcher) throws IOException {
    long startTime = System.currentTimeMillis();

    IndexReader reader = searcher.getIndexReader();
    int maxDoc = reader.maxDoc();

    int[] index = new int[maxDoc];       // immediate term numbers, or the index into the byte[] representing the last number
    this.index = index;
    final int[] lastTerm = new int[maxDoc];    // last term we saw for this document
    final byte[][] bytes = new byte[maxDoc][]; // list of term numbers for the doc (delta encoded vInts)
    maxTermCounts = new int[1024];

    NumberedTermsEnum te = ti.getEnumerator(reader);

    // threshold, over which we use set intersections instead of counting
    // to (1) save memory, and (2) speed up faceting.
    // Add 2 for testing purposes so that there will always be some terms under
    // the threshold even when the index is very small.
    int threshold = maxDoc / 20 + 2;
    // threshold = 2000000000; //////////////////////////////// USE FOR TESTING

    // we need a minimum of 9 bytes, but round up to 12 since the space would
    // be wasted with most allocators anyway.
    byte[] tempArr = new byte[12];

    //
    // enumerate all terms, and build an intermediate form of the un-inverted field.
    //
    // During this intermediate form, every document has a (potential) byte[]
    // and the int[maxDoc()] array either contains the termNumber list directly
    // or the *end* offset of the termNumber list in it's byte array (for faster
    // appending and faster creation of the final form).
    //
    // idea... if things are too large while building, we could do a range of docs
    // at a time (but it would be a fair amount slower to build)
    // could also do ranges in parallel to take advantage of multiple CPUs

    // OPTIONAL: remap the largest df terms to the lowest 128 (single byte)
    // values.  This requires going over the field first to find the most
    // frequent terms ahead of time.

    SolrIndexSearcher.DocsEnumState deState = null;

    for (;;) {
      BytesRef t = te.term();
      if (t==null) break;

      int termNum = te.getTermNumber();

      if (termNum >= maxTermCounts.length) {
        // resize by doubling - for very large number of unique terms, expanding
        // by 4K and resultant GC will dominate uninvert times.  Resize at end if material
        int[] newMaxTermCounts = new int[maxTermCounts.length*2];
        System.arraycopy(maxTermCounts, 0, newMaxTermCounts, 0, termNum);
        maxTermCounts = newMaxTermCounts;
      }

      int df = te.docFreq();
      if (df >= threshold) {
        TopTerm topTerm = new TopTerm();
        topTerm.term = new BytesRef(t);
        topTerm.termNum = termNum;
        bigTerms.put(topTerm.termNum, topTerm);

        if (deState == null) {
          deState = new SolrIndexSearcher.DocsEnumState();
          deState.termsEnum = te.tenum;
          deState.reuse = te.docsEnum;
        }
        DocSet set = searcher.getDocSet(new TermQuery(new Term(ti.field, topTerm.term)), deState);
        te.docsEnum = deState.reuse;

        maxTermCounts[termNum] = set.size();

        te.next();
        continue;
      }

      termsInverted++;

      DocsEnum docsEnum = te.getDocsEnum();

      DocsEnum.BulkReadResult bulkResult = docsEnum.getBulkResult();

      for(;;) {
        int n = docsEnum.read();
        if (n <= 0) break;

        maxTermCounts[termNum] += n;

        for (int i=0; i<n; i++) {
          termInstances++;
          int doc = bulkResult.docs.ints[i];
          // add 2 to the term number to make room for special reserved values:
          // 0 (end term) and 1 (index into byte array follows)
          int delta = termNum - lastTerm[doc] + TNUM_OFFSET;
          lastTerm[doc] = termNum;
          int val = index[doc];

          if ((val & 0xff)==1) {
            // index into byte array (actually the end of
            // the doc-specific byte[] when building)
            int pos = val >>> 8;
            int ilen = vIntSize(delta);
            byte[] arr = bytes[doc];
            int newend = pos+ilen;
            if (newend > arr.length) {
              // We avoid a doubling strategy to lower memory usage.
              // this faceting method isn't for docs with many terms.
              // In hotspot, objects have 2 words of overhead, then fields, rounded up to a 64-bit boundary.
              // TODO: figure out what array lengths we can round up to w/o actually using more memory
              // (how much space does a byte[] take up?  Is data preceded by a 32 bit length only?
              // It should be safe to round up to the nearest 32 bits in any case.
              int newLen = (newend + 3) & 0xfffffffc;  // 4 byte alignment
              byte[] newarr = new byte[newLen];
              System.arraycopy(arr, 0, newarr, 0, pos);
              arr = newarr;
              bytes[doc] = newarr;
            }
            pos = writeInt(delta, arr, pos);
            index[doc] = (pos<<8) | 1;  // update pointer to end index in byte[]
          } else {
            // OK, this int has data in it... find the end (a zero starting byte - not
            // part of another number, hence not following a byte with the high bit set).
            int ipos;
            if (val==0) {
              ipos=0;
            } else if ((val & 0x0000ff80)==0) {
              ipos=1;
            } else if ((val & 0x00ff8000)==0) {
              ipos=2;
            } else if ((val & 0xff800000)==0) {
              ipos=3;
            } else {
              ipos=4;
            }

            int endPos = writeInt(delta, tempArr, ipos);
            if (endPos <= 4) {
              // value will fit in the integer... move bytes back
              for (int j=ipos; j<endPos; j++) {
                val |= (tempArr[j] & 0xff) << (j<<3);
              }
              index[doc] = val;
            } else {
              // value won't fit... move integer into byte[]
              for (int j=0; j<ipos; j++) {
                tempArr[j] = (byte)val;
                val >>>=8;
              }
              // point at the end index in the byte[]
              index[doc] = (endPos<<8) | 1;
              bytes[doc] = tempArr;
              tempArr = new byte[12];
            }

          }

        }

      }

      te.next();
    }

    numTermsInField = te.getTermNumber();
    te.close();

    // free space if outrageously wasteful (tradeoff memory/cpu) 

    if ((maxTermCounts.length - numTermsInField) > 1024) { // too much waste!
      int[] newMaxTermCounts = new int[numTermsInField];
      System.arraycopy(maxTermCounts, 0, newMaxTermCounts, 0, numTermsInField);
      maxTermCounts = newMaxTermCounts;
   }

    long midPoint = System.currentTimeMillis();

    if (termInstances == 0) {
      // we didn't invert anything
      // lower memory consumption.
      index = this.index = null;
      tnums = null;
    } else {

      //
      // transform intermediate form into the final form, building a single byte[]
      // at a time, and releasing the intermediate byte[]s as we go to avoid
      // increasing the memory footprint.
      //
      for (int pass = 0; pass<256; pass++) {
        byte[] target = tnums[pass];
        int pos=0;  // end in target;
        if (target != null) {
          pos = target.length;
        } else {
          target = new byte[4096];
        }

        // loop over documents, 0x00ppxxxx, 0x01ppxxxx, 0x02ppxxxx
        // where pp is the pass (which array we are building), and xx is all values.
        // each pass shares the same byte[] for termNumber lists.
        for (int docbase = pass<<16; docbase<maxDoc; docbase+=(1<<24)) {
          int lim = Math.min(docbase + (1<<16), maxDoc);
          for (int doc=docbase; doc<lim; doc++) {
            int val = index[doc];
            if ((val&0xff) == 1) {
              int len = val >>> 8;
              index[doc] = (pos<<8)|1; // change index to point to start of array
              if ((pos & 0xff000000) != 0) {
                // we only have 24 bits for the array index
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Too many values for UnInvertedField faceting on field "+field);
              }
              byte[] arr = bytes[doc];
              bytes[doc] = null;        // IMPORTANT: allow GC to avoid OOM
              if (target.length <= pos + len) {
                int newlen = target.length;
                /*** we don't have to worry about the array getting too large
                 * since the "pos" param will overflow first (only 24 bits available)
                if ((newlen<<1) <= 0) {
                  // overflow...
                  newlen = Integer.MAX_VALUE;
                  if (newlen <= pos + len) {
                    throw new SolrException(400,"Too many terms to uninvert field!");
                  }
                } else {
                  while (newlen <= pos + len) newlen<<=1;  // doubling strategy
                }
                ****/
                while (newlen <= pos + len) newlen<<=1;  // doubling strategy                 
                byte[] newtarget = new byte[newlen];
                System.arraycopy(target, 0, newtarget, 0, pos);
                target = newtarget;
              }
              System.arraycopy(arr, 0, target, pos, len);
              pos += len + 1;  // skip single byte at end and leave it 0 for terminator
            }
          }
        }

        // shrink array
        if (pos < target.length) {
          byte[] newtarget = new byte[pos];
          System.arraycopy(target, 0, newtarget, 0, pos);
          target = newtarget;
          if (target.length > (1<<24)*.9) {
            SolrCore.log.warn("Approaching too many values for UnInvertedField faceting on field '"+field+"' : bucket size=" + target.length);
          }
        }
        
        tnums[pass] = target;

        if ((pass << 16) > maxDoc)
          break;
      }
    }

    long endTime = System.currentTimeMillis();

    total_time = (int)(endTime-startTime);
    phase1_time = (int)(midPoint-startTime);

    SolrCore.log.info("UnInverted multi-valued field " + toString());
  }




  public NamedList<Integer> getCounts(SolrIndexSearcher searcher, DocSet baseDocs, int offset, int limit, Integer mincount, boolean missing, String sort, String prefix) throws IOException {
    use.incrementAndGet();

    FieldType ft = searcher.getSchema().getFieldType(field);

    NamedList<Integer> res = new NamedList<Integer>();  // order is important

    DocSet docs = baseDocs;
    int baseSize = docs.size();
    int maxDoc = searcher.maxDoc();

    if (baseSize >= mincount) {

      final int[] index = this.index;
      // tricky: we add more more element than we need because we will reuse this array later
      // for ordering term ords before converting to term labels.
      final int[] counts = new int[numTermsInField + 1];

      //
      // If there is prefix, find it's start and end term numbers
      //
      int startTerm = 0;
      int endTerm = numTermsInField;  // one past the end

      NumberedTermsEnum te = ti.getEnumerator(searcher.getIndexReader());
      if (prefix != null && prefix.length() > 0) {
        BytesRef prefixBr = new BytesRef(prefix);
        te.skipTo(prefixBr);
        startTerm = te.getTermNumber();
        prefixBr.append(ByteUtils.bigTerm);
        te.skipTo(prefixBr);
        endTerm = te.getTermNumber();
      }

      /***********
      // Alternative 2: get the docSet of the prefix (could take a while) and
      // then do the intersection with the baseDocSet first.
      if (prefix != null && prefix.length() > 0) {
        docs = searcher.getDocSet(new ConstantScorePrefixQuery(new Term(field, ft.toInternal(prefix))), docs);
        // The issue with this method are problems of returning 0 counts for terms w/o
        // the prefix.  We can't just filter out those terms later because it may
        // mean that we didn't collect enough terms in the queue (in the sorted case).
      }
      ***********/

      boolean doNegative = baseSize > maxDoc >> 1 && termInstances > 0
              && startTerm==0 && endTerm==numTermsInField
              && docs instanceof BitDocSet;

      if (doNegative) {
        OpenBitSet bs = (OpenBitSet)((BitDocSet)docs).getBits().clone();
        bs.flip(0, maxDoc);
        // TODO: when iterator across negative elements is available, use that
        // instead of creating a new bitset and inverting.
        docs = new BitDocSet(bs, maxDoc - baseSize);
        // simply negating will mean that we have deleted docs in the set.
        // that should be OK, as their entries in our table should be empty.
      }

      // For the biggest terms, do straight set intersections
      for (TopTerm tt : bigTerms.values()) {
        // TODO: counts could be deferred if sorted==false
        if (tt.termNum >= startTerm && tt.termNum < endTerm) {
          counts[tt.termNum] = searcher.numDocs(new TermQuery(new Term(ti.field, tt.term)), docs);
        }
      }

      // TODO: we could short-circuit counting altogether for sorted faceting
      // where we already have enough terms from the bigTerms

      // TODO: we could shrink the size of the collection array, and
      // additionally break when the termNumber got above endTerm, but
      // it would require two extra conditionals in the inner loop (although
      // they would be predictable for the non-prefix case).
      // Perhaps a different copy of the code would be warranted.

      if (termInstances > 0) {
        DocIterator iter = docs.iterator();
        while (iter.hasNext()) {
          int doc = iter.nextDoc();
          int code = index[doc];

          if ((code & 0xff)==1) {
            int pos = code>>>8;
            int whichArray = (doc >>> 16) & 0xff;
            byte[] arr = tnums[whichArray];
            int tnum = 0;
            for(;;) {
              int delta = 0;
              for(;;) {
                byte b = arr[pos++];
                delta = (delta << 7) | (b & 0x7f);
                if ((b & 0x80) == 0) break;
              }
              if (delta == 0) break;
              tnum += delta - TNUM_OFFSET;
              counts[tnum]++;
            }
          } else {
            int tnum = 0;
            int delta = 0;
            for (;;) {
              delta = (delta << 7) | (code & 0x7f);
              if ((code & 0x80)==0) {
                if (delta==0) break;
                tnum += delta - TNUM_OFFSET;
                counts[tnum]++;
                delta = 0;
              }
              code >>>= 8;
            }
          }
        }
      }

      CharArr spare = new CharArr();

      int off=offset;
      int lim=limit>=0 ? limit : Integer.MAX_VALUE;

      if (sort.equals(FacetParams.FACET_SORT_COUNT) || sort.equals(FacetParams.FACET_SORT_COUNT_LEGACY)) {
        int maxsize = limit>0 ? offset+limit : Integer.MAX_VALUE-1;
        maxsize = Math.min(maxsize, numTermsInField);
        LongPriorityQueue queue = new LongPriorityQueue(Math.min(maxsize,1000), maxsize, Long.MIN_VALUE);

        int min=mincount-1;  // the smallest value in the top 'N' values
        for (int i=startTerm; i<endTerm; i++) {
          int c = doNegative ? maxTermCounts[i] - counts[i] : counts[i];
          if (c>min) {
            // NOTE: we use c>min rather than c>=min as an optimization because we are going in
            // index order, so we already know that the keys are ordered.  This can be very
            // important if a lot of the counts are repeated (like zero counts would be).

            // smaller term numbers sort higher, so subtract the term number instead
            long pair = (((long)c)<<32) + (Integer.MAX_VALUE - i);
            boolean displaced = queue.insert(pair);
            if (displaced) min=(int)(queue.top() >>> 32);
          }
        }

        // now select the right page from the results

        // if we are deep paging, we don't have to order the highest "offset" counts.
        int collectCount = Math.max(0, queue.size() - off);
        assert collectCount <= lim;

        // the start and end indexes of our list "sorted" (starting with the highest value)
        int sortedIdxStart = queue.size() - (collectCount - 1);
        int sortedIdxEnd = queue.size() + 1;
        final long[] sorted = queue.sort(collectCount);

        final int[] indirect = counts;  // reuse the counts array for the index into the tnums array
        assert indirect.length >= sortedIdxEnd;

        for (int i=sortedIdxStart; i<sortedIdxEnd; i++) {
          long pair = sorted[i];
          int c = (int)(pair >>> 32);
          int tnum = Integer.MAX_VALUE - (int)pair;

          indirect[i] = i;   // store the index for indirect sorting
          sorted[i] = tnum;  // reuse the "sorted" array to store the term numbers for indirect sorting

          // add a null label for now... we'll fill it in later.
          res.add(null, c);
        }

        // now sort the indexes by the term numbers
        PrimUtils.sort(sortedIdxStart, sortedIdxEnd, indirect, new PrimUtils.IntComparator() {
          @Override
          public int compare(int a, int b) {
            return (int)sorted[a] - (int)sorted[b];
          }

          @Override
          public boolean lessThan(int a, int b) {
            return sorted[a] < sorted[b];
          }

          @Override
          public boolean equals(int a, int b) {
            return sorted[a] == sorted[b];
          }
        });

        // convert the term numbers to term values and set as the label
        for (int i=sortedIdxStart; i<sortedIdxEnd; i++) {
          int idx = indirect[i];
          int tnum = (int)sorted[idx];
          String label = getReadableValue(getTermValue(te, tnum), ft, spare);
          res.setName(idx - sortedIdxStart, label);
        }

      } else {
        // add results in index order
        int i=startTerm;
        if (mincount<=0) {
          // if mincount<=0, then we won't discard any terms and we know exactly
          // where to start.
          i=startTerm+off;
          off=0;
        }

        for (; i<endTerm; i++) {
          int c = doNegative ? maxTermCounts[i] - counts[i] : counts[i];
          if (c<mincount || --off>=0) continue;
          if (--lim<0) break;

          String label = getReadableValue(getTermValue(te, i), ft, spare);
          res.add(label, c);
        }
      }

      te.close();
    }


    if (missing) {
      // TODO: a faster solution for this?
      res.add(null, SimpleFacets.getFieldMissingCount(searcher, baseDocs, field));
    }

    return res;
  }

  /**
   * Collect statistics about the UninvertedField.  Code is very similar to {@link #getCounts(org.apache.solr.search.SolrIndexSearcher, org.apache.solr.search.DocSet, int, int, Integer, boolean, String, String)}
   * It can be used to calculate stats on multivalued fields.
   * <p/>
   * This method is mainly used by the {@link org.apache.solr.handler.component.StatsComponent}.
   *
   * @param searcher The Searcher to use to gather the statistics
   * @param baseDocs The {@link org.apache.solr.search.DocSet} to gather the stats on
   * @param facet One or more fields to facet on.
   * @return The {@link org.apache.solr.handler.component.StatsValues} collected
   * @throws IOException
   */
  public StatsValues getStats(SolrIndexSearcher searcher, DocSet baseDocs, String[] facet) throws IOException {
    //this function is ripped off nearly wholesale from the getCounts function to use
    //for multiValued fields within the StatsComponent.  may be useful to find common
    //functionality between the two and refactor code somewhat
    use.incrementAndGet();

    StatsValues allstats = new StatsValues();


    DocSet docs = baseDocs;
    int baseSize = docs.size();
    int maxDoc = searcher.maxDoc();

    if (baseSize <= 0) return allstats;

    FieldType ft = searcher.getSchema().getFieldType(field);

    DocSet missing = docs.andNot( searcher.getDocSet(new TermRangeQuery(field, null, null, false, false)) );

    int i = 0;
    final FieldFacetStats[] finfo = new FieldFacetStats[facet.length];
    //Initialize facetstats, if facets have been passed in
    FieldCache.DocTermsIndex si;
    for (String f : facet) {
      FieldType facet_ft = searcher.getSchema().getFieldType(f);
      try {
        si = FieldCache.DEFAULT.getTermsIndex(searcher.getIndexReader(), f);
      }
      catch (IOException e) {
        throw new RuntimeException("failed to open field cache for: " + f, e);
      }
      finfo[i] = new FieldFacetStats(f, si, facet_ft, numTermsInField);
      i++;
    }

    final int[] index = this.index;
    final int[] counts = new int[numTermsInField];//keep track of the number of times we see each word in the field for all the documents in the docset

    NumberedTermsEnum te = ti.getEnumerator(searcher.getIndexReader());


    boolean doNegative = false;
    if (finfo.length == 0) {
      //if we're collecting statistics with a facet field, can't do inverted counting
      doNegative = baseSize > maxDoc >> 1 && termInstances > 0
              && docs instanceof BitDocSet;
    }

    if (doNegative) {
      OpenBitSet bs = (OpenBitSet) ((BitDocSet) docs).getBits().clone();
      bs.flip(0, maxDoc);
      // TODO: when iterator across negative elements is available, use that
      // instead of creating a new bitset and inverting.
      docs = new BitDocSet(bs, maxDoc - baseSize);
      // simply negating will mean that we have deleted docs in the set.
      // that should be OK, as their entries in our table should be empty.
    }

    // For the biggest terms, do straight set intersections
    for (TopTerm tt : bigTerms.values()) {
      // TODO: counts could be deferred if sorted==false
      if (tt.termNum >= 0 && tt.termNum < numTermsInField) {
        final Term t = new Term(ti.field, tt.term);
        if (finfo.length == 0) {
          counts[tt.termNum] = searcher.numDocs(new TermQuery(t), docs);
        } else {
          //COULD BE VERY SLOW
          //if we're collecting stats for facet fields, we need to iterate on all matching documents
          DocSet bigTermDocSet = searcher.getDocSet(new TermQuery(t)).intersection(docs);
          DocIterator iter = bigTermDocSet.iterator();
          while (iter.hasNext()) {
            int doc = iter.nextDoc();
            counts[tt.termNum]++;
            for (FieldFacetStats f : finfo) {
              f.facetTermNum(doc, tt.termNum);
            }
          }
        }
      }
    }


    if (termInstances > 0) {
      DocIterator iter = docs.iterator();
      while (iter.hasNext()) {
        int doc = iter.nextDoc();
        int code = index[doc];

        if ((code & 0xff) == 1) {
          int pos = code >>> 8;
          int whichArray = (doc >>> 16) & 0xff;
          byte[] arr = tnums[whichArray];
          int tnum = 0;
          for (; ;) {
            int delta = 0;
            for (; ;) {
              byte b = arr[pos++];
              delta = (delta << 7) | (b & 0x7f);
              if ((b & 0x80) == 0) break;
            }
            if (delta == 0) break;
            tnum += delta - TNUM_OFFSET;
            counts[tnum]++;
            for (FieldFacetStats f : finfo) {
              f.facetTermNum(doc, tnum);
            }
          }
        } else {
          int tnum = 0;
          int delta = 0;
          for (; ;) {
            delta = (delta << 7) | (code & 0x7f);
            if ((code & 0x80) == 0) {
              if (delta == 0) break;
              tnum += delta - TNUM_OFFSET;
              counts[tnum]++;
              for (FieldFacetStats f : finfo) {
                f.facetTermNum(doc, tnum);
              }
              delta = 0;
            }
            code >>>= 8;
          }
        }
      }
    }

    // add results in index order
    CharArr spare = new CharArr();

    for (i = 0; i < numTermsInField; i++) {
      int c = doNegative ? maxTermCounts[i] - counts[i] : counts[i];
      if (c == 0) continue;
      String label = getReadableValue(getTermValue(te, i), ft, spare);
      // TODO: we should avoid this re-parse
      Double value = Double.parseDouble(label);

      allstats.accumulate(value, c);
      //as we've parsed the termnum into a value, lets also accumulate fieldfacet statistics
      for (FieldFacetStats f : finfo) {
        f.accumulateTermNum(i, value);
      }
    }
    te.close();

    int c = missing.size();
    allstats.addMissing(c);

    if (finfo.length > 0) {
      allstats.facets = new HashMap<String, Map<String, StatsValues>>();
      for (FieldFacetStats f : finfo) {
        Map<String, StatsValues> facetStatsValues = f.facetStatsValues;
        FieldType facetType = searcher.getSchema().getFieldType(f.name);
        for (Map.Entry<String,StatsValues> entry : facetStatsValues.entrySet()) {
          String termLabel = entry.getKey();
          int missingCount = searcher.numDocs(new TermQuery(new Term(f.name, facetType.toInternal(termLabel))), missing);
          entry.getValue().addMissing(missingCount);
        }
        allstats.facets.put(f.name, facetStatsValues);
      }
    }

    return allstats;

  }

  String getReadableValue(BytesRef termval, FieldType ft, CharArr spare) {
    if (spare == null) {
      spare = new CharArr();
    } else {
      spare.reset();
    }
    ft.indexedToReadable(termval, spare);
    return spare.toString();    
  }

  /** may return a reused BytesRef */
  BytesRef getTermValue(NumberedTermsEnum te, int termNum) throws IOException {
    if (bigTerms.size() > 0) {
      // see if the term is one of our big terms.
      TopTerm tt = bigTerms.get(termNum);
      if (tt != null) {
        return tt.term;
      }
    }

    return te.skipTo(termNum);
  }

  @Override
  public String toString() {
    return "{field=" + field
            + ",memSize="+memSize()
            + ",tindexSize="+ti.memSize()
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

    UnInvertedField uif = cache.get(field);
    if (uif == null) {
      synchronized (cache) {
        uif = cache.get(field);
        if (uif == null) {
          uif = new UnInvertedField(field, searcher);
          cache.put(field, uif);
        }
      }
    }

    return uif;
  }
}


// How to share TermDocs (int[] score[])???
// Hot to share TermPositions?
/***
class TermEnumListener {
  void doTerm(Term t) {
  }
  void done() {
  }
}
***/


class NumberedTermsEnum extends TermsEnum {
  protected final IndexReader reader;
  protected final TermIndex tindex;
  protected TermsEnum tenum;
  protected int pos=-1;
  protected BytesRef termText;
  protected DocsEnum docsEnum;
  protected Bits deletedDocs;


  NumberedTermsEnum(IndexReader reader, TermIndex tindex) throws IOException {
    this.reader = reader;
    this.tindex = tindex;
  }


  NumberedTermsEnum(IndexReader reader, TermIndex tindex, BytesRef termValue, int pos) throws IOException {
    this.reader = reader;
    this.tindex = tindex;
    this.pos = pos;
    Terms terms = MultiFields.getTerms(reader, tindex.field);
    deletedDocs = MultiFields.getDeletedDocs(reader);
    if (terms != null) {
      tenum = terms.iterator();
      tenum.seek(termValue);
      setTerm();
    }
  }

  @Override
  public Comparator<BytesRef> getComparator() throws IOException {
    return tenum.getComparator();
  }

  public DocsEnum getDocsEnum() throws IOException {
    docsEnum = tenum.docs(deletedDocs, docsEnum);
    return docsEnum;
  }

  protected BytesRef setTerm() throws IOException {
    termText = tenum.term();
    if (tindex.prefix != null && !termText.startsWith(tindex.prefix)) {
      termText = null;
    }
    return termText;
  }

  @Override
  public BytesRef next() throws IOException {
    pos++;
    if (tenum.next() == null) {
      termText = null;
      return null;
    }
    return setTerm();  // this is extra work if we know we are in bounds...
  }

  @Override
  public BytesRef term() {
    return termText;
  }

  @Override
  public int docFreq() throws IOException {
    return tenum.docFreq();
  }

  @Override
  public long totalTermFreq() throws IOException {
    return tenum.totalTermFreq();
  }

  public BytesRef skipTo(BytesRef target) throws IOException {

    // already here
    if (termText != null && termText.equals(target)) return termText;

    if (tenum == null) {
      return null;
    }

    int startIdx = Arrays.binarySearch(tindex.index,target);

    if (startIdx >= 0) {
      // we hit the term exactly... lucky us!
      TermsEnum.SeekStatus seekStatus = tenum.seek(target);
      assert seekStatus == TermsEnum.SeekStatus.FOUND;
      pos = startIdx << tindex.intervalBits;
      return setTerm();
    }

    // we didn't hit the term exactly
    startIdx=-startIdx-1;
    
    if (startIdx == 0) {
      // our target occurs *before* the first term
      TermsEnum.SeekStatus seekStatus = tenum.seek(target);
      assert seekStatus == TermsEnum.SeekStatus.NOT_FOUND;
      pos = 0;
      return setTerm();
    }

    // back up to the start of the block
    startIdx--;

    if ((pos >> tindex.intervalBits) == startIdx && termText != null && termText.compareTo(target)<=0) {
      // we are already in the right block and the current term is before the term we want,
      // so we don't need to seek.
    } else {
      // seek to the right block
      TermsEnum.SeekStatus seekStatus = tenum.seek(tindex.index[startIdx]);
      assert seekStatus == TermsEnum.SeekStatus.FOUND;
      pos = startIdx << tindex.intervalBits;
      setTerm();  // should be non-null since it's in the index
    }

    while (termText != null && termText.compareTo(target) < 0) {
      next();
    }

    return termText;
  }

  public BytesRef skipTo(int termNumber) throws IOException {
    int delta = termNumber - pos;
    if (delta < 0 || delta > tindex.interval || tenum==null) {
      int idx = termNumber >>> tindex.intervalBits;
      BytesRef base = tindex.index[idx];
      pos = idx << tindex.intervalBits;
      delta = termNumber - pos;
      TermsEnum.SeekStatus seekStatus = tenum.seek(base);
      assert seekStatus == TermsEnum.SeekStatus.FOUND;
    }
    while (--delta >= 0) {
      BytesRef br = tenum.next();
      if (br == null) {
        termText = null;
        return null;
      }
      ++pos;
    }
    return setTerm();
  }

  protected void close() throws IOException {
    // no-op, needed so the anon subclass that does indexing
    // can build its index
  }

  /** The current term number, starting at 0.
   * Only valid if the previous call to next() or skipTo() returned true.
   */
  public int getTermNumber() {
    return pos;
  }

  @Override
  public long ord() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SeekStatus seek(long ord) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DocsEnum docs(Bits skipDocs, DocsEnum reuse) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SeekStatus seek(BytesRef target, boolean useCache) {
    throw new UnsupportedOperationException();
  }
}


/**
 * Class to save memory by only storing every nth term (for random access), while
 * numbering the terms, allowing them to be retrieved later by number.
 * This is only valid when used with the IndexReader it was created with.
 * The IndexReader is not actually stored to facilitate caching by using it as a key in
 * a weak hash map.
 */
class TermIndex {
  final static int intervalBits = 7;  // decrease to a low number like 2 for testing
  final static int intervalMask = 0xffffffff >>> (32-intervalBits);
  final static int interval = 1 << intervalBits;

  final String field;
  final BytesRef prefix;
  BytesRef[] index;
  int nTerms;
  long sizeOfStrings;

  TermIndex(String field) {
    this(field, null);
  }

  TermIndex(String field, String prefix) {
    this.field = field;
    this.prefix = prefix == null ? null : new BytesRef(prefix);
  }

  NumberedTermsEnum getEnumerator(IndexReader reader, int termNumber) throws IOException {
    NumberedTermsEnum te = new NumberedTermsEnum(reader, this);
    te.skipTo(termNumber);
    return te;
  }

  /* The first time an enumerator is requested, it should be used
     with next() to fully traverse all of the terms so the index
     will be built.
   */
  NumberedTermsEnum getEnumerator(IndexReader reader) throws IOException {
    if (index==null) return new NumberedTermsEnum(reader,this, prefix==null?new BytesRef():prefix, 0) {
      ArrayList<BytesRef> lst;
      PagedBytes bytes;

      @Override
      protected BytesRef setTerm() throws IOException {
        BytesRef br = super.setTerm();
        if (br != null && (pos & intervalMask)==0) {
          sizeOfStrings += br.length;
          if (lst==null) {
            lst = new ArrayList<BytesRef>();
            bytes = new PagedBytes(15);
          }
          BytesRef out = new BytesRef();
          bytes.copy(br, out);
          lst.add(out);
        }
        return br;
      }

      @Override
      public BytesRef skipTo(int termNumber) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close() throws IOException {
        nTerms=pos;
        super.close();
        index = lst!=null ? lst.toArray(new BytesRef[lst.size()]) : new BytesRef[0];
      }
    };
    else return new NumberedTermsEnum(reader,this,new BytesRef(),0);
  }


  /**
   * Returns the approximate amount of memory taken by this TermIndex.
   * This is only an approximation and doesn't take into account java object overhead.
   *
   * @return
   * the approximate memory consumption in bytes
   */
  public long memSize() {
    // assume 8 byte references?
    return 8+8+8+8+(index.length<<3)+sizeOfStrings;
  }
}

