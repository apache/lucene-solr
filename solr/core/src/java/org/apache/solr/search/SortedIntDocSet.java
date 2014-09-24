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

package org.apache.solr.search;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * <code>SortedIntDocSet</code> represents a sorted set of Lucene Document Ids.
 */
public class SortedIntDocSet extends DocSetBase {
  protected final int[] docs;

  /**
   * @param docs  Sorted list of ids
   */
  public SortedIntDocSet(int[] docs) {
    this.docs = docs;
    // if (firstNonSorted(docs,0,docs.length)>=0) throw new RuntimeException("NON SORTED DOCS!!!");
  }

  /**
   * @param docs Sorted list of ids
   * @param len  Number of ids in the list
   */
  public SortedIntDocSet(int[] docs, int len) {
    this(shrink(docs,len));
  }

  public int[] getDocs() { return docs; }

  @Override
  public int size()      { return docs.length; }

  @Override
  public long memSize() {
    return (docs.length<<2)+8;
  }

  public static int[] zeroInts = new int[0];
  public static SortedIntDocSet zero = new SortedIntDocSet(zeroInts);

  public static int[] shrink(int[] arr, int newSize) {
    if (arr.length == newSize) return arr;
    int[] newArr = new int[newSize];
    System.arraycopy(arr, 0, newArr, 0, newSize);
    return newArr;
  }

  /** Returns the index of the first non-sorted element or -1 if they are all sorted */
  public static int firstNonSorted(int[] arr, int offset, int len) {
    if (len <= 1) return -1;
    int lower = arr[offset];
    int end = offset + len;
    for(int i=offset+1; i<end; i++) {
      int next = arr[i];
      if (next <= lower) {
        for (int j=i-1; j>offset; j--) {
          if (arr[j]<next) return j+1;
        }
        return offset;
      }
      lower = next;
    }
    return -1;
  }

  public static int intersectionSize(int[] smallerSortedList, int[] biggerSortedList) {
    final int a[] = smallerSortedList;
    final int b[] = biggerSortedList;

    // The next doc we are looking for will be much closer to the last position we tried
    // than it will be to the midpoint between last and high... so probe ahead using
    // a function of the ratio of the sizes of the sets.
    int step = (b.length/a.length)+1;

    // Since the majority of probes should be misses, we'll already be above the last probe
    // and shouldn't need to move larger than the step size on average to step over our target (and thus lower
    // the high upper bound a lot.)... but if we don't go over our target, it's a big miss... so double it.
    step = step + step;

    // FUTURE: come up with a density such that target * density == likely position?
    // then check step on one side or the other?
    // (density could be cached in the DocSet)... length/maxDoc

    // FUTURE: try partitioning like a sort algorithm.  Pick the midpoint of the big
    // array, find where that should be in the small array, and then recurse with
    // the top and bottom half of both arrays until they are small enough to use
    // a fallback insersection method.
    // NOTE: I tried this and it worked, but it was actually slower than this current
    // highly optimized approach.

    int icount = 0;
    int low = 0;
    int max = b.length-1;

    for (int i=0; i<a.length; i++) {
      int doca = a[i];

      int high = max;

      int probe = low + step;     // 40% improvement!

      // short linear probe to see if we can drop the high pointer in one big jump.
      if (probe<high) {
        if (b[probe]>=doca) {
          // success!  we cut down the upper bound by a lot in one step!
          high=probe;
        } else {
          // relative failure... we get to move the low pointer, but not my much
          low=probe+1;

          // reprobe worth it? it appears so!
          probe = low + step;
          if (probe<high) {
            if (b[probe]>=doca) {
              high=probe;
            } else {
              low=probe+1;
            }
          }
        }
      }

      // binary search the rest of the way
      while (low <= high) {
        int mid = (low+high) >>> 1;
        int docb = b[mid];

        if (docb < doca) {
          low = mid+1;
        }
        else if (docb > doca) {
          high = mid-1;
        }
        else {
          icount++;
          low = mid+1;  // found it, so start at next element
          break;
        }
      }
      // Didn't find it... low is now positioned on the insertion point,
      // which is higher than what we were looking for, so continue using
      // the same low point.
    }

    return icount;
  }


  public static boolean intersects(int[] smallerSortedList, int[] biggerSortedList) {
    // see intersectionSize for more in-depth comments of this algorithm

    final int a[] = smallerSortedList;
    final int b[] = biggerSortedList;

    int step = (b.length/a.length)+1;

    step = step + step;

    int low = 0;
    int max = b.length-1;

    for (int i=0; i<a.length; i++) {
      int doca = a[i];
      int high = max;
      int probe = low + step;
      if (probe<high) {
        if (b[probe]>=doca) {
          high=probe;
        } else {
          low=probe+1;
          probe = low + step;
          if (probe<high) {
            if (b[probe]>=doca) {
              high=probe;
            } else {
              low=probe+1;
            }
          }
        }
      }

      while (low <= high) {
        int mid = (low+high) >>> 1;
        int docb = b[mid];

        if (docb < doca) {
          low = mid+1;
        }
        else if (docb > doca) {
          high = mid-1;
        }
        else {
          return true;
        }
      }
    }

    return false;
  }

  @Override
  public int intersectionSize(DocSet other) {
    if (!(other instanceof SortedIntDocSet)) {
      // assume other implementations are better at random access than we are,
      // true of BitDocSet and HashDocSet.
      int icount = 0;
      for (int i=0; i<docs.length; i++) {
        if (other.exists(docs[i])) icount++;
      }
      return icount;
    }

    // make "a" the smaller set.
    int[] otherDocs = ((SortedIntDocSet)other).docs;
    final int[] a = docs.length < otherDocs.length ? docs : otherDocs;
    final int[] b = docs.length < otherDocs.length ? otherDocs : docs;

    if (a.length==0) return 0;

    // if b is 8 times bigger than a, use the modified binary search.
    if ((b.length>>3) >= a.length) {
      return intersectionSize(a,b);
    }

    // if they are close in size, just do a linear walk of both.
    int icount=0;
    int i=0,j=0;
    int doca=a[i],docb=b[j];
    for(;;) {
      // switch on the sign bit somehow?  Hopefull JVM is smart enough to just test once.

      // Since set a is less dense then set b, doca is likely to be greater than docb so
      // check that case first.  This resulted in a 13% speedup.
      if (doca > docb) {
        if (++j >= b.length) break;
        docb=b[j];
      } else if (doca < docb) {
        if (++i >= a.length) break;
        doca=a[i];
      } else {
        icount++;
        if (++i >= a.length) break;
        doca=a[i];
        if (++j >= b.length) break;
        docb=b[j];
      }
    }
    return icount;
  }

  @Override
  public boolean intersects(DocSet other) {
    if (!(other instanceof SortedIntDocSet)) {
      // assume other implementations are better at random access than we are,
      // true of BitDocSet and HashDocSet.
      for (int i=0; i<docs.length; i++) {
        if (other.exists(docs[i])) return true;
      }
      return false;
    }

    // make "a" the smaller set.
    int[] otherDocs = ((SortedIntDocSet)other).docs;
    final int[] a = docs.length < otherDocs.length ? docs : otherDocs;
    final int[] b = docs.length < otherDocs.length ? otherDocs : docs;

    if (a.length==0) return false;

    // if b is 8 times bigger than a, use the modified binary search.
    if ((b.length>>3) >= a.length) {
      return intersects(a,b);
    }

    // if they are close in size, just do a linear walk of both.
    int i=0,j=0;
    int doca=a[i],docb=b[j];
    for(;;) {
      // switch on the sign bit somehow?  Hopefull JVM is smart enough to just test once.

      // Since set a is less dense then set b, doca is likely to be greater than docb so
      // check that case first.  This resulted in a 13% speedup.
      if (doca > docb) {
        if (++j >= b.length) break;
        docb=b[j];
      } else if (doca < docb) {
        if (++i >= a.length) break;
        doca=a[i];
      } else {
        return true;
      }
    }
    return false;
  }

  /** puts the intersection of a and b into the target array and returns the size */
  public static int intersection(int a[], int lena, int b[], int lenb, int[] target) {
    if (lena > lenb) {
      int ti=lena; lena=lenb; lenb=ti;
      int[] ta=a; a=b; b=ta;
    }

    if (lena==0) return 0;


    // if b is 8 times bigger than a, use the modified binary search.
    if ((lenb>>3) >= lena) {
      return intersectionBinarySearch(a, lena, b, lenb, target);
    }

    int icount=0;
    int i=0,j=0;
    int doca=a[i],docb=b[j];
    for(;;) {
      if (doca > docb) {
        if (++j >= lenb) break;
        docb=b[j];
      } else if (doca < docb) {
        if (++i >= lena) break;
        doca=a[i];
      } else {
        target[icount++] = doca;
        if (++i >= lena) break;
        doca=a[i];
        if (++j >= lenb) break;
        docb=b[j];
      }
    }
    return icount;
  }

  /** Puts the intersection of a and b into the target array and returns the size.
   * lena should be smaller than lenb */
  protected static int intersectionBinarySearch(int[] a, int lena, int[] b, int lenb, int[] target) {
    int step = (lenb/lena)+1;
    step = step + step;


    int icount = 0;
    int low = 0;
    int max = lenb-1;

    for (int i=0; i<lena; i++) {
      int doca = a[i];

      int high = max;

      int probe = low + step;     // 40% improvement!

      // short linear probe to see if we can drop the high pointer in one big jump.
      if (probe<high) {
        if (b[probe]>=doca) {
          // success!  we cut down the upper bound by a lot in one step!
          high=probe;
        } else {
          // relative failure... we get to move the low pointer, but not my much
          low=probe+1;

          // reprobe worth it? it appears so!
          probe = low + step;
          if (probe<high) {
            if (b[probe]>=doca) {
              high=probe;
            } else {
              low=probe+1;
            }
          }
        }
      }


      // binary search
      while (low <= high) {
        int mid = (low+high) >>> 1;
        int docb = b[mid];

        if (docb < doca) {
          low = mid+1;
        }
        else if (docb > doca) {
          high = mid-1;
        }
        else {
          target[icount++] = doca;
          low = mid+1;  // found it, so start at next element
          break;
        }
      }
      // Didn't find it... low is now positioned on the insertion point,
      // which is higher than what we were looking for, so continue using
      // the same low point.
    }

    return icount;
  }

  @Override
  public DocSet intersection(DocSet other) {
    if (!(other instanceof SortedIntDocSet)) {
      int icount = 0;
      int arr[] = new int[docs.length];
      for (int i=0; i<docs.length; i++) {
        int doc = docs[i];
        if (other.exists(doc)) arr[icount++] = doc;
      }
      return new SortedIntDocSet(arr,icount);
    }

    int[] otherDocs = ((SortedIntDocSet)other).docs;
    int maxsz = Math.min(docs.length, otherDocs.length);
    int[] arr = new int[maxsz];
    int sz = intersection(docs, docs.length, otherDocs, otherDocs.length, arr);
    return new SortedIntDocSet(arr,sz);
  }


  protected static int andNotBinarySearch(int a[], int lena, int b[], int lenb, int[] target) {
   int step = (lenb/lena)+1;
    step = step + step;


    int count = 0;
    int low = 0;
    int max = lenb-1;

    outer:
    for (int i=0; i<lena; i++) {
      int doca = a[i];

      int high = max;

      int probe = low + step;     // 40% improvement!

      // short linear probe to see if we can drop the high pointer in one big jump.
      if (probe<high) {
        if (b[probe]>=doca) {
          // success!  we cut down the upper bound by a lot in one step!
          high=probe;
        } else {
          // relative failure... we get to move the low pointer, but not my much
          low=probe+1;

          // reprobe worth it? it appears so!
          probe = low + step;
          if (probe<high) {
            if (b[probe]>=doca) {
              high=probe;
            } else {
              low=probe+1;
            }
          }
        }
      }


      // binary search
      while (low <= high) {
        int mid = (low+high) >>> 1;
        int docb = b[mid];

        if (docb < doca) {
          low = mid+1;
        }
        else if (docb > doca) {
          high = mid-1;
        }
        else {
          low = mid+1;  // found it, so start at next element
          continue outer;
        }
      }
      // Didn't find it... low is now positioned on the insertion point,
      // which is higher than what we were looking for, so continue using
      // the same low point.
      target[count++] = doca;
    }

    return count;
  }

    /** puts the intersection of a and not b into the target array and returns the size */
  public static int andNot(int a[], int lena, int b[], int lenb, int[] target) {
    if (lena==0) return 0;
    if (lenb==0) {
      System.arraycopy(a,0,target,0,lena);
      return lena;
    }

    // if b is 8 times bigger than a, use the modified binary search.
    if ((lenb>>3) >= lena) {
      return andNotBinarySearch(a, lena, b, lenb, target);
    }

    int count=0;
    int i=0,j=0;
    int doca=a[i],docb=b[j];
    for(;;) {
      if (doca > docb) {
        if (++j >= lenb) break;
        docb=b[j];
      } else if (doca < docb) {
        target[count++] = doca;
        if (++i >= lena) break;
        doca=a[i];
      } else {
        if (++i >= lena) break;
        doca=a[i];
        if (++j >= lenb) break;
        docb=b[j];
      }
    }

    int leftover=lena - i;

    if (leftover > 0) {
      System.arraycopy(a,i,target,count,leftover);
      count += leftover;
    }

    return count;
  }

  @Override
  public DocSet andNot(DocSet other) {
    if (other.size()==0) return this;

    if (!(other instanceof SortedIntDocSet)) {
      int count = 0;
      int arr[] = new int[docs.length];
      for (int i=0; i<docs.length; i++) {
        int doc = docs[i];
        if (!other.exists(doc)) arr[count++] = doc;
      }
      return new SortedIntDocSet(arr,count);
    }

    int[] otherDocs = ((SortedIntDocSet)other).docs;
    int[] arr = new int[docs.length];
    int sz = andNot(docs, docs.length, otherDocs, otherDocs.length, arr);
    return new SortedIntDocSet(arr,sz);
  }

  @Override
  public void addAllTo(DocSet target) {
    for (int doc : docs) {
      target.add(doc);
    }
  }

  @Override
  public boolean exists(int doc) {
    // this could be faster by estimating where in the list the doc is likely to appear,
    // but we should get away from using exists() anyway.
    int low = 0;
    int high = docs.length-1;
    // binary search
    while (low <= high) {
      int mid = (low+high) >>> 1;
      int docb = docs[mid];

      if (docb < doc) {
        low = mid+1;
      }
      else if (docb > doc) {
        high = mid-1;
      }
      else {
        return true;
      }
    }
    return false;
  }
  

  @Override
  public DocIterator iterator() {
    return new DocIterator() {
      int pos=0;
      @Override
      public boolean hasNext() {
        return pos < docs.length;
      }

      @Override
      public Integer next() {
        return nextDoc();
      }

      /**
       * The remove  operation is not supported by this Iterator.
       */
      @Override
      public void remove() {
        throw new UnsupportedOperationException("The remove  operation is not supported by this Iterator.");
      }

      @Override
      public int nextDoc() {
        return docs[pos++];
      }

      @Override
      public float score() {
        return 0.0f;
      }
    };
  }
  
  @Override
  public FixedBitSet getBits() {
    int maxDoc = size() > 0 ? docs[size()-1] : 0;
    FixedBitSet bs = new FixedBitSet(maxDoc+1);
    for (int doc : docs) {
      bs.set(doc);
    }
    return bs;
  }

  public static int findIndex(int[] arr, int value, int low, int high) {
    // binary search
    while (low <= high) {
      int mid = (low+high) >>> 1;
      int found = arr[mid];

      if (found < value) {
        low = mid+1;
      }
      else if (found > value) {
        high = mid-1;
      }
      else {
        return mid;
      }
    }
    return low;
  }

  @Override
  public Filter getTopFilter() {
    return new Filter() {
      int lastEndIdx = 0;

      @Override
      public DocIdSet getDocIdSet(final LeafReaderContext context, final Bits acceptDocs) {
        LeafReader reader = context.reader();
        // all Solr DocSets that are used as filters only include live docs
        final Bits acceptDocs2 = acceptDocs == null ? null : (reader.getLiveDocs() == acceptDocs ? null : acceptDocs);

        final int base = context.docBase;
        final int maxDoc = reader.maxDoc();
        final int max = base + maxDoc;   // one past the max doc in this segment.
        int sidx = Math.max(0,lastEndIdx);

        if (sidx > 0 && docs[sidx-1] >= base) {
          // oops, the lastEndIdx isn't correct... we must have been used
          // in a multi-threaded context, or the indexreaders are being
          // used out-of-order.  start at 0.
          sidx = 0;
        }
        if (sidx < docs.length && docs[sidx] < base) {
          // if docs[sidx] is < base, we need to seek to find the real start.
          sidx = findIndex(docs, base, sidx, docs.length-1);
        }

        final int startIdx = sidx;

        // Largest possible end index is limited to the start index
        // plus the number of docs contained in the segment.  Subtract 1 since
        // the end index is inclusive.
        int eidx = Math.min(docs.length, startIdx + maxDoc) - 1;

        // find the real end
        eidx = findIndex(docs, max, startIdx, eidx) - 1;

        final int endIdx = eidx;
        lastEndIdx = endIdx;


        return BitsFilteredDocIdSet.wrap(new DocIdSet() {
          @Override
          public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {
              int idx = startIdx;
              int adjustedDoc = -1;

              @Override
              public int docID() {
                return adjustedDoc;
              }

              @Override
              public int nextDoc() {
                return adjustedDoc = (idx > endIdx) ? NO_MORE_DOCS : (docs[idx++] - base);
              }

              @Override
              public int advance(int target) {
                if (idx > endIdx || target==NO_MORE_DOCS) return adjustedDoc=NO_MORE_DOCS;
                target += base;

                // probe next
                int rawDoc = docs[idx++];
                if (rawDoc >= target) return adjustedDoc=rawDoc-base;

                int high = endIdx;

                // TODO: probe more before resorting to binary search?

                // binary search
                while (idx <= high) {
                  int mid = (idx+high) >>> 1;
                  rawDoc = docs[mid];

                  if (rawDoc < target) {
                    idx = mid+1;
                  }
                  else if (rawDoc > target) {
                    high = mid-1;
                  }
                  else {
                    idx=mid+1;
                    return adjustedDoc=rawDoc - base;
                  }
                }

                // low is on the insertion point...
                if (idx <= endIdx) {
                  return adjustedDoc = docs[idx++] - base;
                } else {
                  return adjustedDoc=NO_MORE_DOCS;
                }
              }

              @Override
              public long cost() {
                return docs.length;
              }
            };
          }

          @Override
          public boolean isCacheable() {
            return true;
          }

          @Override
          public long ramBytesUsed() {
            return RamUsageEstimator.sizeOf(docs);
          }
          
          @Override
          public Bits bits() {
            // random access is expensive for this set
            return null;
          }

        }, acceptDocs2);
      }
    };
  }

  @Override
  protected SortedIntDocSet clone() {
    return new SortedIntDocSet(docs.clone());
  }
}
