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

package org.apache.lucene.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.codecs.PostingsFormat; // javadocs
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.StringHelper;

/**
 * This class enables fast access to multiple term ords for
 * a specified field across all docIDs.
 *
 * Like FieldCache, it uninverts the index and holds a
 * packed data structure in RAM to enable fast access.
 * Unlike FieldCache, it can handle multi-valued fields,
 * and, it does not hold the term bytes in RAM.  Rather, you
 * must obtain a TermsEnum from the {@link #getOrdTermsEnum}
 * method, and then seek-by-ord to get the term's bytes.
 *
 * While normally term ords are type long, in this API they are
 * int as the internal representation here cannot address
 * more than MAX_INT unique terms.  Also, typically this
 * class is used on fields with relatively few unique terms
 * vs the number of documents.  In addition, there is an
 * internal limit (16 MB) on how many bytes each chunk of
 * documents may consume.  If you trip this limit you'll hit
 * an IllegalStateException.
 *
 * Deleted documents are skipped during uninversion, and if
 * you look them up you'll get 0 ords.
 *
 * The returned per-document ords do not retain their
 * original order in the document.  Instead they are returned
 * in sorted (by ord, ie term's BytesRef comparator) order.  They
 * are also de-dup'd (ie if doc has same term more than once
 * in this field, you'll only get that ord back once).
 *
 * This class tests whether the provided reader is able to
 * retrieve terms by ord (ie, it's single segment, and it
 * uses an ord-capable terms index).  If not, this class
 * will create its own term index internally, allowing to
 * create a wrapped TermsEnum that can handle ord.  The
 * {@link #getOrdTermsEnum} method then provides this
 * wrapped enum, if necessary.
 *
 * The RAM consumption of this class can be high!
 *
 * @lucene.experimental
 */

/*
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

public class DocTermOrds {

  // Term ords are shifted by this, internally, to reserve
  // values 0 (end term) and 1 (index is a pointer into byte array)
  private final static int TNUM_OFFSET = 2;

  /** Every 128th term is indexed, by default. */
  public final static int DEFAULT_INDEX_INTERVAL_BITS = 7; // decrease to a low number like 2 for testing

  private int indexIntervalBits;
  private int indexIntervalMask;
  private int indexInterval;

  /** Don't uninvert terms that exceed this count. */
  protected final int maxTermDocFreq;

  /** Field we are uninverting. */
  protected final String field;

  /** Number of terms in the field. */
  protected int numTermsInField;

  /** Total number of references to term numbers. */
  protected long termInstances;
  private long memsz;

  /** Total time to uninvert the field. */
  protected int total_time;

  /** Time for phase1 of the uninvert process. */
  protected int phase1_time;

  /** Holds the per-document ords or a pointer to the ords. */
  protected int[] index;

  /** Holds term ords for documents. */
  protected byte[][] tnums = new byte[256][];

  /** Total bytes (sum of term lengths) for all indexed terms.*/
  protected long sizeOfIndexedStrings;

  /** Holds the indexed (by default every 128th) terms. */
  protected BytesRef[] indexedTermsArray;

  /** If non-null, only terms matching this prefix were
   *  indexed. */
  protected BytesRef prefix;

  /** Ordinal of the first term in the field, or 0 if the
   *  {@link PostingsFormat} does not implement {@link
   *  TermsEnum#ord}. */
  protected int ordBase;

  /** Used while uninverting. */
  protected DocsEnum docsEnum;

  /** Returns total bytes used. */
  public long ramUsedInBytes() {
    // can cache the mem size since it shouldn't change
    if (memsz!=0) return memsz;
    long sz = 8*8 + 32; // local fields
    if (index != null) sz += index.length * 4;
    if (tnums!=null) {
      for (byte[] arr : tnums)
        if (arr != null) sz += arr.length;
    }
    memsz = sz;
    return sz;
  }

  /** Inverts all terms */
  public DocTermOrds(AtomicReader reader, Bits liveDocs, String field) throws IOException {
    this(reader, liveDocs, field, null, Integer.MAX_VALUE);
  }

  /** Inverts only terms starting w/ prefix */
  public DocTermOrds(AtomicReader reader, Bits liveDocs, String field, BytesRef termPrefix) throws IOException {
    this(reader, liveDocs, field, termPrefix, Integer.MAX_VALUE);
  }

  /** Inverts only terms starting w/ prefix, and only terms
   *  whose docFreq (not taking deletions into account) is
   *  <=  maxTermDocFreq */
  public DocTermOrds(AtomicReader reader, Bits liveDocs, String field, BytesRef termPrefix, int maxTermDocFreq) throws IOException {
    this(reader, liveDocs, field, termPrefix, maxTermDocFreq, DEFAULT_INDEX_INTERVAL_BITS);
  }

  /** Inverts only terms starting w/ prefix, and only terms
   *  whose docFreq (not taking deletions into account) is
   *  <=  maxTermDocFreq, with a custom indexing interval
   *  (default is every 128nd term). */
  public DocTermOrds(AtomicReader reader, Bits liveDocs, String field, BytesRef termPrefix, int maxTermDocFreq, int indexIntervalBits) throws IOException {
    this(field, maxTermDocFreq, indexIntervalBits);
    uninvert(reader, liveDocs, termPrefix);
  }

  /** Subclass inits w/ this, but be sure you then call
   *  uninvert, only once */
  protected DocTermOrds(String field, int maxTermDocFreq, int indexIntervalBits) {
    //System.out.println("DTO init field=" + field + " maxTDFreq=" + maxTermDocFreq);
    this.field = field;
    this.maxTermDocFreq = maxTermDocFreq;
    this.indexIntervalBits = indexIntervalBits;
    indexIntervalMask = 0xffffffff >>> (32-indexIntervalBits);
    indexInterval = 1 << indexIntervalBits;
  }

  /** Returns a TermsEnum that implements ord.  If the
   *  provided reader supports ord, we just return its
   *  TermsEnum; if it does not, we build a "private" terms
   *  index internally (WARNING: consumes RAM) and use that
   *  index to implement ord.  This also enables ord on top
   *  of a composite reader.  The returned TermsEnum is
   *  unpositioned.  This returns null if there are no terms.
   *
   *  <p><b>NOTE</b>: you must pass the same reader that was
   *  used when creating this class */
  public TermsEnum getOrdTermsEnum(AtomicReader reader) throws IOException {
    if (indexedTermsArray == null) {
      //System.out.println("GET normal enum");
      final Fields fields = reader.fields();
      if (fields == null) {
        return null;
      }
      final Terms terms = fields.terms(field);
      if (terms == null) {
        return null;
      } else {
        return terms.iterator(null);
      }
    } else {
      //System.out.println("GET wrapped enum ordBase=" + ordBase);
      return new OrdWrappedTermsEnum(reader);
    }
  }

  /**
   * Returns the number of terms in this field
   */
  public int numTerms() {
    return numTermsInField;
  }

  /**
   * Returns {@code true} if no terms were indexed.
   */
  public boolean isEmpty() {
    return index == null;
  }

  /** Subclass can override this */
  protected void visitTerm(TermsEnum te, int termNum) throws IOException {
  }

  /** Invoked during {@link #uninvert(AtomicReader,Bits,BytesRef)}
   *  to record the document frequency for each uninverted
   *  term. */
  protected void setActualDocFreq(int termNum, int df) throws IOException {
  }

  /** Call this only once (if you subclass!) */
  protected void uninvert(final AtomicReader reader, Bits liveDocs, final BytesRef termPrefix) throws IOException {
    final FieldInfo info = reader.getFieldInfos().fieldInfo(field);
    if (info != null && info.hasDocValues()) {
      throw new IllegalStateException("Type mismatch: " + field + " was indexed as " + info.getDocValuesType());
    }
    //System.out.println("DTO uninvert field=" + field + " prefix=" + termPrefix);
    final long startTime = System.currentTimeMillis();
    prefix = termPrefix == null ? null : BytesRef.deepCopyOf(termPrefix);

    final int maxDoc = reader.maxDoc();
    final int[] index = new int[maxDoc];       // immediate term numbers, or the index into the byte[] representing the last number
    final int[] lastTerm = new int[maxDoc];    // last term we saw for this document
    final byte[][] bytes = new byte[maxDoc][]; // list of term numbers for the doc (delta encoded vInts)

    final Fields fields = reader.fields();
    if (fields == null) {
      // No terms
      return;
    }
    final Terms terms = fields.terms(field);
    if (terms == null) {
      // No terms
      return;
    }

    final TermsEnum te = terms.iterator(null);
    final BytesRef seekStart = termPrefix != null ? termPrefix : new BytesRef();
    //System.out.println("seekStart=" + seekStart.utf8ToString());
    if (te.seekCeil(seekStart) == TermsEnum.SeekStatus.END) {
      // No terms match
      return;
    }

    // If we need our "term index wrapper", these will be
    // init'd below:
    List<BytesRef> indexedTerms = null;
    PagedBytes indexedTermsBytes = null;

    boolean testedOrd = false;

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

    int termNum = 0;
    docsEnum = null;

    // Loop begins with te positioned to first term (we call
    // seek above):
    for (;;) {
      final BytesRef t = te.term();
      if (t == null || (termPrefix != null && !StringHelper.startsWith(t, termPrefix))) {
        break;
      }
      //System.out.println("visit term=" + t.utf8ToString() + " " + t + " termNum=" + termNum);

      if (!testedOrd) {
        try {
          ordBase = (int) te.ord();
          //System.out.println("got ordBase=" + ordBase);
        } catch (UnsupportedOperationException uoe) {
          // Reader cannot provide ord support, so we wrap
          // our own support by creating our own terms index:
          indexedTerms = new ArrayList<BytesRef>();
          indexedTermsBytes = new PagedBytes(15);
          //System.out.println("NO ORDS");
        }
        testedOrd = true;
      }

      visitTerm(te, termNum);

      if (indexedTerms != null && (termNum & indexIntervalMask) == 0) {
        // Index this term
        sizeOfIndexedStrings += t.length;
        BytesRef indexedTerm = new BytesRef();
        indexedTermsBytes.copy(t, indexedTerm);
        // TODO: really should 1) strip off useless suffix,
        // and 2) use FST not array/PagedBytes
        indexedTerms.add(indexedTerm);
      }

      final int df = te.docFreq();
      if (df <= maxTermDocFreq) {

        docsEnum = te.docs(liveDocs, docsEnum, DocsEnum.FLAG_NONE);

        // dF, but takes deletions into account
        int actualDF = 0;

        for (;;) {
          int doc = docsEnum.nextDoc();
          if (doc == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
          //System.out.println("  chunk=" + chunk + " docs");

          actualDF ++;
          termInstances++;
          
          //System.out.println("    docID=" + doc);
          // add TNUM_OFFSET to the term number to make room for special reserved values:
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

            //System.out.println("      ipos=" + ipos);

            int endPos = writeInt(delta, tempArr, ipos);
            //System.out.println("      endpos=" + endPos);
            if (endPos <= 4) {
              //System.out.println("      fits!");
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
        setActualDocFreq(termNum, actualDF);
      }

      termNum++;
      if (te.next() == null) {
        break;
      }
    }

    numTermsInField = termNum;

    long midPoint = System.currentTimeMillis();

    if (termInstances == 0) {
      // we didn't invert anything
      // lower memory consumption.
      tnums = null;
    } else {

      this.index = index;

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
            //System.out.println("  pass=" + pass + " process docID=" + doc);
            int val = index[doc];
            if ((val&0xff) == 1) {
              int len = val >>> 8;
              //System.out.println("    ptr pos=" + pos);
              index[doc] = (pos<<8)|1; // change index to point to start of array
              if ((pos & 0xff000000) != 0) {
                // we only have 24 bits for the array index
                throw new IllegalStateException("Too many values for UnInvertedField faceting on field "+field);
              }
              byte[] arr = bytes[doc];
              /*
              for(byte b : arr) {
                //System.out.println("      b=" + Integer.toHexString((int) b));
              }
              */
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
        }
        
        tnums[pass] = target;

        if ((pass << 16) > maxDoc)
          break;
      }

    }
    if (indexedTerms != null) {
      indexedTermsArray = indexedTerms.toArray(new BytesRef[indexedTerms.size()]);
    }

    long endTime = System.currentTimeMillis();

    total_time = (int)(endTime-startTime);
    phase1_time = (int)(midPoint-startTime);
  }

  /** Number of bytes to represent an unsigned int as a vint. */
  private static int vIntSize(int x) {
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
  private static int writeInt(int x, byte[] arr, int pos) {
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

  /* Only used if original IndexReader doesn't implement
   * ord; in this case we "wrap" our own terms index
   * around it. */
  private final class OrdWrappedTermsEnum extends TermsEnum {
    private final TermsEnum termsEnum;
    private BytesRef term;
    private long ord = -indexInterval-1;          // force "real" seek
    
    public OrdWrappedTermsEnum(AtomicReader reader) throws IOException {
      assert indexedTermsArray != null;
      termsEnum = reader.fields().terms(field).iterator(null);
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return termsEnum.getComparator();
    }

    @Override    
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
      return termsEnum.docs(liveDocs, reuse, flags);
    }

    @Override    
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
      return termsEnum.docsAndPositions(liveDocs, reuse, flags);
    }

    @Override
    public BytesRef term() {
      return term;
    }

    @Override
    public BytesRef next() throws IOException {
      if (++ord < 0) {
        ord = 0;
      }
      if (termsEnum.next() == null) {
        term = null;
        return null;
      }
      return setTerm();  // this is extra work if we know we are in bounds...
    }

    @Override
    public int docFreq() throws IOException {
      return termsEnum.docFreq();
    }

    @Override
    public long totalTermFreq() throws IOException {
      return termsEnum.totalTermFreq();
    }

    @Override
    public long ord() {
      return ordBase + ord;
    }

    @Override
    public SeekStatus seekCeil(BytesRef target, boolean useCache) throws IOException {

      // already here
      if (term != null && term.equals(target)) {
        return SeekStatus.FOUND;
      }

      int startIdx = Arrays.binarySearch(indexedTermsArray, target);

      if (startIdx >= 0) {
        // we hit the term exactly... lucky us!
        TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(target);
        assert seekStatus == TermsEnum.SeekStatus.FOUND;
        ord = startIdx << indexIntervalBits;
        setTerm();
        assert term != null;
        return SeekStatus.FOUND;
      }

      // we didn't hit the term exactly
      startIdx = -startIdx-1;
    
      if (startIdx == 0) {
        // our target occurs *before* the first term
        TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(target);
        assert seekStatus == TermsEnum.SeekStatus.NOT_FOUND;
        ord = 0;
        setTerm();
        assert term != null;
        return SeekStatus.NOT_FOUND;
      }

      // back up to the start of the block
      startIdx--;

      if ((ord >> indexIntervalBits) == startIdx && term != null && term.compareTo(target) <= 0) {
        // we are already in the right block and the current term is before the term we want,
        // so we don't need to seek.
      } else {
        // seek to the right block
        TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(indexedTermsArray[startIdx]);
        assert seekStatus == TermsEnum.SeekStatus.FOUND;
        ord = startIdx << indexIntervalBits;
        setTerm();
        assert term != null;  // should be non-null since it's in the index
      }

      while (term != null && term.compareTo(target) < 0) {
        next();
      }

      if (term == null) {
        return SeekStatus.END;
      } else if (term.compareTo(target) == 0) {
        return SeekStatus.FOUND;
      } else {
        return SeekStatus.NOT_FOUND;
      }
    }

    @Override
    public void seekExact(long targetOrd) throws IOException {
      int delta = (int) (targetOrd - ordBase - ord);
      //System.out.println("  seek(ord) targetOrd=" + targetOrd + " delta=" + delta + " ord=" + ord + " ii=" + indexInterval);
      if (delta < 0 || delta > indexInterval) {
        final int idx = (int) (targetOrd >>> indexIntervalBits);
        final BytesRef base = indexedTermsArray[idx];
        //System.out.println("  do seek term=" + base.utf8ToString());
        ord = idx << indexIntervalBits;
        delta = (int) (targetOrd - ord);
        final TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(base, true);
        assert seekStatus == TermsEnum.SeekStatus.FOUND;
      } else {
        //System.out.println("seek w/in block");
      }

      while (--delta >= 0) {
        BytesRef br = termsEnum.next();
        if (br == null) {
          assert false;
          return;
        }
        ord++;
      }

      setTerm();
      assert term != null;
    }

    private BytesRef setTerm() throws IOException {
      term = termsEnum.term();
      //System.out.println("  setTerm() term=" + term.utf8ToString() + " vs prefix=" + (prefix == null ? "null" : prefix.utf8ToString()));
      if (prefix != null && !StringHelper.startsWith(term, prefix)) {
        term = null;
      }
      return term;
    }
  }

  /** Returns the term ({@link BytesRef}) corresponding to
   *  the provided ordinal. */
  public BytesRef lookupTerm(TermsEnum termsEnum, int ord) throws IOException {
    termsEnum.seekExact(ord);
    return termsEnum.term();
  }
  
  /** Returns a SortedSetDocValues view of this instance */
  public SortedSetDocValues iterator(AtomicReader reader) throws IOException {
    if (isEmpty()) {
      return SortedSetDocValues.EMPTY;
    } else {
      return new Iterator(reader);
    }
  }
  
  private class Iterator extends SortedSetDocValues {
    final AtomicReader reader;
    final TermsEnum te;  // used internally for lookupOrd() and lookupTerm()
    // currently we read 5 at a time (using the logic of the old iterator)
    final int buffer[] = new int[5];
    int bufferUpto;
    int bufferLength;
    
    private int tnum;
    private int upto;
    private byte[] arr;
    
    Iterator(AtomicReader reader) throws IOException {
      this.reader = reader;
      this.te = termsEnum();
    }
    
    @Override
    public long nextOrd() {
      while (bufferUpto == bufferLength) {
        if (bufferLength < buffer.length) {
          return NO_MORE_ORDS;
        } else {
          bufferLength = read(buffer);
          bufferUpto = 0;
        }
      }
      return buffer[bufferUpto++];
    }
    
    /** Buffer must be at least 5 ints long.  Returns number
     *  of term ords placed into buffer; if this count is
     *  less than buffer.length then that is the end. */
    int read(int[] buffer) {
      int bufferUpto = 0;
      if (arr == null) {
        // code is inlined into upto
        //System.out.println("inlined");
        int code = upto;
        int delta = 0;
        for (;;) {
          delta = (delta << 7) | (code & 0x7f);
          if ((code & 0x80)==0) {
            if (delta==0) break;
            tnum += delta - TNUM_OFFSET;
            buffer[bufferUpto++] = ordBase+tnum;
            //System.out.println("  tnum=" + tnum);
            delta = 0;
          }
          code >>>= 8;
        }
      } else {
        // code is a pointer
        for(;;) {
          int delta = 0;
          for(;;) {
            byte b = arr[upto++];
            delta = (delta << 7) | (b & 0x7f);
            //System.out.println("    cycle: upto=" + upto + " delta=" + delta + " b=" + b);
            if ((b & 0x80) == 0) break;
          }
          //System.out.println("  delta=" + delta);
          if (delta == 0) break;
          tnum += delta - TNUM_OFFSET;
          //System.out.println("  tnum=" + tnum);
          buffer[bufferUpto++] = ordBase+tnum;
          if (bufferUpto == buffer.length) {
            break;
          }
        }
      }

      return bufferUpto;
    }

    @Override
    public void setDocument(int docID) {
      tnum = 0;
      final int code = index[docID];
      if ((code & 0xff)==1) {
        // a pointer
        upto = code>>>8;
        //System.out.println("    pointer!  upto=" + upto);
        int whichArray = (docID >>> 16) & 0xff;
        arr = tnums[whichArray];
      } else {
        //System.out.println("    inline!");
        arr = null;
        upto = code;
      }
      bufferUpto = 0;
      bufferLength = read(buffer);
    }

    @Override
    public void lookupOrd(long ord, BytesRef result) {
      BytesRef ref = null;
      try {
        ref = DocTermOrds.this.lookupTerm(te, (int) ord);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      result.bytes = ref.bytes;
      result.offset = ref.offset;
      result.length = ref.length;
    }

    @Override
    public long getValueCount() {
      return numTerms();
    }

    @Override
    public long lookupTerm(BytesRef key) {
      try {
        if (te.seekCeil(key) == SeekStatus.FOUND) {
          return te.ord();
        } else {
          return -te.ord()-1;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    @Override
    public TermsEnum termsEnum() {    
      try {
        return getOrdTermsEnum(reader);
      } catch (IOException e) {
        throw new RuntimeException();
      }
    }
  }
}
