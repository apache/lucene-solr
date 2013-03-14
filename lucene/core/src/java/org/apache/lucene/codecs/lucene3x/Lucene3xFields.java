package org.apache.lucene.codecs.lucene3x;

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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.UnicodeUtil;

/** Exposes flex API on a pre-flex index, as a codec. 
 * @lucene.experimental
 * @deprecated (4.0)
 */
@Deprecated
class Lucene3xFields extends FieldsProducer {
  
  private static final boolean DEBUG_SURROGATES = false;

  public TermInfosReader tis;
  public final TermInfosReader tisNoIndex;

  public final IndexInput freqStream;
  public final IndexInput proxStream;
  final private FieldInfos fieldInfos;
  private final SegmentInfo si;
  final TreeMap<String,FieldInfo> fields = new TreeMap<String,FieldInfo>();
  final Map<String,Terms> preTerms = new HashMap<String,Terms>();
  private final Directory dir;
  private final IOContext context;
  private Directory cfsReader;

  public Lucene3xFields(Directory dir, FieldInfos fieldInfos, SegmentInfo info, IOContext context, int indexDivisor)
    throws IOException {

    si = info;

    // NOTE: we must always load terms index, even for
    // "sequential" scan during merging, because what is
    // sequential to merger may not be to TermInfosReader
    // since we do the surrogates dance:
    if (indexDivisor < 0) {
      indexDivisor = -indexDivisor;
    }
    
    boolean success = false;
    try {
      TermInfosReader r = new TermInfosReader(dir, info.name, fieldInfos, context, indexDivisor);    
      if (indexDivisor == -1) {
        tisNoIndex = r;
      } else {
        tisNoIndex = null;
        tis = r;
      }
      this.context = context;
      this.fieldInfos = fieldInfos;

      // make sure that all index files have been read or are kept open
      // so that if an index update removes them we'll still have them
      freqStream = dir.openInput(IndexFileNames.segmentFileName(info.name, "", Lucene3xPostingsFormat.FREQ_EXTENSION), context);
      boolean anyProx = false;
      for (FieldInfo fi : fieldInfos) {
        if (fi.isIndexed()) {
          fields.put(fi.name, fi);
          preTerms.put(fi.name, new PreTerms(fi));
          if (fi.getIndexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
            anyProx = true;
          }
        }
      }

      if (anyProx) {
        proxStream = dir.openInput(IndexFileNames.segmentFileName(info.name, "", Lucene3xPostingsFormat.PROX_EXTENSION), context);
      } else {
        proxStream = null;
      }
      success = true;
    } finally {
      // With lock-less commits, it's entirely possible (and
      // fine) to hit a FileNotFound exception above. In
      // this case, we want to explicitly close any subset
      // of things that were opened so that we don't have to
      // wait for a GC to do so.
      if (!success) {
        close();
      }
    }
    this.dir = dir;
  }

  // If this returns, we do the surrogates dance so that the
  // terms are sorted by unicode sort order.  This should be
  // true when segments are used for "normal" searching;
  // it's only false during testing, to create a pre-flex
  // index, using the test-only PreFlexRW.
  protected boolean sortTermsByUnicode() {
    return true;
  }

  @Override
  public Iterator<String> iterator() {
    return Collections.unmodifiableSet(fields.keySet()).iterator();
  }

  @Override
  public Terms terms(String field) {
    return preTerms.get(field);
  }

  @Override
  public int size() {
    assert preTerms.size() == fields.size();
    return fields.size();
  }

  @Override
  public long getUniqueTermCount() throws IOException {
    return getTermsDict().size();
  }

  synchronized private TermInfosReader getTermsDict() {
    if (tis != null) {
      return tis;
    } else {
      return tisNoIndex;
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(tis, tisNoIndex, cfsReader, freqStream, proxStream);
  }
  
  private class PreTerms extends Terms {
    final FieldInfo fieldInfo;
    PreTerms(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
    }

    @Override
    public TermsEnum iterator(TermsEnum reuse) throws IOException {    
      PreTermsEnum termsEnum = new PreTermsEnum();
      termsEnum.reset(fieldInfo);
      return termsEnum;
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      // Pre-flex indexes always sorted in UTF16 order, but
      // we remap on-the-fly to unicode order
      if (sortTermsByUnicode()) {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
      } else {
        return BytesRef.getUTF8SortedAsUTF16Comparator();
      }
    }

    @Override
    public long size() throws IOException {
      return -1;
    }

    @Override
    public long getSumTotalTermFreq() {
      return -1;
    }

    @Override
    public long getSumDocFreq() throws IOException {
      return -1;
    }

    @Override
    public int getDocCount() throws IOException {
      return -1;
    }

    @Override
    public boolean hasOffsets() {
      // preflex doesn't support this
      assert fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) < 0;
      return false;
    }

    @Override
    public boolean hasPositions() {
      return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }

    @Override
    public boolean hasPayloads() {
      return fieldInfo.hasPayloads();
    }
  }

  private class PreTermsEnum extends TermsEnum {
    private SegmentTermEnum termEnum;
    private FieldInfo fieldInfo;
    private String internedFieldName;
    private boolean skipNext;
    private BytesRef current;

    private SegmentTermEnum seekTermEnum;
    
    private static final byte UTF8_NON_BMP_LEAD = (byte) 0xf0;
    private static final byte UTF8_HIGH_BMP_LEAD = (byte) 0xee;

    // Returns true if the unicode char is "after" the
    // surrogates in UTF16, ie >= U+E000 and <= U+FFFF:
    private final boolean isHighBMPChar(byte[] b, int idx) {
      return (b[idx] & UTF8_HIGH_BMP_LEAD) == UTF8_HIGH_BMP_LEAD;
    }

    // Returns true if the unicode char in the UTF8 byte
    // sequence starting at idx encodes a char outside of
    // BMP (ie what would be a surrogate pair in UTF16):
    private final boolean isNonBMPChar(byte[] b, int idx) {
      return (b[idx] & UTF8_NON_BMP_LEAD) == UTF8_NON_BMP_LEAD;
    }

    private final byte[] scratch = new byte[4];
    private final BytesRef prevTerm = new BytesRef();
    private final BytesRef scratchTerm = new BytesRef();
    private int newSuffixStart;

    // Swap in S, in place of E:
    private boolean seekToNonBMP(SegmentTermEnum te, BytesRef term, int pos) throws IOException {
      final int savLength = term.length;

      assert term.offset == 0;

      // The 3 bytes starting at downTo make up 1
      // unicode character:
      assert isHighBMPChar(term.bytes, pos);

      // NOTE: we cannot make this assert, because
      // AutomatonQuery legitimately sends us malformed UTF8
      // (eg the UTF8 bytes with just 0xee)
      // assert term.length >= pos + 3: "term.length=" + term.length + " pos+3=" + (pos+3) + " byte=" + Integer.toHexString(term.bytes[pos]) + " term=" + term.toString();

      // Save the bytes && length, since we need to
      // restore this if seek "back" finds no matching
      // terms
      if (term.bytes.length < 4+pos) {
        term.grow(4+pos);
      }

      scratch[0] = term.bytes[pos];
      scratch[1] = term.bytes[pos+1];
      scratch[2] = term.bytes[pos+2];

      term.bytes[pos] = (byte) 0xf0;
      term.bytes[pos+1] = (byte) 0x90;
      term.bytes[pos+2] = (byte) 0x80;
      term.bytes[pos+3] = (byte) 0x80;
      term.length = 4+pos;

      if (DEBUG_SURROGATES) {
        System.out.println("      try seek term=" + UnicodeUtil.toHexString(term.utf8ToString()));
      }

      // Seek "back":
      getTermsDict().seekEnum(te, new Term(fieldInfo.name, term), true);

      // Test if the term we seek'd to in fact found a
      // surrogate pair at the same position as the E:
      Term t2 = te.term();

      // Cannot be null (or move to next field) because at
      // "worst" it'd seek to the same term we are on now,
      // unless we are being called from seek
      if (t2 == null || t2.field() != internedFieldName) {
        return false;
      }

      if (DEBUG_SURROGATES) {
        System.out.println("      got term=" + UnicodeUtil.toHexString(t2.text()));
      }

      // Now test if prefix is identical and we found
      // a non-BMP char at the same position:
      BytesRef b2 = t2.bytes();
      assert b2.offset == 0;

      boolean matches;
      if (b2.length >= term.length && isNonBMPChar(b2.bytes, pos)) {
        matches = true;
        for(int i=0;i<pos;i++) {
          if (term.bytes[i] != b2.bytes[i]) {
            matches = false;
            break;
          }
        }              
      } else {
        matches = false;
      }

      // Restore term:
      term.length = savLength;
      term.bytes[pos] = scratch[0];
      term.bytes[pos+1] = scratch[1];
      term.bytes[pos+2] = scratch[2];

      return matches;
    }

    // Seek type 2 "continue" (back to the start of the
    // surrogates): scan the stripped suffix from the
    // prior term, backwards. If there was an E in that
    // part, then we try to seek back to S.  If that
    // seek finds a matching term, we go there.
    private boolean doContinue() throws IOException {

      if (DEBUG_SURROGATES) {
        System.out.println("  try cont");
      }

      int downTo = prevTerm.length-1;

      boolean didSeek = false;
      
      final int limit = Math.min(newSuffixStart, scratchTerm.length-1);

      while(downTo > limit) {

        if (isHighBMPChar(prevTerm.bytes, downTo)) {

          if (DEBUG_SURROGATES) {
            System.out.println("    found E pos=" + downTo + " vs len=" + prevTerm.length);
          }

          if (seekToNonBMP(seekTermEnum, prevTerm, downTo)) {
            // TODO: more efficient seek?
            getTermsDict().seekEnum(termEnum, seekTermEnum.term(), true);
            //newSuffixStart = downTo+4;
            newSuffixStart = downTo;
            scratchTerm.copyBytes(termEnum.term().bytes());
            didSeek = true;
            if (DEBUG_SURROGATES) {
              System.out.println("      seek!");
            }
            break;
          } else {
            if (DEBUG_SURROGATES) {
              System.out.println("      no seek");
            }
          }
        }

        // Shorten prevTerm in place so that we don't redo
        // this loop if we come back here:
        if ((prevTerm.bytes[downTo] & 0xc0) == 0xc0 || (prevTerm.bytes[downTo] & 0x80) == 0) {
          prevTerm.length = downTo;
        }
        
        downTo--;
      }

      return didSeek;
    }

    // Look for seek type 3 ("pop"): if the delta from
    // prev -> current was replacing an S with an E,
    // we must now seek to beyond that E.  This seek
    // "finishes" the dance at this character
    // position.
    private boolean doPop() throws IOException {

      if (DEBUG_SURROGATES) {
        System.out.println("  try pop");
      }

      assert newSuffixStart <= prevTerm.length;
      assert newSuffixStart < scratchTerm.length || newSuffixStart == 0;

      if (prevTerm.length > newSuffixStart &&
          isNonBMPChar(prevTerm.bytes, newSuffixStart) &&
          isHighBMPChar(scratchTerm.bytes, newSuffixStart)) {

        // Seek type 2 -- put 0xFF at this position:
        scratchTerm.bytes[newSuffixStart] = (byte) 0xff;
        scratchTerm.length = newSuffixStart+1;

        if (DEBUG_SURROGATES) {
          System.out.println("    seek to term=" + UnicodeUtil.toHexString(scratchTerm.utf8ToString()) + " " + scratchTerm.toString());
        }
          
        // TODO: more efficient seek?  can we simply swap
        // the enums?
        getTermsDict().seekEnum(termEnum, new Term(fieldInfo.name, scratchTerm), true);

        final Term t2 = termEnum.term();

        // We could hit EOF or different field since this
        // was a seek "forward":
        if (t2 != null && t2.field() == internedFieldName) {

          if (DEBUG_SURROGATES) {
            System.out.println("      got term=" + UnicodeUtil.toHexString(t2.text()) + " " + t2.bytes());
          }

          final BytesRef b2 = t2.bytes();
          assert b2.offset == 0;


          // Set newSuffixStart -- we can't use
          // termEnum's since the above seek may have
          // done no scanning (eg, term was precisely
          // and index term, or, was in the term seek
          // cache):
          scratchTerm.copyBytes(b2);
          setNewSuffixStart(prevTerm, scratchTerm);

          return true;
        } else if (newSuffixStart != 0 || scratchTerm.length != 0) {
          if (DEBUG_SURROGATES) {
            System.out.println("      got term=null (or next field)");
          }
          newSuffixStart = 0;
          scratchTerm.length = 0;
          return true;
        }
      }

      return false;
    }

    // Pre-flex indices store terms in UTF16 sort order, but
    // certain queries require Unicode codepoint order; this
    // method carefully seeks around surrogates to handle
    // this impedance mismatch

    private void surrogateDance() throws IOException {

      if (!unicodeSortOrder) {
        return;
      }

      // We are invoked after TIS.next() (by UTF16 order) to
      // possibly seek to a different "next" (by unicode
      // order) term.

      // We scan only the "delta" from the last term to the
      // current term, in UTF8 bytes.  We look at 1) the bytes
      // stripped from the prior term, and then 2) the bytes
      // appended to that prior term's prefix.
    
      // We don't care about specific UTF8 sequences, just
      // the "category" of the UTF16 character.  Category S
      // is a high/low surrogate pair (it non-BMP).
      // Category E is any BMP char > UNI_SUR_LOW_END (and <
      // U+FFFF). Category A is the rest (any unicode char
      // <= UNI_SUR_HIGH_START).

      // The core issue is that pre-flex indices sort the
      // characters as ASE, while flex must sort as AES.  So
      // when scanning, when we hit S, we must 1) seek
      // forward to E and enum the terms there, then 2) seek
      // back to S and enum all terms there, then 3) seek to
      // after E.  Three different seek points (1, 2, 3).
    
      // We can easily detect S in UTF8: if a byte has
      // prefix 11110 (0xf0), then that byte and the
      // following 3 bytes encode a single unicode codepoint
      // in S.  Similarly, we can detect E: if a byte has
      // prefix 1110111 (0xee), then that byte and the
      // following 2 bytes encode a single unicode codepoint
      // in E.

      // Note that this is really a recursive process --
      // maybe the char at pos 2 needs to dance, but any
      // point in its dance, suddenly pos 4 needs to dance
      // so you must finish pos 4 before returning to pos
      // 2.  But then during pos 4's dance maybe pos 7 needs
      // to dance, etc.  However, despite being recursive,
      // we don't need to hold any state because the state
      // can always be derived by looking at prior term &
      // current term.

      // TODO: can we avoid this copy?
      if (termEnum.term() == null || termEnum.term().field() != internedFieldName) {
        scratchTerm.length = 0;
      } else {
        scratchTerm.copyBytes(termEnum.term().bytes());
      }
      
      if (DEBUG_SURROGATES) {
        System.out.println("  dance");
        System.out.println("    prev=" + UnicodeUtil.toHexString(prevTerm.utf8ToString()));
        System.out.println("         " + prevTerm.toString());
        System.out.println("    term=" + UnicodeUtil.toHexString(scratchTerm.utf8ToString()));
        System.out.println("         " + scratchTerm.toString());
      }

      // This code assumes TermInfosReader/SegmentTermEnum
      // always use BytesRef.offset == 0
      assert prevTerm.offset == 0;
      assert scratchTerm.offset == 0;

      // Need to loop here because we may need to do multiple
      // pops, and possibly a continue in the end, ie:
      //
      //  cont
      //  pop, cont
      //  pop, pop, cont
      //  <nothing>
      //

      while(true) {
        if (doContinue()) {
          break;
        } else {
          if (!doPop()) {
            break;
          }
        }
      }

      if (DEBUG_SURROGATES) {
        System.out.println("  finish bmp ends");
      }

      doPushes();
    }


    // Look for seek type 1 ("push"): if the newly added
    // suffix contains any S, we must try to seek to the
    // corresponding E.  If we find a match, we go there;
    // else we keep looking for additional S's in the new
    // suffix.  This "starts" the dance, at this character
    // position:
    private void doPushes() throws IOException {

      int upTo = newSuffixStart;
      if (DEBUG_SURROGATES) {
        System.out.println("  try push newSuffixStart=" + newSuffixStart + " scratchLen=" + scratchTerm.length);
      }

      while(upTo < scratchTerm.length) {
        if (isNonBMPChar(scratchTerm.bytes, upTo) &&
            (upTo > newSuffixStart ||
             (upTo >= prevTerm.length ||
              (!isNonBMPChar(prevTerm.bytes, upTo) &&
               !isHighBMPChar(prevTerm.bytes, upTo))))) {

          // A non-BMP char (4 bytes UTF8) starts here:
          assert scratchTerm.length >= upTo + 4;
          
          final int savLength = scratchTerm.length;
          scratch[0] = scratchTerm.bytes[upTo];
          scratch[1] = scratchTerm.bytes[upTo+1];
          scratch[2] = scratchTerm.bytes[upTo+2];

          scratchTerm.bytes[upTo] = UTF8_HIGH_BMP_LEAD;
          scratchTerm.bytes[upTo+1] = (byte) 0x80;
          scratchTerm.bytes[upTo+2] = (byte) 0x80;
          scratchTerm.length = upTo+3;

          if (DEBUG_SURROGATES) {
            System.out.println("    try seek 1 pos=" + upTo + " term=" + UnicodeUtil.toHexString(scratchTerm.utf8ToString()) + " " + scratchTerm.toString() + " len=" + scratchTerm.length);
          }

          // Seek "forward":
          // TODO: more efficient seek?
          getTermsDict().seekEnum(seekTermEnum, new Term(fieldInfo.name, scratchTerm), true);

          scratchTerm.bytes[upTo] = scratch[0];
          scratchTerm.bytes[upTo+1] = scratch[1];
          scratchTerm.bytes[upTo+2] = scratch[2];
          scratchTerm.length = savLength;

          // Did we find a match?
          final Term t2 = seekTermEnum.term();
            
          if (DEBUG_SURROGATES) {
            if (t2 == null) {
              System.out.println("      hit term=null");
            } else {
              System.out.println("      hit term=" + UnicodeUtil.toHexString(t2.text()) + " " + (t2==null? null:t2.bytes()));
            }
          }

          // Since this was a seek "forward", we could hit
          // EOF or a different field:
          boolean matches;

          if (t2 != null && t2.field() == internedFieldName) {
            final BytesRef b2 = t2.bytes();
            assert b2.offset == 0;
            if (b2.length >= upTo+3 && isHighBMPChar(b2.bytes, upTo)) {
              matches = true;
              for(int i=0;i<upTo;i++) {
                if (scratchTerm.bytes[i] != b2.bytes[i]) {
                  matches = false;
                  break;
                }
              }              
                
            } else {
              matches = false;
            }
          } else {
            matches = false;
          }

          if (matches) {

            if (DEBUG_SURROGATES) {
              System.out.println("      matches!");
            }

            // OK seek "back"
            // TODO: more efficient seek?
            getTermsDict().seekEnum(termEnum, seekTermEnum.term(), true);

            scratchTerm.copyBytes(seekTermEnum.term().bytes());

            // +3 because we don't need to check the char
            // at upTo: we know it's > BMP
            upTo += 3;

            // NOTE: we keep iterating, now, since this
            // can easily "recurse".  Ie, after seeking
            // forward at a certain char position, we may
            // find another surrogate in our [new] suffix
            // and must then do another seek (recurse)
          } else {
            upTo++;
          }
        } else {
          upTo++;
        }
      }
    }

    private boolean unicodeSortOrder;

    void reset(FieldInfo fieldInfo) throws IOException {
      //System.out.println("pff.reset te=" + termEnum);
      this.fieldInfo = fieldInfo;
      internedFieldName = fieldInfo.name.intern();
      final Term term = new Term(internedFieldName);
      if (termEnum == null) {
        termEnum = getTermsDict().terms(term);
        seekTermEnum = getTermsDict().terms(term);
        //System.out.println("  term=" + termEnum.term());
      } else {
        getTermsDict().seekEnum(termEnum, term, true);
      }
      skipNext = true;

      unicodeSortOrder = sortTermsByUnicode();

      final Term t = termEnum.term();
      if (t != null && t.field() == internedFieldName) {
        newSuffixStart = 0;
        prevTerm.length = 0;
        surrogateDance();
      }
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      // Pre-flex indexes always sorted in UTF16 order, but
      // we remap on-the-fly to unicode order
      if (unicodeSortOrder) {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
      } else {
        return BytesRef.getUTF8SortedAsUTF16Comparator();
      }
    }

    @Override
    public void seekExact(long ord) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SeekStatus seekCeil(BytesRef term, boolean useCache) throws IOException {
      if (DEBUG_SURROGATES) {
        System.out.println("TE.seek target=" + UnicodeUtil.toHexString(term.utf8ToString()));
      }
      skipNext = false;
      final TermInfosReader tis = getTermsDict();
      final Term t0 = new Term(fieldInfo.name, term);

      assert termEnum != null;

      tis.seekEnum(termEnum, t0, useCache);

      final Term t = termEnum.term();

      if (t != null && t.field() == internedFieldName && term.bytesEquals(t.bytes())) {
        // If we found an exact match, no need to do the
        // surrogate dance
        if (DEBUG_SURROGATES) {
          System.out.println("  seek exact match");
        }
        current = t.bytes();
        return SeekStatus.FOUND;
      } else if (t == null || t.field() != internedFieldName) {

        // TODO: maybe we can handle this like the next()
        // into null?  set term as prevTerm then dance?

        if (DEBUG_SURROGATES) {
          System.out.println("  seek hit EOF");
        }

        // We hit EOF; try end-case surrogate dance: if we
        // find an E, try swapping in S, backwards:
        scratchTerm.copyBytes(term);

        assert scratchTerm.offset == 0;

        for(int i=scratchTerm.length-1;i>=0;i--) {
          if (isHighBMPChar(scratchTerm.bytes, i)) {
            if (DEBUG_SURROGATES) {
              System.out.println("    found E pos=" + i + "; try seek");
            }

            if (seekToNonBMP(seekTermEnum, scratchTerm, i)) {

              scratchTerm.copyBytes(seekTermEnum.term().bytes());
              getTermsDict().seekEnum(termEnum, seekTermEnum.term(), useCache);

              newSuffixStart = 1+i;

              doPushes();

              // Found a match
              // TODO: faster seek?
              current = termEnum.term().bytes();
              return SeekStatus.NOT_FOUND;
            }
          }
        }
        
        if (DEBUG_SURROGATES) {
          System.out.println("  seek END");
        }

        current = null;
        return SeekStatus.END;
      } else {

        // We found a non-exact but non-null term; this one
        // is fun -- just treat it like next, by pretending
        // requested term was prev:
        prevTerm.copyBytes(term);

        if (DEBUG_SURROGATES) {
          System.out.println("  seek hit non-exact term=" + UnicodeUtil.toHexString(t.text()));
        }

        final BytesRef br = t.bytes();
        assert br.offset == 0;

        setNewSuffixStart(term, br);

        surrogateDance();

        final Term t2 = termEnum.term();
        if (t2 == null || t2.field() != internedFieldName) {
          // PreFlex codec interns field names; verify:
          assert t2 == null || !t2.field().equals(internedFieldName);
          current = null;
          return SeekStatus.END;
        } else {
          current = t2.bytes();
          assert !unicodeSortOrder || term.compareTo(current) < 0 : "term=" + UnicodeUtil.toHexString(term.utf8ToString()) + " vs current=" + UnicodeUtil.toHexString(current.utf8ToString());
          return SeekStatus.NOT_FOUND;
        }
      }
    }

    private void setNewSuffixStart(BytesRef br1, BytesRef br2) {
      final int limit = Math.min(br1.length, br2.length);
      int lastStart = 0;
      for(int i=0;i<limit;i++) {
        if ((br1.bytes[br1.offset+i] & 0xc0) == 0xc0 || (br1.bytes[br1.offset+i] & 0x80) == 0) {
          lastStart = i;
        }
        if (br1.bytes[br1.offset+i] != br2.bytes[br2.offset+i]) {
          newSuffixStart = lastStart;
          if (DEBUG_SURROGATES) {
            System.out.println("    set newSuffixStart=" + newSuffixStart);
          }
          return;
        }
      }
      newSuffixStart = limit;
      if (DEBUG_SURROGATES) {
        System.out.println("    set newSuffixStart=" + newSuffixStart);
      }
    }

    @Override
    public BytesRef next() throws IOException {
      if (DEBUG_SURROGATES) {
        System.out.println("TE.next()");
      }
      if (skipNext) {
        if (DEBUG_SURROGATES) {
          System.out.println("  skipNext=true");
        }
        skipNext = false;
        if (termEnum.term() == null) {
          return null;
        // PreFlex codec interns field names:
        } else if (termEnum.term().field() != internedFieldName) {
          return null;
        } else {
          return current = termEnum.term().bytes();
        }
      }

      // TODO: can we use STE's prevBuffer here?
      prevTerm.copyBytes(termEnum.term().bytes());

      if (termEnum.next() && termEnum.term().field() == internedFieldName) {
        newSuffixStart = termEnum.newSuffixStart;
        if (DEBUG_SURROGATES) {
          System.out.println("  newSuffixStart=" + newSuffixStart);
        }
        surrogateDance();
        final Term t = termEnum.term();
        if (t == null || t.field() != internedFieldName) {
          // PreFlex codec interns field names; verify:
          assert t == null || !t.field().equals(internedFieldName);
          current = null;
        } else {
          current = t.bytes();
        }
        return current;
      } else {
        // This field is exhausted, but we have to give
        // surrogateDance a chance to seek back:
        if (DEBUG_SURROGATES) {
          System.out.println("  force cont");
        }
        //newSuffixStart = prevTerm.length;
        newSuffixStart = 0;
        surrogateDance();
        
        final Term t = termEnum.term();
        if (t == null || t.field() != internedFieldName) {
          // PreFlex codec interns field names; verify:
          assert t == null || !t.field().equals(internedFieldName);
          return null;
        } else {
          current = t.bytes();
          return current;
        }
      }
    }

    @Override
    public BytesRef term() {
      return current;
    }

    @Override
    public int docFreq() {
      return termEnum.docFreq();
    }

    @Override
    public long totalTermFreq() {
      return -1;
    }

    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
      PreDocsEnum docsEnum;
      if (reuse == null || !(reuse instanceof PreDocsEnum)) {
        docsEnum = new PreDocsEnum();
      } else {
        docsEnum = (PreDocsEnum) reuse;
        if (docsEnum.getFreqStream() != freqStream) {
          docsEnum = new PreDocsEnum();
        }
      }
      return docsEnum.reset(termEnum, liveDocs);
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
      PreDocsAndPositionsEnum docsPosEnum;
      if (fieldInfo.getIndexOptions() != IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
        return null;
      } else if (reuse == null || !(reuse instanceof PreDocsAndPositionsEnum)) {
        docsPosEnum = new PreDocsAndPositionsEnum();
      } else {
        docsPosEnum = (PreDocsAndPositionsEnum) reuse;
        if (docsPosEnum.getFreqStream() != freqStream) {
          docsPosEnum = new PreDocsAndPositionsEnum();
        }
      }
      return docsPosEnum.reset(termEnum, liveDocs);        
    }
  }

  private final class PreDocsEnum extends DocsEnum {
    final private SegmentTermDocs docs;
    private int docID = -1;
    PreDocsEnum() throws IOException {
      docs = new SegmentTermDocs(freqStream, getTermsDict(), fieldInfos);
    }

    IndexInput getFreqStream() {
      return freqStream;
    }

    public PreDocsEnum reset(SegmentTermEnum termEnum, Bits liveDocs) throws IOException {
      docs.setLiveDocs(liveDocs);
      docs.seek(termEnum);
      docs.freq = 1;
      docID = -1;
      return this;
    }

    @Override
    public int nextDoc() throws IOException {
      if (docs.next()) {
        return docID = docs.doc();
      } else {
        return docID = NO_MORE_DOCS;
      }
    }

    @Override
    public int advance(int target) throws IOException {
      if (docs.skipTo(target)) {
        return docID = docs.doc();
      } else {
        return docID = NO_MORE_DOCS;
      }
    }

    @Override
    public int freq() throws IOException {
      return docs.freq();
    }

    @Override
    public int docID() {
      return docID;
    }
    
    @Override
    public long cost() {
      return docs.df;
    }
  }

  private final class PreDocsAndPositionsEnum extends DocsAndPositionsEnum {
    final private SegmentTermPositions pos;
    private int docID = -1;
    PreDocsAndPositionsEnum() throws IOException {
      pos = new SegmentTermPositions(freqStream, proxStream, getTermsDict(), fieldInfos);
    }

    IndexInput getFreqStream() {
      return freqStream;
    }

    public DocsAndPositionsEnum reset(SegmentTermEnum termEnum, Bits liveDocs) throws IOException {
      pos.setLiveDocs(liveDocs);
      pos.seek(termEnum);
      docID = -1;
      return this;
    }

    @Override
    public int nextDoc() throws IOException {
      if (pos.next()) {
        return docID = pos.doc();
      } else {
        return docID = NO_MORE_DOCS;
      }
    }

    @Override
    public int advance(int target) throws IOException {
      if (pos.skipTo(target)) {
        return docID = pos.doc();
      } else {
        return docID = NO_MORE_DOCS;
      }
    }

    @Override
    public int freq() throws IOException {
      return pos.freq();
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextPosition() throws IOException {
      assert docID != NO_MORE_DOCS;
      return pos.nextPosition();
    }

    @Override
    public int startOffset() throws IOException {
      return -1;
    }

    @Override
    public int endOffset() throws IOException {
      return -1;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return pos.getPayload();
    }
    
    @Override
    public long cost() {
      return pos.df;
    }
  }
}
