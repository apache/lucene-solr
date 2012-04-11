package org.apache.lucene.codecs.simpletext;

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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.UnicodeUtil;

import static org.apache.lucene.codecs.simpletext.SimpleTextTermVectorsWriter.*;

/**
 * Reads plain-text term vectors.
 * <p>
 * <b><font color="red">FOR RECREATIONAL USE ONLY</font></B>
 * @lucene.experimental
 */
public class SimpleTextTermVectorsReader extends TermVectorsReader {
  private ArrayList<Long> offsets; /* docid -> offset in .vec file */
  private IndexInput in;
  private BytesRef scratch = new BytesRef();
  private CharsRef scratchUTF16 = new CharsRef();
  
  public SimpleTextTermVectorsReader(Directory directory, SegmentInfo si, IOContext context) throws IOException {
    boolean success = false;
    try {
      in = directory.openInput(IndexFileNames.segmentFileName(si.name, "", VECTORS_EXTENSION), context);
      success = true;
    } finally {
      if (!success) {
        close();
      }
    }
    readIndex();
  }
  
  // used by clone
  SimpleTextTermVectorsReader(ArrayList<Long> offsets, IndexInput in) {
    this.offsets = offsets;
    this.in = in;
  }
  
  // we don't actually write a .tvx-like index, instead we read the 
  // vectors file in entirety up-front and save the offsets 
  // so we can seek to the data later.
  private void readIndex() throws IOException {
    offsets = new ArrayList<Long>();
    while (!scratch.equals(END)) {
      readLine();
      if (StringHelper.startsWith(scratch, DOC)) {
        offsets.add(in.getFilePointer());
      }
    }
  }
  
  @Override
  public Fields get(int doc) throws IOException {
    // TestTV tests for this in testBadParams... but is this
    // really guaranteed by the API?
    if (doc < 0 || doc >= offsets.size()) {
      throw new IllegalArgumentException("doc id out of range");
    }

    SortedMap<String,SimpleTVTerms> fields = new TreeMap<String,SimpleTVTerms>();
    in.seek(offsets.get(doc));
    readLine();
    assert StringHelper.startsWith(scratch, NUMFIELDS);
    int numFields = parseIntAt(NUMFIELDS.length);
    if (numFields == 0) {
      return null; // no vectors for this doc
    }
    for (int i = 0; i < numFields; i++) {
      readLine();
      assert StringHelper.startsWith(scratch, FIELD);
      // skip fieldNumber:
      parseIntAt(FIELD.length);
      
      readLine();
      assert StringHelper.startsWith(scratch, FIELDNAME);
      String fieldName = readString(FIELDNAME.length, scratch);
      
      readLine();
      assert StringHelper.startsWith(scratch, FIELDPOSITIONS);
      boolean positions = Boolean.parseBoolean(readString(FIELDPOSITIONS.length, scratch));
      
      readLine();
      assert StringHelper.startsWith(scratch, FIELDOFFSETS);
      boolean offsets = Boolean.parseBoolean(readString(FIELDOFFSETS.length, scratch));
      
      readLine();
      assert StringHelper.startsWith(scratch, FIELDTERMCOUNT);
      int termCount = parseIntAt(FIELDTERMCOUNT.length);
      
      SimpleTVTerms terms = new SimpleTVTerms();
      fields.put(fieldName, terms);
      
      for (int j = 0; j < termCount; j++) {
        readLine();
        assert StringHelper.startsWith(scratch, TERMTEXT);
        BytesRef term = new BytesRef();
        int termLength = scratch.length - TERMTEXT.length;
        term.grow(termLength);
        term.length = termLength;
        System.arraycopy(scratch.bytes, scratch.offset+TERMTEXT.length, term.bytes, term.offset, termLength);
        
        SimpleTVPostings postings = new SimpleTVPostings();
        terms.terms.put(term, postings);
        
        readLine();
        assert StringHelper.startsWith(scratch, TERMFREQ);
        postings.freq = parseIntAt(TERMFREQ.length);
        
        if (positions || offsets) {
          if (positions) {
            postings.positions = new int[postings.freq];
          }
        
          if (offsets) {
            postings.startOffsets = new int[postings.freq];
            postings.endOffsets = new int[postings.freq];
          }
          
          for (int k = 0; k < postings.freq; k++) {
            if (positions) {
              readLine();
              assert StringHelper.startsWith(scratch, POSITION);
              postings.positions[k] = parseIntAt(POSITION.length);
            }
            
            if (offsets) {
              readLine();
              assert StringHelper.startsWith(scratch, STARTOFFSET);
              postings.startOffsets[k] = parseIntAt(STARTOFFSET.length);
              
              readLine();
              assert StringHelper.startsWith(scratch, ENDOFFSET);
              postings.endOffsets[k] = parseIntAt(ENDOFFSET.length);
            }
          }
        }
      }
    }
    return new SimpleTVFields(fields);
  }

  @Override
  public TermVectorsReader clone() {
    if (in == null) {
      throw new AlreadyClosedException("this TermVectorsReader is closed");
    }
    return new SimpleTextTermVectorsReader(offsets, (IndexInput) in.clone());
  }
  
  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(in); 
    } finally {
      in = null;
      offsets = null;
    }
  }
  
  public static void files(SegmentInfo info, Set<String> files) throws IOException {
    if (info.getHasVectors()) {
      files.add(IndexFileNames.segmentFileName(info.name, "", VECTORS_EXTENSION));
    }
  }
  
  private void readLine() throws IOException {
    SimpleTextUtil.readLine(in, scratch);
  }
  
  private int parseIntAt(int offset) throws IOException {
    UnicodeUtil.UTF8toUTF16(scratch.bytes, scratch.offset+offset, scratch.length-offset, scratchUTF16);
    return ArrayUtil.parseInt(scratchUTF16.chars, 0, scratchUTF16.length);
  }
  
  private String readString(int offset, BytesRef scratch) {
    UnicodeUtil.UTF8toUTF16(scratch.bytes, scratch.offset+offset, scratch.length-offset, scratchUTF16);
    return scratchUTF16.toString();
  }
  
  private class SimpleTVFields extends Fields {
    private final SortedMap<String,SimpleTVTerms> fields;
    
    SimpleTVFields(SortedMap<String,SimpleTVTerms> fields) throws IOException {
      this.fields = fields;
    }

    @Override
    public FieldsEnum iterator() throws IOException {
      return new FieldsEnum() {
        private Iterator<Map.Entry<String,SimpleTVTerms>> iterator = fields.entrySet().iterator();
        private Map.Entry<String,SimpleTVTerms> current = null;
        
        @Override
        public String next() throws IOException {
          if (!iterator.hasNext()) {
            return null;
          } else {
            current = iterator.next();
            return current.getKey();
          }
        }

        @Override
        public Terms terms() throws IOException {
          return current.getValue();
        }
      };
    }

    @Override
    public Terms terms(String field) throws IOException {
      return fields.get(field);
    }

    @Override
    public int size() throws IOException {
      return fields.size();
    }
  }
  
  private static class SimpleTVTerms extends Terms {
    final SortedMap<BytesRef,SimpleTVPostings> terms;
    
    SimpleTVTerms() {
      terms = new TreeMap<BytesRef,SimpleTVPostings>();
    }
    
    @Override
    public TermsEnum iterator(TermsEnum reuse) throws IOException {
      // TODO: reuse
      return new SimpleTVTermsEnum(terms);
    }

    @Override
    public Comparator<BytesRef> getComparator() throws IOException {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }

    @Override
    public long size() throws IOException {
      return terms.size();
    }

    @Override
    public long getSumTotalTermFreq() throws IOException {
      return -1;
    }

    @Override
    public long getSumDocFreq() throws IOException {
      return terms.size();
    }

    @Override
    public int getDocCount() throws IOException {
      return 1;
    }
  }
  
  private static class SimpleTVPostings {
    private int freq;
    private int positions[];
    private int startOffsets[];
    private int endOffsets[];
  }
  
  private static class SimpleTVTermsEnum extends TermsEnum {
    SortedMap<BytesRef,SimpleTVPostings> terms;
    Iterator<Map.Entry<BytesRef,SimpleTextTermVectorsReader.SimpleTVPostings>> iterator;
    Map.Entry<BytesRef,SimpleTextTermVectorsReader.SimpleTVPostings> current;
    
    SimpleTVTermsEnum(SortedMap<BytesRef,SimpleTVPostings> terms) {
      this.terms = terms;
      this.iterator = terms.entrySet().iterator();
    }
    
    @Override
    public SeekStatus seekCeil(BytesRef text, boolean useCache) throws IOException {
      iterator = terms.tailMap(text).entrySet().iterator();
      if (!iterator.hasNext()) {
        return SeekStatus.END;
      } else {
        return next().equals(text) ? SeekStatus.FOUND : SeekStatus.NOT_FOUND;
      }
    }

    @Override
    public void seekExact(long ord) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef next() throws IOException {
      if (!iterator.hasNext()) {
        return null;
      } else {
        current = iterator.next();
        return current.getKey();
      }
    }

    @Override
    public BytesRef term() throws IOException {
      return current.getKey();
    }

    @Override
    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq() throws IOException {
      return 1;
    }

    @Override
    public long totalTermFreq() throws IOException {
      return current.getValue().freq;
    }

    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, boolean needsFreqs) throws IOException {
      // TODO: reuse
      SimpleTVDocsEnum e = new SimpleTVDocsEnum();
      e.reset(liveDocs, needsFreqs ? current.getValue().freq : -1);
      return e;
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, boolean needsOffsets) throws IOException {
      SimpleTVPostings postings = current.getValue();
      if (postings.positions == null && postings.startOffsets == null) {
        return null;
      }
      if (needsOffsets && (postings.startOffsets == null || postings.endOffsets == null)) {
        return null;
      }
      // TODO: reuse
      SimpleTVDocsAndPositionsEnum e = new SimpleTVDocsAndPositionsEnum();
      e.reset(liveDocs, postings.positions, postings.startOffsets, postings.endOffsets);
      return e;
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }
  }
  
  // note: these two enum classes are exactly like the Default impl...
  private static class SimpleTVDocsEnum extends DocsEnum {
    private boolean didNext;
    private int doc = -1;
    private int freq;
    private Bits liveDocs;

    @Override
    public int freq() {
      assert freq != -1;
      return freq;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      if (!didNext && (liveDocs == null || liveDocs.get(0))) {
        didNext = true;
        return (doc = 0);
      } else {
        return (doc = NO_MORE_DOCS);
      }
    }

    @Override
    public int advance(int target) {
      if (!didNext && target == 0) {
        return nextDoc();
      } else {
        return (doc = NO_MORE_DOCS);
      }
    }

    public void reset(Bits liveDocs, int freq) {
      this.liveDocs = liveDocs;
      this.freq = freq;
      this.doc = -1;
      didNext = false;
    }
  }
  
  private static class SimpleTVDocsAndPositionsEnum extends DocsAndPositionsEnum {
    private boolean didNext;
    private int doc = -1;
    private int nextPos;
    private Bits liveDocs;
    private int[] positions;
    private int[] startOffsets;
    private int[] endOffsets;

    @Override
    public int freq() {
      if (positions != null) {
        return positions.length;
      } else {
        assert startOffsets != null;
        return startOffsets.length;
      }
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      if (!didNext && (liveDocs == null || liveDocs.get(0))) {
        didNext = true;
        return (doc = 0);
      } else {
        return (doc = NO_MORE_DOCS);
      }
    }

    @Override
    public int advance(int target) {
      if (!didNext && target == 0) {
        return nextDoc();
      } else {
        return (doc = NO_MORE_DOCS);
      }
    }

    public void reset(Bits liveDocs, int[] positions, int[] startOffsets, int[] endOffsets) {
      this.liveDocs = liveDocs;
      this.positions = positions;
      this.startOffsets = startOffsets;
      this.endOffsets = endOffsets;
      this.doc = -1;
      didNext = false;
      nextPos = 0;
    }

    @Override
    public BytesRef getPayload() {
      return null;
    }

    @Override
    public boolean hasPayload() {
      return false;
    }

    @Override
    public int nextPosition() {
      assert (positions != null && nextPos < positions.length) ||
        startOffsets != null && nextPos < startOffsets.length;
      if (positions != null) {
        return positions[nextPos++];
      } else {
        nextPos++;
        return -1;
      }
    }

    @Override
    public int startOffset() {
      return startOffsets[nextPos-1];
    }

    @Override
    public int endOffset() {
      return endOffsets[nextPos-1];
    }
  }
}
