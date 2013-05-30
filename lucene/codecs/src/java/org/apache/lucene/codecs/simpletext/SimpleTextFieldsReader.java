package org.apache.lucene.codecs.simpletext;

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
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PairOutputs;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

class SimpleTextFieldsReader extends FieldsProducer {
  private final TreeMap<String,Long> fields;
  private final IndexInput in;
  private final FieldInfos fieldInfos;

  final static BytesRef END          = SimpleTextFieldsWriter.END;
  final static BytesRef FIELD        = SimpleTextFieldsWriter.FIELD;
  final static BytesRef TERM         = SimpleTextFieldsWriter.TERM;
  final static BytesRef DOC          = SimpleTextFieldsWriter.DOC;
  final static BytesRef FREQ         = SimpleTextFieldsWriter.FREQ;
  final static BytesRef POS          = SimpleTextFieldsWriter.POS;
  final static BytesRef START_OFFSET = SimpleTextFieldsWriter.START_OFFSET;
  final static BytesRef END_OFFSET   = SimpleTextFieldsWriter.END_OFFSET;
  final static BytesRef PAYLOAD      = SimpleTextFieldsWriter.PAYLOAD;

  public SimpleTextFieldsReader(SegmentReadState state) throws IOException {
    fieldInfos = state.fieldInfos;
    in = state.directory.openInput(SimpleTextPostingsFormat.getPostingsFileName(state.segmentInfo.name, state.segmentSuffix), state.context);
    boolean success = false;
    try {
      fields = readFields(in.clone());
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }
  
  private TreeMap<String,Long> readFields(IndexInput in) throws IOException {
    BytesRef scratch = new BytesRef(10);
    TreeMap<String,Long> fields = new TreeMap<String,Long>();
    
    while (true) {
      SimpleTextUtil.readLine(in, scratch);
      if (scratch.equals(END)) {
        return fields;
      } else if (StringHelper.startsWith(scratch, FIELD)) {
        String fieldName = new String(scratch.bytes, scratch.offset + FIELD.length, scratch.length - FIELD.length, "UTF-8");
        fields.put(fieldName, in.getFilePointer());
      }
    }
  }

  private class SimpleTextTermsEnum extends TermsEnum {
    private final IndexOptions indexOptions;
    private int docFreq;
    private long totalTermFreq;
    private long docsStart;
    private boolean ended;
    private final BytesRefFSTEnum<PairOutputs.Pair<Long,PairOutputs.Pair<Long,Long>>> fstEnum;

    public SimpleTextTermsEnum(FST<PairOutputs.Pair<Long,PairOutputs.Pair<Long,Long>>> fst, IndexOptions indexOptions) {
      this.indexOptions = indexOptions;
      fstEnum = new BytesRefFSTEnum<PairOutputs.Pair<Long,PairOutputs.Pair<Long,Long>>>(fst);
    }

    @Override
    public boolean seekExact(BytesRef text, boolean useCache /* ignored */) throws IOException {

      final BytesRefFSTEnum.InputOutput<PairOutputs.Pair<Long,PairOutputs.Pair<Long,Long>>> result = fstEnum.seekExact(text);
      if (result != null) {
        PairOutputs.Pair<Long,PairOutputs.Pair<Long,Long>> pair1 = result.output;
        PairOutputs.Pair<Long,Long> pair2 = pair1.output2;
        docsStart = pair1.output1;
        docFreq = pair2.output1.intValue();
        totalTermFreq = pair2.output2;
        return true;
      } else {
        return false;
      }
    }

    @Override
    public SeekStatus seekCeil(BytesRef text, boolean useCache /* ignored */) throws IOException {

      //System.out.println("seek to text=" + text.utf8ToString());
      final BytesRefFSTEnum.InputOutput<PairOutputs.Pair<Long,PairOutputs.Pair<Long,Long>>> result = fstEnum.seekCeil(text);
      if (result == null) {
        //System.out.println("  end");
        return SeekStatus.END;
      } else {
        //System.out.println("  got text=" + term.utf8ToString());
        PairOutputs.Pair<Long,PairOutputs.Pair<Long,Long>> pair1 = result.output;
        PairOutputs.Pair<Long,Long> pair2 = pair1.output2;
        docsStart = pair1.output1;
        docFreq = pair2.output1.intValue();
        totalTermFreq = pair2.output2;

        if (result.input.equals(text)) {
          //System.out.println("  match docsStart=" + docsStart);
          return SeekStatus.FOUND;
        } else {
          //System.out.println("  not match docsStart=" + docsStart);
          return SeekStatus.NOT_FOUND;
        }
      }
    }

    @Override
    public BytesRef next() throws IOException {
      assert !ended;
      final BytesRefFSTEnum.InputOutput<PairOutputs.Pair<Long,PairOutputs.Pair<Long,Long>>> result = fstEnum.next();
      if (result != null) {
        PairOutputs.Pair<Long,PairOutputs.Pair<Long,Long>> pair1 = result.output;
        PairOutputs.Pair<Long,Long> pair2 = pair1.output2;
        docsStart = pair1.output1;
        docFreq = pair2.output1.intValue();
        totalTermFreq = pair2.output2;
        return result.input;
      } else {
        return null;
      }
    }

    @Override
    public BytesRef term() {
      return fstEnum.current().input;
    }

    @Override
    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seekExact(long ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq() {
      return docFreq;
    }

    @Override
    public long totalTermFreq() {
      return indexOptions == IndexOptions.DOCS_ONLY ? -1 : totalTermFreq;
    }
 
    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
      SimpleTextDocsEnum docsEnum;
      if (reuse != null && reuse instanceof SimpleTextDocsEnum && ((SimpleTextDocsEnum) reuse).canReuse(SimpleTextFieldsReader.this.in)) {
        docsEnum = (SimpleTextDocsEnum) reuse;
      } else {
        docsEnum = new SimpleTextDocsEnum();
      }
      return docsEnum.reset(docsStart, liveDocs, indexOptions == IndexOptions.DOCS_ONLY, docFreq);
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {

      if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
        // Positions were not indexed
        return null;
      }

      SimpleTextDocsAndPositionsEnum docsAndPositionsEnum;
      if (reuse != null && reuse instanceof SimpleTextDocsAndPositionsEnum && ((SimpleTextDocsAndPositionsEnum) reuse).canReuse(SimpleTextFieldsReader.this.in)) {
        docsAndPositionsEnum = (SimpleTextDocsAndPositionsEnum) reuse;
      } else {
        docsAndPositionsEnum = new SimpleTextDocsAndPositionsEnum();
      } 
      return docsAndPositionsEnum.reset(docsStart, liveDocs, indexOptions, docFreq);
    }
    
    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }
  }

  private class SimpleTextDocsEnum extends DocsEnum {
    private final IndexInput inStart;
    private final IndexInput in;
    private boolean omitTF;
    private int docID = -1;
    private int tf;
    private Bits liveDocs;
    private final BytesRef scratch = new BytesRef(10);
    private final CharsRef scratchUTF16 = new CharsRef(10);
    private int cost;
    
    public SimpleTextDocsEnum() {
      this.inStart = SimpleTextFieldsReader.this.in;
      this.in = this.inStart.clone();
    }

    public boolean canReuse(IndexInput in) {
      return in == inStart;
    }

    public SimpleTextDocsEnum reset(long fp, Bits liveDocs, boolean omitTF, int docFreq) throws IOException {
      this.liveDocs = liveDocs;
      in.seek(fp);
      this.omitTF = omitTF;
      docID = -1;
      tf = 1;
      cost = docFreq;
      return this;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int freq() throws IOException {
      return tf;
    }

    @Override
    public int nextDoc() throws IOException {
      if (docID == NO_MORE_DOCS) {
        return docID;
      }
      boolean first = true;
      int termFreq = 0;
      while(true) {
        final long lineStart = in.getFilePointer();
        SimpleTextUtil.readLine(in, scratch);
        if (StringHelper.startsWith(scratch, DOC)) {
          if (!first && (liveDocs == null || liveDocs.get(docID))) {
            in.seek(lineStart);
            if (!omitTF) {
              tf = termFreq;
            }
            return docID;
          }
          UnicodeUtil.UTF8toUTF16(scratch.bytes, scratch.offset+DOC.length, scratch.length-DOC.length, scratchUTF16);
          docID = ArrayUtil.parseInt(scratchUTF16.chars, 0, scratchUTF16.length);
          termFreq = 0;
          first = false;
        } else if (StringHelper.startsWith(scratch, FREQ)) {
          UnicodeUtil.UTF8toUTF16(scratch.bytes, scratch.offset+FREQ.length, scratch.length-FREQ.length, scratchUTF16);
          termFreq = ArrayUtil.parseInt(scratchUTF16.chars, 0, scratchUTF16.length);
        } else if (StringHelper.startsWith(scratch, POS)) {
          // skip termFreq++;
        } else if (StringHelper.startsWith(scratch, START_OFFSET)) {
          // skip
        } else if (StringHelper.startsWith(scratch, END_OFFSET)) {
          // skip
        } else if (StringHelper.startsWith(scratch, PAYLOAD)) {
          // skip
        } else {
          assert StringHelper.startsWith(scratch, TERM) || StringHelper.startsWith(scratch, FIELD) || StringHelper.startsWith(scratch, END): "scratch=" + scratch.utf8ToString();
          if (!first && (liveDocs == null || liveDocs.get(docID))) {
            in.seek(lineStart);
            if (!omitTF) {
              tf = termFreq;
            }
            return docID;
          }
          return docID = NO_MORE_DOCS;
        }
      }
    }

    @Override
    public int advance(int target) throws IOException {
      // Naive -- better to index skip data
      return slowAdvance(target);
    }
    
    @Override
    public long cost() {
      return cost;
    }
  }

  private class SimpleTextDocsAndPositionsEnum extends DocsAndPositionsEnum {
    private final IndexInput inStart;
    private final IndexInput in;
    private int docID = -1;
    private int tf;
    private Bits liveDocs;
    private final BytesRef scratch = new BytesRef(10);
    private final BytesRef scratch2 = new BytesRef(10);
    private final CharsRef scratchUTF16 = new CharsRef(10);
    private final CharsRef scratchUTF16_2 = new CharsRef(10);
    private BytesRef payload;
    private long nextDocStart;
    private boolean readOffsets;
    private boolean readPositions;
    private int startOffset;
    private int endOffset;
    private int cost;

    public SimpleTextDocsAndPositionsEnum() {
      this.inStart = SimpleTextFieldsReader.this.in;
      this.in = inStart.clone();
    }

    public boolean canReuse(IndexInput in) {
      return in == inStart;
    }

    public SimpleTextDocsAndPositionsEnum reset(long fp, Bits liveDocs, IndexOptions indexOptions, int docFreq) {
      this.liveDocs = liveDocs;
      nextDocStart = fp;
      docID = -1;
      readPositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      readOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
      if (!readOffsets) {
        startOffset = -1;
        endOffset = -1;
      }
      cost = docFreq;
      return this;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int freq() throws IOException {
      return tf;
    }

    @Override
    public int nextDoc() throws IOException {
      boolean first = true;
      in.seek(nextDocStart);
      long posStart = 0;
      while(true) {
        final long lineStart = in.getFilePointer();
        SimpleTextUtil.readLine(in, scratch);
        //System.out.println("NEXT DOC: " + scratch.utf8ToString());
        if (StringHelper.startsWith(scratch, DOC)) {
          if (!first && (liveDocs == null || liveDocs.get(docID))) {
            nextDocStart = lineStart;
            in.seek(posStart);
            return docID;
          }
          UnicodeUtil.UTF8toUTF16(scratch.bytes, scratch.offset+DOC.length, scratch.length-DOC.length, scratchUTF16);
          docID = ArrayUtil.parseInt(scratchUTF16.chars, 0, scratchUTF16.length);
          tf = 0;
          first = false;
        } else if (StringHelper.startsWith(scratch, FREQ)) {
          UnicodeUtil.UTF8toUTF16(scratch.bytes, scratch.offset+FREQ.length, scratch.length-FREQ.length, scratchUTF16);
          tf = ArrayUtil.parseInt(scratchUTF16.chars, 0, scratchUTF16.length);
          posStart = in.getFilePointer();
        } else if (StringHelper.startsWith(scratch, POS)) {
          // skip
        } else if (StringHelper.startsWith(scratch, START_OFFSET)) {
          // skip
        } else if (StringHelper.startsWith(scratch, END_OFFSET)) {
          // skip
        } else if (StringHelper.startsWith(scratch, PAYLOAD)) {
          // skip
        } else {
          assert StringHelper.startsWith(scratch, TERM) || StringHelper.startsWith(scratch, FIELD) || StringHelper.startsWith(scratch, END);
          if (!first && (liveDocs == null || liveDocs.get(docID))) {
            nextDocStart = lineStart;
            in.seek(posStart);
            return docID;
          }
          return docID = NO_MORE_DOCS;
        }
      }
    }

    @Override
    public int advance(int target) throws IOException {
      // Naive -- better to index skip data
      return slowAdvance(target);
    }

    @Override
    public int nextPosition() throws IOException {
      final int pos;
      if (readPositions) {
        SimpleTextUtil.readLine(in, scratch);
        assert StringHelper.startsWith(scratch, POS): "got line=" + scratch.utf8ToString();
        UnicodeUtil.UTF8toUTF16(scratch.bytes, scratch.offset+POS.length, scratch.length-POS.length, scratchUTF16_2);
        pos = ArrayUtil.parseInt(scratchUTF16_2.chars, 0, scratchUTF16_2.length);
      } else {
        pos = -1;
      }

      if (readOffsets) {
        SimpleTextUtil.readLine(in, scratch);
        assert StringHelper.startsWith(scratch, START_OFFSET): "got line=" + scratch.utf8ToString();
        UnicodeUtil.UTF8toUTF16(scratch.bytes, scratch.offset+START_OFFSET.length, scratch.length-START_OFFSET.length, scratchUTF16_2);
        startOffset = ArrayUtil.parseInt(scratchUTF16_2.chars, 0, scratchUTF16_2.length);
        SimpleTextUtil.readLine(in, scratch);
        assert StringHelper.startsWith(scratch, END_OFFSET): "got line=" + scratch.utf8ToString();
        UnicodeUtil.UTF8toUTF16(scratch.bytes, scratch.offset+END_OFFSET.length, scratch.length-END_OFFSET.length, scratchUTF16_2);
        endOffset = ArrayUtil.parseInt(scratchUTF16_2.chars, 0, scratchUTF16_2.length);
      }

      final long fp = in.getFilePointer();
      SimpleTextUtil.readLine(in, scratch);
      if (StringHelper.startsWith(scratch, PAYLOAD)) {
        final int len = scratch.length - PAYLOAD.length;
        if (scratch2.bytes.length < len) {
          scratch2.grow(len);
        }
        System.arraycopy(scratch.bytes, PAYLOAD.length, scratch2.bytes, 0, len);
        scratch2.length = len;
        payload = scratch2;
      } else {
        payload = null;
        in.seek(fp);
      }
      return pos;
    }

    @Override
    public int startOffset() throws IOException {
      return startOffset;
    }

    @Override
    public int endOffset() throws IOException {
      return endOffset;
    }

    @Override
    public BytesRef getPayload() {
      return payload;
    }
    
    @Override
    public long cost() {
      return cost;
    }
  }

  static class TermData {
    public long docsStart;
    public int docFreq;

    public TermData(long docsStart, int docFreq) {
      this.docsStart = docsStart;
      this.docFreq = docFreq;
    }
  }

  private class SimpleTextTerms extends Terms {
    private final long termsStart;
    private final FieldInfo fieldInfo;
    private long sumTotalTermFreq;
    private long sumDocFreq;
    private int docCount;
    private FST<PairOutputs.Pair<Long,PairOutputs.Pair<Long,Long>>> fst;
    private int termCount;
    private final BytesRef scratch = new BytesRef(10);
    private final CharsRef scratchUTF16 = new CharsRef(10);

    public SimpleTextTerms(String field, long termsStart) throws IOException {
      this.termsStart = termsStart;
      fieldInfo = fieldInfos.fieldInfo(field);
      loadTerms();
    }

    private void loadTerms() throws IOException {
      PositiveIntOutputs posIntOutputs = PositiveIntOutputs.getSingleton(false);
      final Builder<PairOutputs.Pair<Long,PairOutputs.Pair<Long,Long>>> b;
      final PairOutputs<Long,Long> outputsInner = new PairOutputs<Long,Long>(posIntOutputs, posIntOutputs);
      final PairOutputs<Long,PairOutputs.Pair<Long,Long>> outputs = new PairOutputs<Long,PairOutputs.Pair<Long,Long>>(posIntOutputs,
                                                                                                                      outputsInner);
      b = new Builder<PairOutputs.Pair<Long,PairOutputs.Pair<Long,Long>>>(FST.INPUT_TYPE.BYTE1, outputs);
      IndexInput in = SimpleTextFieldsReader.this.in.clone();
      in.seek(termsStart);
      final BytesRef lastTerm = new BytesRef(10);
      long lastDocsStart = -1;
      int docFreq = 0;
      long totalTermFreq = 0;
      OpenBitSet visitedDocs = new OpenBitSet();
      final IntsRef scratchIntsRef = new IntsRef();
      while(true) {
        SimpleTextUtil.readLine(in, scratch);
        if (scratch.equals(END) || StringHelper.startsWith(scratch, FIELD)) {
          if (lastDocsStart != -1) {
            b.add(Util.toIntsRef(lastTerm, scratchIntsRef),
                  outputs.newPair(lastDocsStart,
                                  outputsInner.newPair((long) docFreq, totalTermFreq)));
            sumTotalTermFreq += totalTermFreq;
          }
          break;
        } else if (StringHelper.startsWith(scratch, DOC)) {
          docFreq++;
          sumDocFreq++;
          UnicodeUtil.UTF8toUTF16(scratch.bytes, scratch.offset+DOC.length, scratch.length-DOC.length, scratchUTF16);
          int docID = ArrayUtil.parseInt(scratchUTF16.chars, 0, scratchUTF16.length);
          visitedDocs.set(docID);
        } else if (StringHelper.startsWith(scratch, FREQ)) {
          UnicodeUtil.UTF8toUTF16(scratch.bytes, scratch.offset+FREQ.length, scratch.length-FREQ.length, scratchUTF16);
          totalTermFreq += ArrayUtil.parseInt(scratchUTF16.chars, 0, scratchUTF16.length);
        } else if (StringHelper.startsWith(scratch, TERM)) {
          if (lastDocsStart != -1) {
            b.add(Util.toIntsRef(lastTerm, scratchIntsRef), outputs.newPair(lastDocsStart,
                                                                            outputsInner.newPair((long) docFreq, totalTermFreq)));
          }
          lastDocsStart = in.getFilePointer();
          final int len = scratch.length - TERM.length;
          if (len > lastTerm.length) {
            lastTerm.grow(len);
          }
          System.arraycopy(scratch.bytes, TERM.length, lastTerm.bytes, 0, len);
          lastTerm.length = len;
          docFreq = 0;
          sumTotalTermFreq += totalTermFreq;
          totalTermFreq = 0;
          termCount++;
        }
      }
      docCount = (int) visitedDocs.cardinality();
      fst = b.finish();
      /*
      PrintStream ps = new PrintStream("out.dot");
      fst.toDot(ps);
      ps.close();
      System.out.println("SAVED out.dot");
      */
      //System.out.println("FST " + fst.sizeInBytes());
    }

    @Override
    public TermsEnum iterator(TermsEnum reuse) throws IOException {
      if (fst != null) {
        return new SimpleTextTermsEnum(fst, fieldInfo.getIndexOptions());
      } else {
        return TermsEnum.EMPTY;
      }
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }

    @Override
    public long size() {
      return (long) termCount;
    }

    @Override
    public long getSumTotalTermFreq() {
      return fieldInfo.getIndexOptions() == IndexOptions.DOCS_ONLY ? -1 : sumTotalTermFreq;
    }

    @Override
    public long getSumDocFreq() throws IOException {
      return sumDocFreq;
    }

    @Override
    public int getDocCount() throws IOException {
      return docCount;
    }

    @Override
    public boolean hasOffsets() {
      return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
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

  @Override
  public Iterator<String> iterator() {
    return Collections.unmodifiableSet(fields.keySet()).iterator();
  }

  private final Map<String,Terms> termsCache = new HashMap<String,Terms>();

  @Override
  synchronized public Terms terms(String field) throws IOException {
    Terms terms = termsCache.get(field);
    if (terms == null) {
      Long fp = fields.get(field);
      if (fp == null) {
        return null;
      } else {
        terms = new SimpleTextTerms(field, fp);
        termsCache.put(field, terms);
      }
    }
    return terms;
  }

  @Override
  public int size() {
    return -1;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
