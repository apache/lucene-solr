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
package org.apache.lucene.codecs.simpletext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PairOutputs;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

import static org.apache.lucene.codecs.simpletext.SimpleTextFieldsWriter.DOC;
import static org.apache.lucene.codecs.simpletext.SimpleTextFieldsWriter.END;
import static org.apache.lucene.codecs.simpletext.SimpleTextFieldsWriter.END_OFFSET;
import static org.apache.lucene.codecs.simpletext.SimpleTextFieldsWriter.FIELD;
import static org.apache.lucene.codecs.simpletext.SimpleTextFieldsWriter.FREQ;
import static org.apache.lucene.codecs.simpletext.SimpleTextFieldsWriter.PAYLOAD;
import static org.apache.lucene.codecs.simpletext.SimpleTextFieldsWriter.POS;
import static org.apache.lucene.codecs.simpletext.SimpleTextFieldsWriter.START_OFFSET;
import static org.apache.lucene.codecs.simpletext.SimpleTextFieldsWriter.TERM;
import static org.apache.lucene.codecs.simpletext.SimpleTextSkipWriter.SKIP_LIST;

class SimpleTextFieldsReader extends FieldsProducer {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(SimpleTextFieldsReader.class)
          + RamUsageEstimator.shallowSizeOfInstance(TreeMap.class);

  private final TreeMap<String,Long> fields;
  private final IndexInput in;
  private final FieldInfos fieldInfos;
  private final int maxDoc;

  public SimpleTextFieldsReader(SegmentReadState state) throws IOException {
    this.maxDoc = state.segmentInfo.maxDoc();
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
    ChecksumIndexInput input = new BufferedChecksumIndexInput(in);
    BytesRefBuilder scratch = new BytesRefBuilder();
    TreeMap<String,Long> fields = new TreeMap<>();

    while (true) {
      SimpleTextUtil.readLine(input, scratch);
      if (scratch.get().equals(END)) {
        SimpleTextUtil.checkFooter(input);
        return fields;
      } else if (StringHelper.startsWith(scratch.get(), FIELD)) {
        String fieldName = new String(scratch.bytes(), FIELD.length, scratch.length() - FIELD.length, StandardCharsets.UTF_8);
        fields.put(fieldName, input.getFilePointer());
      }
    }
  }

  private class SimpleTextTermsEnum extends BaseTermsEnum {
    private final IndexOptions indexOptions;
    private int docFreq;
    private long totalTermFreq;
    private long docsStart;
    private long skipPointer;
    private boolean ended;
    private final BytesRefFSTEnum<
            PairOutputs.Pair<PairOutputs.Pair<Long, Long>, PairOutputs.Pair<Long, Long>>>
        fstEnum;

    public SimpleTextTermsEnum(
        FST<PairOutputs.Pair<PairOutputs.Pair<Long, Long>, PairOutputs.Pair<Long, Long>>> fst,
        IndexOptions indexOptions) {
      this.indexOptions = indexOptions;
      fstEnum = new BytesRefFSTEnum<>(fst);
    }

    @Override
    public boolean seekExact(BytesRef text) throws IOException {

      final BytesRefFSTEnum.InputOutput<
              PairOutputs.Pair<PairOutputs.Pair<Long, Long>, PairOutputs.Pair<Long, Long>>>
          result = fstEnum.seekExact(text);
      if (result != null) {
        PairOutputs.Pair<PairOutputs.Pair<Long, Long>, PairOutputs.Pair<Long, Long>> pair =
            result.output;
        PairOutputs.Pair<Long, Long> pair1 = pair.output1;
        PairOutputs.Pair<Long, Long> pair2 = pair.output2;
        docsStart = pair1.output1;
        skipPointer = pair1.output2;
        docFreq = pair2.output1.intValue();
        totalTermFreq = pair2.output2;
        return true;
      } else {
        return false;
      }
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {

      // System.out.println("seek to text=" + text.utf8ToString());
      final BytesRefFSTEnum.InputOutput<
              PairOutputs.Pair<PairOutputs.Pair<Long, Long>, PairOutputs.Pair<Long, Long>>>
          result = fstEnum.seekCeil(text);
      if (result == null) {
        //System.out.println("  end");
        return SeekStatus.END;
      } else {
        // System.out.println("  got text=" + term.utf8ToString());
        PairOutputs.Pair<PairOutputs.Pair<Long, Long>, PairOutputs.Pair<Long, Long>> pair =
            result.output;
        PairOutputs.Pair<Long, Long> pair1 = pair.output1;
        PairOutputs.Pair<Long, Long> pair2 = pair.output2;
        docsStart = pair1.output1;
        skipPointer = pair1.output2;
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
      final BytesRefFSTEnum.InputOutput<
              PairOutputs.Pair<PairOutputs.Pair<Long, Long>, PairOutputs.Pair<Long, Long>>>
          result = fstEnum.next();
      if (result != null) {
        PairOutputs.Pair<PairOutputs.Pair<Long, Long>, PairOutputs.Pair<Long, Long>> pair =
            result.output;
        PairOutputs.Pair<Long, Long> pair1 = pair.output1;
        PairOutputs.Pair<Long, Long> pair2 = pair.output2;
        docsStart = pair1.output1;
        skipPointer = pair1.output2;
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
      return indexOptions == IndexOptions.DOCS ? docFreq : totalTermFreq;
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {

      boolean hasPositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      if (hasPositions && PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS)) {

        SimpleTextPostingsEnum docsAndPositionsEnum;
        if (reuse != null && reuse instanceof SimpleTextPostingsEnum && ((SimpleTextPostingsEnum) reuse).canReuse(SimpleTextFieldsReader.this.in)) {
          docsAndPositionsEnum = (SimpleTextPostingsEnum) reuse;
        } else {
          docsAndPositionsEnum = new SimpleTextPostingsEnum();
        }
        return docsAndPositionsEnum.reset(docsStart, indexOptions, docFreq, skipPointer);
      }

      SimpleTextDocsEnum docsEnum;
      if (reuse != null && reuse instanceof SimpleTextDocsEnum && ((SimpleTextDocsEnum) reuse).canReuse(SimpleTextFieldsReader.this.in)) {
        docsEnum = (SimpleTextDocsEnum) reuse;
      } else {
        docsEnum = new SimpleTextDocsEnum();
      }
      return docsEnum.reset(docsStart, indexOptions == IndexOptions.DOCS, docFreq, skipPointer);
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      if (docFreq <= SimpleTextSkipWriter.BLOCK_SIZE) {
        // no skip data
        return new SlowImpactsEnum(postings(null, flags));
      }
      return (ImpactsEnum) postings(null, flags);
    }
  }

  private class SimpleTextDocsEnum extends ImpactsEnum {
    private final IndexInput inStart;
    private final IndexInput in;
    private boolean omitTF;
    private int docID = -1;
    private int tf;
    private final BytesRefBuilder scratch = new BytesRefBuilder();
    private final CharsRefBuilder scratchUTF16 = new CharsRefBuilder();
    private int cost;

    // for skip list data
    private SimpleTextSkipReader skipReader;
    private int nextSkipDoc = 0;
    private long seekTo = -1;

    public SimpleTextDocsEnum() {
      this.inStart = SimpleTextFieldsReader.this.in;
      this.in = this.inStart.clone();
      this.skipReader = new SimpleTextSkipReader(this.inStart.clone());
    }

    public boolean canReuse(IndexInput in) {
      return in == inStart;
    }

    public SimpleTextDocsEnum reset(long fp, boolean omitTF, int docFreq, long skipPointer)
        throws IOException {
      in.seek(fp);
      this.omitTF = omitTF;
      docID = -1;
      tf = 1;
      cost = docFreq;
      skipReader.reset(skipPointer, docFreq);
      nextSkipDoc = 0;
      seekTo = -1;
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
    public int nextPosition() throws IOException {
      return -1;
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
      return null;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(docID + 1);
    }

    private int readDoc() throws IOException {
      if (docID == NO_MORE_DOCS) {
        return docID;
      }
      boolean first = true;
      int termFreq = 0;
      while(true) {
        final long lineStart = in.getFilePointer();
        SimpleTextUtil.readLine(in, scratch);
        if (StringHelper.startsWith(scratch.get(), DOC)) {
          if (!first) {
            in.seek(lineStart);
            if (!omitTF) {
              tf = termFreq;
            }
            return docID;
          }
          scratchUTF16.copyUTF8Bytes(scratch.bytes(), DOC.length, scratch.length()-DOC.length);
          docID = ArrayUtil.parseInt(scratchUTF16.chars(), 0, scratchUTF16.length());
          termFreq = 0;
          first = false;
        } else if (StringHelper.startsWith(scratch.get(), FREQ)) {
          scratchUTF16.copyUTF8Bytes(scratch.bytes(), FREQ.length, scratch.length()-FREQ.length);
          termFreq = ArrayUtil.parseInt(scratchUTF16.chars(), 0, scratchUTF16.length());
        } else if (StringHelper.startsWith(scratch.get(), POS)) {
          // skip termFreq++;
        } else if (StringHelper.startsWith(scratch.get(), START_OFFSET)) {
          // skip
        } else if (StringHelper.startsWith(scratch.get(), END_OFFSET)) {
          // skip
        } else if (StringHelper.startsWith(scratch.get(), PAYLOAD)) {
          // skip
        } else {
          assert StringHelper.startsWith(scratch.get(), SimpleTextSkipWriter.SKIP_LIST)
                  || StringHelper.startsWith(scratch.get(), TERM)
                  || StringHelper.startsWith(scratch.get(), FIELD)
                  || StringHelper.startsWith(scratch.get(), END)
              : "scratch=" + scratch.get().utf8ToString();
          if (!first) {
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

    private int advanceTarget(int target) throws IOException {
      if (seekTo > 0) {
        in.seek(seekTo);
        seekTo = -1;
      }
      assert docID() < target;
      int doc;
      do {
        doc = readDoc();
      } while (doc < target);
      return doc;
    }

    @Override
    public int advance(int target) throws IOException {
      advanceShallow(target);
      return advanceTarget(target);
    }

    @Override
    public long cost() {
      return cost;
    }

    @Override
    public void advanceShallow(int target) throws IOException {
      if (target > nextSkipDoc) {
        skipReader.skipTo(target);
        if (skipReader.getNextSkipDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          seekTo = skipReader.getNextSkipDocFP();
        }
        nextSkipDoc = skipReader.getNextSkipDoc();
      }
      assert nextSkipDoc >= target;
    }

    @Override
    public Impacts getImpacts() throws IOException {
      advanceShallow(docID);
      return skipReader.getImpacts();
    }
  }

  private class SimpleTextPostingsEnum extends ImpactsEnum {
    private final IndexInput inStart;
    private final IndexInput in;
    private int docID = -1;
    private int tf;
    private final BytesRefBuilder scratch = new BytesRefBuilder();
    private final BytesRefBuilder scratch2 = new BytesRefBuilder();
    private final CharsRefBuilder scratchUTF16 = new CharsRefBuilder();
    private final CharsRefBuilder scratchUTF16_2 = new CharsRefBuilder();
    private int pos;
    private BytesRef payload;
    private long nextDocStart;
    private boolean readOffsets;
    private boolean readPositions;
    private int startOffset;
    private int endOffset;
    private int cost;

    // for skip list data
    private SimpleTextSkipReader skipReader;
    private int nextSkipDoc = 0;
    private long seekTo = -1;

    public SimpleTextPostingsEnum() {
      this.inStart = SimpleTextFieldsReader.this.in;
      this.in = inStart.clone();
      this.skipReader = new SimpleTextSkipReader(this.inStart.clone());
    }

    public boolean canReuse(IndexInput in) {
      return in == inStart;
    }

    public SimpleTextPostingsEnum reset(
        long fp, IndexOptions indexOptions, int docFreq, long skipPointer) throws IOException {
      nextDocStart = fp;
      docID = -1;
      readPositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      readOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
      if (!readOffsets) {
        startOffset = -1;
        endOffset = -1;
      }
      cost = docFreq;
      skipReader.reset(skipPointer, docFreq);
      nextSkipDoc = 0;
      seekTo = -1;
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
      return advance(docID + 1);
    }

    private int readDoc() throws IOException {
      boolean first = true;
      in.seek(nextDocStart);
      long posStart = 0;
      while(true) {
        final long lineStart = in.getFilePointer();
        SimpleTextUtil.readLine(in, scratch);
        //System.out.println("NEXT DOC: " + scratch.utf8ToString());
        if (StringHelper.startsWith(scratch.get(), DOC)) {
          if (!first) {
            nextDocStart = lineStart;
            in.seek(posStart);
            return docID;
          }
          scratchUTF16.copyUTF8Bytes(scratch.bytes(), DOC.length, scratch.length()-DOC.length);
          docID = ArrayUtil.parseInt(scratchUTF16.chars(), 0, scratchUTF16.length());
          tf = 0;
          first = false;
        } else if (StringHelper.startsWith(scratch.get(), FREQ)) {
          scratchUTF16.copyUTF8Bytes(scratch.bytes(), FREQ.length, scratch.length()-FREQ.length);
          tf = ArrayUtil.parseInt(scratchUTF16.chars(), 0, scratchUTF16.length());
          posStart = in.getFilePointer();
        } else if (StringHelper.startsWith(scratch.get(), POS)) {
          // skip
        } else if (StringHelper.startsWith(scratch.get(), START_OFFSET)) {
          // skip
        } else if (StringHelper.startsWith(scratch.get(), END_OFFSET)) {
          // skip
        } else if (StringHelper.startsWith(scratch.get(), PAYLOAD)) {
          // skip
        } else {
          assert StringHelper.startsWith(scratch.get(), SimpleTextSkipWriter.SKIP_LIST)
              || StringHelper.startsWith(scratch.get(), TERM)
              || StringHelper.startsWith(scratch.get(), FIELD)
              || StringHelper.startsWith(scratch.get(), END);
          if (!first) {
            nextDocStart = lineStart;
            in.seek(posStart);
            return docID;
          }
          return docID = NO_MORE_DOCS;
        }
      }
    }

    private int advanceTarget(int target) throws IOException {
      if (seekTo > 0) {
        nextDocStart = seekTo;
        seekTo = -1;
      }
      assert docID() < target;
      int doc;
      do {
        doc = readDoc();
      } while (doc < target);
      return doc;
    }

    @Override
    public int advance(int target) throws IOException {
      advanceShallow(target);
      return advanceTarget(target);
    }

    @Override
    public int nextPosition() throws IOException {
      if (readPositions) {
        SimpleTextUtil.readLine(in, scratch);
        assert StringHelper.startsWith(scratch.get(), POS): "got line=" + scratch.get().utf8ToString();
        scratchUTF16_2.copyUTF8Bytes(scratch.bytes(), POS.length, scratch.length()-POS.length);
        pos = ArrayUtil.parseInt(scratchUTF16_2.chars(), 0, scratchUTF16_2.length());
      } else {
        pos = -1;
      }

      if (readOffsets) {
        SimpleTextUtil.readLine(in, scratch);
        assert StringHelper.startsWith(scratch.get(), START_OFFSET): "got line=" + scratch.get().utf8ToString();
        scratchUTF16_2.copyUTF8Bytes(scratch.bytes(), START_OFFSET.length, scratch.length()-START_OFFSET.length);
        startOffset = ArrayUtil.parseInt(scratchUTF16_2.chars(), 0, scratchUTF16_2.length());
        SimpleTextUtil.readLine(in, scratch);
        assert StringHelper.startsWith(scratch.get(), END_OFFSET): "got line=" + scratch.get().utf8ToString();
        scratchUTF16_2.grow(scratch.length()-END_OFFSET.length);
        scratchUTF16_2.copyUTF8Bytes(scratch.bytes(), END_OFFSET.length, scratch.length()-END_OFFSET.length);
        endOffset = ArrayUtil.parseInt(scratchUTF16_2.chars(), 0, scratchUTF16_2.length());
      }

      final long fp = in.getFilePointer();
      SimpleTextUtil.readLine(in, scratch);
      if (StringHelper.startsWith(scratch.get(), PAYLOAD)) {
        final int len = scratch.length() - PAYLOAD.length;
        scratch2.grow(len);
        System.arraycopy(scratch.bytes(), PAYLOAD.length, scratch2.bytes(), 0, len);
        scratch2.setLength(len);
        payload = scratch2.get();
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

    @Override
    public void advanceShallow(int target) throws IOException {
      if (target > nextSkipDoc) {
        skipReader.skipTo(target);
        if (skipReader.getNextSkipDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          seekTo = skipReader.getNextSkipDocFP();
        }
      }
      nextSkipDoc = skipReader.getNextSkipDoc();
      assert nextSkipDoc >= target;
    }

    @Override
    public Impacts getImpacts() throws IOException {
      advanceShallow(docID);
      return skipReader.getImpacts();
    }
  }

  private static final long TERMS_BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(SimpleTextTerms.class)
          + RamUsageEstimator.shallowSizeOfInstance(BytesRef.class)
          + RamUsageEstimator.shallowSizeOfInstance(CharsRef.class);
  private class SimpleTextTerms extends Terms implements Accountable {
    private final long termsStart;
    private final FieldInfo fieldInfo;
    private final int maxDoc;
    private long sumTotalTermFreq;
    private long sumDocFreq;
    private int docCount;
    private FST<PairOutputs.Pair<PairOutputs.Pair<Long, Long>, PairOutputs.Pair<Long, Long>>> fst;
    private int termCount;
    private final BytesRefBuilder scratch = new BytesRefBuilder();
    private final CharsRefBuilder scratchUTF16 = new CharsRefBuilder();

    public SimpleTextTerms(String field, long termsStart, int maxDoc) throws IOException {
      this.maxDoc = maxDoc;
      this.termsStart = termsStart;
      fieldInfo = fieldInfos.fieldInfo(field);
      loadTerms();
    }

    private void loadTerms() throws IOException {
      PositiveIntOutputs posIntOutputs = PositiveIntOutputs.getSingleton();
      final Builder<
              PairOutputs.Pair<PairOutputs.Pair<Long, Long>, PairOutputs.Pair<Long, Long>>>
          fstCompiler;
      final PairOutputs<Long, Long> outputsOuter = new PairOutputs<>(posIntOutputs, posIntOutputs);
      final PairOutputs<Long, Long> outputsInner = new PairOutputs<>(posIntOutputs, posIntOutputs);
      final PairOutputs<PairOutputs.Pair<Long, Long>, PairOutputs.Pair<Long, Long>> outputs =
          new PairOutputs<>(outputsOuter, outputsInner);
      fstCompiler = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);
      IndexInput in = SimpleTextFieldsReader.this.in.clone();
      in.seek(termsStart);
      final BytesRefBuilder lastTerm = new BytesRefBuilder();
      long lastDocsStart = -1;
      int docFreq = 0;
      long totalTermFreq = 0;
      long skipPointer = 0;
      FixedBitSet visitedDocs = new FixedBitSet(maxDoc);
      final IntsRefBuilder scratchIntsRef = new IntsRefBuilder();
      while(true) {
        SimpleTextUtil.readLine(in, scratch);
        if (scratch.get().equals(END) || StringHelper.startsWith(scratch.get(), FIELD)) {
          if (lastDocsStart != -1) {
            fstCompiler.add(
                Util.toIntsRef(lastTerm.get(), scratchIntsRef),
                outputs.newPair(
                    outputsOuter.newPair(lastDocsStart, skipPointer),
                    outputsInner.newPair((long) docFreq, totalTermFreq)));
            sumTotalTermFreq += totalTermFreq;
          }
          break;
        } else if (StringHelper.startsWith(scratch.get(), DOC)) {
          docFreq++;
          sumDocFreq++;
          totalTermFreq++;
          scratchUTF16.copyUTF8Bytes(scratch.bytes(), DOC.length, scratch.length()-DOC.length);
          int docID = ArrayUtil.parseInt(scratchUTF16.chars(), 0, scratchUTF16.length());
          visitedDocs.set(docID);
        } else if (StringHelper.startsWith(scratch.get(), FREQ)) {
          scratchUTF16.copyUTF8Bytes(scratch.bytes(), FREQ.length, scratch.length()-FREQ.length);
          totalTermFreq += ArrayUtil.parseInt(scratchUTF16.chars(), 0, scratchUTF16.length()) - 1;
        } else if (StringHelper.startsWith(scratch.get(), SKIP_LIST)) {
          skipPointer = in.getFilePointer();
        } else if (StringHelper.startsWith(scratch.get(), TERM)) {
          if (lastDocsStart != -1) {
            fstCompiler.add(
                Util.toIntsRef(lastTerm.get(), scratchIntsRef),
                outputs.newPair(
                    outputsOuter.newPair(lastDocsStart, skipPointer),
                    outputsInner.newPair((long) docFreq, totalTermFreq)));
          }
          lastDocsStart = in.getFilePointer();
          final int len = scratch.length() - TERM.length;
          lastTerm.grow(len);
          System.arraycopy(scratch.bytes(), TERM.length, lastTerm.bytes(), 0, len);
          lastTerm.setLength(len);
          docFreq = 0;
          sumTotalTermFreq += totalTermFreq;
          totalTermFreq = 0;
          termCount++;
          skipPointer = 0;
        }
      }
      docCount = visitedDocs.cardinality();
      fst = fstCompiler.finish();
      /*
      PrintStream ps = new PrintStream("out.dot");
      fst.toDot(ps);
      ps.close();
      System.out.println("SAVED out.dot");
      */
      //System.out.println("FST " + fst.sizeInBytes());
    }

    @Override
    public long ramBytesUsed() {
      return TERMS_BASE_RAM_BYTES_USED + (fst!=null ? fst.ramBytesUsed() : 0)
          + RamUsageEstimator.sizeOf(scratch.bytes()) + RamUsageEstimator.sizeOf(scratchUTF16.chars());
    }

    @Override
    public Collection<Accountable> getChildResources() {
      if (fst == null) {
        return Collections.emptyList();
      } else {
        return Collections.singletonList(Accountables.namedAccountable("term cache", fst));
      }
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(terms=" + termCount + ",postings=" + sumDocFreq + ",positions=" + sumTotalTermFreq + ",docs=" + docCount + ")";
    }

    @Override
    public TermsEnum iterator() throws IOException {
      if (fst != null) {
        return new SimpleTextTermsEnum(fst, fieldInfo.getIndexOptions());
      } else {
        return TermsEnum.EMPTY;
      }
    }

    @Override
    public long size() {
      return (long) termCount;
    }

    @Override
    public long getSumTotalTermFreq() {
      return sumTotalTermFreq;
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
    public boolean hasFreqs() {
      return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
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

  private final Map<String,SimpleTextTerms> termsCache = new HashMap<>();

  @Override
  synchronized public Terms terms(String field) throws IOException {
    SimpleTextTerms terms = termsCache.get(field);
    if (terms == null) {
      Long fp = fields.get(field);
      if (fp == null) {
        return null;
      } else {
        terms = new SimpleTextTerms(field, fp, maxDoc);
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

  @Override
  public synchronized long ramBytesUsed() {
    long sizeInBytes = BASE_RAM_BYTES_USED + fields.size() * 2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    for(SimpleTextTerms simpleTextTerms : termsCache.values()) {
      sizeInBytes += (simpleTextTerms!=null) ? simpleTextTerms.ramBytesUsed() : 0;
    }
    return sizeInBytes;
  }

  @Override
  public synchronized Collection<Accountable> getChildResources() {
    return Accountables.namedAccountables("field", termsCache);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(fields=" + fields.size() + ")";
  }

  @Override
  public void checkIntegrity() throws IOException {}
}
