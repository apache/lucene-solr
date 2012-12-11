package org.apache.lucene.codecs.blockterms;

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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

/**
 * Selects index terms according to provided pluggable
 * {@link IndexTermSelector}, and stores them in a prefix trie that's
 * loaded entirely in RAM stored as an FST.  This terms
 * index only supports unsigned byte term sort order
 * (unicode codepoint order when the bytes are UTF8).
 *
 * @lucene.experimental */
public class VariableGapTermsIndexWriter extends TermsIndexWriterBase {
  protected final IndexOutput out;

  /** Extension of terms index file */
  static final String TERMS_INDEX_EXTENSION = "tiv";

  final static String CODEC_NAME = "VARIABLE_GAP_TERMS_INDEX";
  final static int VERSION_START = 0;
  final static int VERSION_APPEND_ONLY = 1;
  final static int VERSION_CURRENT = VERSION_APPEND_ONLY;

  private final List<FSTFieldWriter> fields = new ArrayList<FSTFieldWriter>();
  
  @SuppressWarnings("unused") private final FieldInfos fieldInfos; // unread
  private final IndexTermSelector policy;

  /** 
   * Hook for selecting which terms should be placed in the terms index.
   * <p>
   * {@link #newField} is called at the start of each new field, and
   * {@link #isIndexTerm} for each term in that field.
   * 
   * @lucene.experimental 
   */
  public static abstract class IndexTermSelector {
    /** 
     * Called sequentially on every term being written,
     * returning true if this term should be indexed
     */
    public abstract boolean isIndexTerm(BytesRef term, TermStats stats);
    /**
     * Called when a new field is started.
     */
    public abstract void newField(FieldInfo fieldInfo);
  }

  /** Same policy as {@link FixedGapTermsIndexWriter} */
  public static final class EveryNTermSelector extends IndexTermSelector {
    private int count;
    private final int interval;

    public EveryNTermSelector(int interval) {
      this.interval = interval;
      // First term is first indexed term:
      count = interval;
    }

    @Override
    public boolean isIndexTerm(BytesRef term, TermStats stats) {
      if (count >= interval) {
        count = 1;
        return true;
      } else {
        count++;
        return false;
      }
    }

    @Override
    public void newField(FieldInfo fieldInfo) {
      count = interval;
    }
  }

  /** Sets an index term when docFreq >= docFreqThresh, or
   *  every interval terms.  This should reduce seek time
   *  to high docFreq terms.  */
  public static final class EveryNOrDocFreqTermSelector extends IndexTermSelector {
    private int count;
    private final int docFreqThresh;
    private final int interval;

    public EveryNOrDocFreqTermSelector(int docFreqThresh, int interval) {
      this.interval = interval;
      this.docFreqThresh = docFreqThresh;

      // First term is first indexed term:
      count = interval;
    }

    @Override
    public boolean isIndexTerm(BytesRef term, TermStats stats) {
      if (stats.docFreq >= docFreqThresh || count >= interval) {
        count = 1;
        return true;
      } else {
        count++;
        return false;
      }
    }

    @Override
    public void newField(FieldInfo fieldInfo) {
      count = interval;
    }
  }

  // TODO: it'd be nice to let the FST builder prune based
  // on term count of each node (the prune1/prune2 that it
  // accepts), and build the index based on that.  This
  // should result in a more compact terms index, more like
  // a prefix trie than the other selectors, because it
  // only stores enough leading bytes to get down to N
  // terms that may complete that prefix.  It becomes
  // "deeper" when terms are dense, and "shallow" when they
  // are less dense.
  //
  // However, it's not easy to make that work this this
  // API, because that pruning doesn't immediately know on
  // seeing each term whether that term will be a seek point
  // or not.  It requires some non-causality in the API, ie
  // only on seeing some number of future terms will the
  // builder decide which past terms are seek points.
  // Somehow the API'd need to be able to return a "I don't
  // know" value, eg like a Future, which only later on is
  // flipped (frozen) to true or false.
  //
  // We could solve this with a 2-pass approach, where the
  // first pass would build an FSA (no outputs) solely to
  // determine which prefixes are the 'leaves' in the
  // pruning. The 2nd pass would then look at this prefix
  // trie to mark the seek points and build the FST mapping
  // to the true output.
  //
  // But, one downside to this approach is that it'd result
  // in uneven index term selection.  EG with prune1=10, the
  // resulting index terms could be as frequent as every 10
  // terms or as rare as every <maxArcCount> * 10 (eg 2560),
  // in the extremes.

  public VariableGapTermsIndexWriter(SegmentWriteState state, IndexTermSelector policy) throws IOException {
    final String indexFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TERMS_INDEX_EXTENSION);
    out = state.directory.createOutput(indexFileName, state.context);
    boolean success = false;
    try {
      fieldInfos = state.fieldInfos;
      this.policy = policy;
      writeHeader(out);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(out);
      }
    }
  }
  
  private void writeHeader(IndexOutput out) throws IOException {
    CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT);
  }

  @Override
  public FieldWriter addField(FieldInfo field, long termsFilePointer) throws IOException {
    ////System.out.println("VGW: field=" + field.name);
    policy.newField(field);
    FSTFieldWriter writer = new FSTFieldWriter(field, termsFilePointer);
    fields.add(writer);
    return writer;
  }

  /** NOTE: if your codec does not sort in unicode code
   *  point order, you must override this method, to simply
   *  return indexedTerm.length. */
  protected int indexedTermPrefixLength(final BytesRef priorTerm, final BytesRef indexedTerm) {
    // As long as codec sorts terms in unicode codepoint
    // order, we can safely strip off the non-distinguishing
    // suffix to save RAM in the loaded terms index.
    final int idxTermOffset = indexedTerm.offset;
    final int priorTermOffset = priorTerm.offset;
    final int limit = Math.min(priorTerm.length, indexedTerm.length);
    for(int byteIdx=0;byteIdx<limit;byteIdx++) {
      if (priorTerm.bytes[priorTermOffset+byteIdx] != indexedTerm.bytes[idxTermOffset+byteIdx]) {
        return byteIdx+1;
      }
    }
    return Math.min(1+priorTerm.length, indexedTerm.length);
  }

  private class FSTFieldWriter extends FieldWriter {
    private final Builder<Long> fstBuilder;
    private final PositiveIntOutputs fstOutputs;
    private final long startTermsFilePointer;

    final FieldInfo fieldInfo;
    FST<Long> fst;
    final long indexStart;

    private final BytesRef lastTerm = new BytesRef();
    private boolean first = true;

    public FSTFieldWriter(FieldInfo fieldInfo, long termsFilePointer) throws IOException {
      this.fieldInfo = fieldInfo;
      fstOutputs = PositiveIntOutputs.getSingleton(true);
      fstBuilder = new Builder<Long>(FST.INPUT_TYPE.BYTE1, fstOutputs);
      indexStart = out.getFilePointer();
      ////System.out.println("VGW: field=" + fieldInfo.name);

      // Always put empty string in
      fstBuilder.add(new IntsRef(), termsFilePointer);
      startTermsFilePointer = termsFilePointer;
    }

    @Override
    public boolean checkIndexTerm(BytesRef text, TermStats stats) throws IOException {
      //System.out.println("VGW: index term=" + text.utf8ToString());
      // NOTE: we must force the first term per field to be
      // indexed, in case policy doesn't:
      if (policy.isIndexTerm(text, stats) || first) {
        first = false;
        //System.out.println("  YES");
        return true;
      } else {
        lastTerm.copyBytes(text);
        return false;
      }
    }

    private final IntsRef scratchIntsRef = new IntsRef();

    @Override
    public void add(BytesRef text, TermStats stats, long termsFilePointer) throws IOException {
      if (text.length == 0) {
        // We already added empty string in ctor
        assert termsFilePointer == startTermsFilePointer;
        return;
      }
      final int lengthSave = text.length;
      text.length = indexedTermPrefixLength(lastTerm, text);
      try {
        fstBuilder.add(Util.toIntsRef(text, scratchIntsRef), termsFilePointer);
      } finally {
        text.length = lengthSave;
      }
      lastTerm.copyBytes(text);
    }

    @Override
    public void finish(long termsFilePointer) throws IOException {
      fst = fstBuilder.finish();
      if (fst != null) {
        fst.save(out);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
    final long dirStart = out.getFilePointer();
    final int fieldCount = fields.size();

    int nonNullFieldCount = 0;
    for(int i=0;i<fieldCount;i++) {
      FSTFieldWriter field = fields.get(i);
      if (field.fst != null) {
        nonNullFieldCount++;
      }
    }

    out.writeVInt(nonNullFieldCount);
    for(int i=0;i<fieldCount;i++) {
      FSTFieldWriter field = fields.get(i);
      if (field.fst != null) {
        out.writeVInt(field.fieldInfo.number);
        out.writeVLong(field.indexStart);
      }
    }
    writeTrailer(dirStart);
    } finally {
    out.close();
  }
  }

  private void writeTrailer(long dirStart) throws IOException {
    out.writeLong(dirStart);
  }
}
