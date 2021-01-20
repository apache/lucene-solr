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
package org.apache.lucene.codecs.blockterms;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;

/**
 * See {@link VariableGapTermsIndexWriter}
 *
 * @lucene.experimental
 */
public class VariableGapTermsIndexReader extends TermsIndexReaderBase {

  private final PositiveIntOutputs fstOutputs = PositiveIntOutputs.getSingleton();

  final HashMap<String, FieldIndexData> fields = new HashMap<>();

  public VariableGapTermsIndexReader(SegmentReadState state) throws IOException {
    String fileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            VariableGapTermsIndexWriter.TERMS_INDEX_EXTENSION);
    final IndexInput in = state.directory.openInput(fileName, new IOContext(state.context, true));
    boolean success = false;

    try {

      CodecUtil.checkIndexHeader(
          in,
          VariableGapTermsIndexWriter.CODEC_NAME,
          VariableGapTermsIndexWriter.VERSION_START,
          VariableGapTermsIndexWriter.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);

      CodecUtil.checksumEntireFile(in);

      seekDir(in);

      // Read directory
      final int numFields = in.readVInt();
      if (numFields < 0) {
        throw new CorruptIndexException("invalid numFields: " + numFields, in);
      }

      for (int i = 0; i < numFields; i++) {
        final int field = in.readVInt();
        final long indexStart = in.readVLong();
        final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
        FieldIndexData previous =
            fields.put(fieldInfo.name, new FieldIndexData(in, fieldInfo, indexStart));
        if (previous != null) {
          throw new CorruptIndexException("duplicate field: " + fieldInfo.name, in);
        }
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(in);
      } else {
        IOUtils.closeWhileHandlingException(in);
      }
    }
  }

  private static class IndexEnum extends FieldIndexEnum {
    private final BytesRefFSTEnum<Long> fstEnum;
    private BytesRefFSTEnum.InputOutput<Long> current;

    public IndexEnum(FST<Long> fst) {
      fstEnum = new BytesRefFSTEnum<>(fst);
    }

    @Override
    public BytesRef term() {
      if (current == null) {
        return null;
      } else {
        return current.input;
      }
    }

    @Override
    public long seek(BytesRef target) throws IOException {
      // System.out.println("VGR: seek field=" + fieldInfo.name + " target=" + target);
      current = fstEnum.seekFloor(target);
      // System.out.println("  got input=" + current.input + " output=" + current.output);
      return current.output;
    }

    @Override
    public long next() throws IOException {
      // System.out.println("VGR: next field=" + fieldInfo.name);
      current = fstEnum.next();
      if (current == null) {
        // System.out.println("  eof");
        return -1;
      } else {
        return current.output;
      }
    }

    @Override
    public long ord() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long seek(long ord) {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public boolean supportsOrd() {
    return false;
  }

  private final class FieldIndexData implements Accountable {
    private final FST<Long> fst;

    public FieldIndexData(IndexInput in, FieldInfo fieldInfo, long indexStart) throws IOException {
      IndexInput clone = in.clone();
      clone.seek(indexStart);
      fst = new FST<>(clone, clone, fstOutputs);
      clone.close();

      /*
      final String dotFileName = segment + "_" + fieldInfo.name + ".dot";
      Writer w = new OutputStreamWriter(new FileOutputStream(dotFileName));
      Util.toDot(fst, w, false, false);
      System.out.println("FST INDEX: SAVED to " + dotFileName);
      w.close();
      */
    }

    @Override
    public long ramBytesUsed() {
      return fst == null ? 0 : fst.ramBytesUsed();
    }

    @Override
    public Collection<Accountable> getChildResources() {
      if (fst == null) {
        return Collections.emptyList();
      } else {
        return Collections.singletonList(Accountables.namedAccountable("index data", fst));
      }
    }

    @Override
    public String toString() {
      return "VarGapTermIndex";
    }
  }

  @Override
  public FieldIndexEnum getFieldEnum(FieldInfo fieldInfo) {
    final FieldIndexData fieldData = fields.get(fieldInfo.name);
    if (fieldData.fst == null) {
      return null;
    } else {
      return new IndexEnum(fieldData.fst);
    }
  }

  @Override
  public void close() throws IOException {}

  private void seekDir(IndexInput input) throws IOException {
    input.seek(input.length() - CodecUtil.footerLength() - 8);
    long dirOffset = input.readLong();
    input.seek(dirOffset);
  }

  @Override
  public long ramBytesUsed() {
    long sizeInBytes = 0;
    for (FieldIndexData entry : fields.values()) {
      sizeInBytes += entry.ramBytesUsed();
    }
    return sizeInBytes;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Accountables.namedAccountables("field", fields);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(fields=" + fields.size() + ")";
  }
}
