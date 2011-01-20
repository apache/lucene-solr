package org.apache.lucene.index.codecs;

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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.automaton.fst.Builder;
import org.apache.lucene.util.automaton.fst.BytesRefFSTEnum;
import org.apache.lucene.util.automaton.fst.FST;
import org.apache.lucene.util.automaton.fst.PositiveIntOutputs;

/** See {@link VariableGapTermsIndexWriter}
 * 
 * @lucene.experimental */
public class VariableGapTermsIndexReader extends TermsIndexReaderBase {

  private final PositiveIntOutputs fstOutputs = PositiveIntOutputs.getSingleton(true);
  private int indexDivisor;

  // Closed if indexLoaded is true:
  private IndexInput in;
  private volatile boolean indexLoaded;

  final HashMap<FieldInfo,FieldIndexData> fields = new HashMap<FieldInfo,FieldIndexData>();
  
  // start of the field info data
  protected long dirOffset;

  public VariableGapTermsIndexReader(Directory dir, FieldInfos fieldInfos, String segment, int indexDivisor, String codecId)
    throws IOException {

    in = dir.openInput(IndexFileNames.segmentFileName(segment, codecId, VariableGapTermsIndexWriter.TERMS_INDEX_EXTENSION));
    
    boolean success = false;

    try {
      
      readHeader(in);
      this.indexDivisor = indexDivisor;

      seekDir(in, dirOffset);

      // Read directory
      final int numFields = in.readVInt();

      for(int i=0;i<numFields;i++) {
        final int field = in.readVInt();
        final long indexStart = in.readVLong();
        final FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
        fields.put(fieldInfo, new FieldIndexData(fieldInfo, indexStart));
      }
      success = true;
    } finally {
      if (indexDivisor > 0) {
        in.close();
        in = null;
        if (success) {
          indexLoaded = true;
        }
      }
    }
  }

  @Override
  public int getDivisor() {
    return indexDivisor;
  }
  
  protected void readHeader(IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, VariableGapTermsIndexWriter.CODEC_NAME,
      VariableGapTermsIndexWriter.VERSION_START, VariableGapTermsIndexWriter.VERSION_START);
    dirOffset = input.readLong();
  }

  private static class IndexEnum extends FieldIndexEnum {
    private final BytesRefFSTEnum<Long> fstEnum;
    private BytesRefFSTEnum.InputOutput<Long> current;

    public IndexEnum(FST<Long> fst) {
      fstEnum = new BytesRefFSTEnum<Long>(fst);
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
      //System.out.println("VGR: seek field=" + fieldInfo.name + " target=" + target);
      current = fstEnum.seekFloor(target);
      //System.out.println("  got input=" + current.input + " output=" + current.output);
      return current.output;
    }

    @Override
    public long next() throws IOException {
      //System.out.println("VGR: next field=" + fieldInfo.name);
      current = fstEnum.next();
      if (current == null) {
        //System.out.println("  eof");
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

  private final class FieldIndexData {

    private final FieldInfo fieldInfo;
    private final long indexStart;

    // Set only if terms index is loaded:
    private volatile FST<Long> fst;

    public FieldIndexData(FieldInfo fieldInfo, long indexStart) throws IOException {

      this.fieldInfo = fieldInfo;
      this.indexStart = indexStart;

      if (indexDivisor > 0) {
        loadTermsIndex();
      }
    }

    public void loadTermsIndex() throws IOException {
      if (fst == null) {
        IndexInput clone = (IndexInput) in.clone();
        clone.seek(indexStart);
        fst = new FST<Long>(clone, fstOutputs);
        clone.close();

        if (indexDivisor > 1) {
          // subsample
          final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);
          final Builder<Long> builder = new Builder<Long>(FST.INPUT_TYPE.BYTE1, 0, 0, true, outputs);
          final BytesRefFSTEnum<Long> fstEnum = new BytesRefFSTEnum<Long>(fst);
          BytesRefFSTEnum.InputOutput<Long> result;
          int count = indexDivisor;
          while((result = fstEnum.next()) != null) {
            if (count == indexDivisor) {
              builder.add(result.input, result.output);
              count = 0;
            }
            count++;
          }
          fst = builder.finish();
        }
      }
    }
  }

  // Externally synced in IndexWriter
  @Override
  public void loadTermsIndex(int indexDivisor) throws IOException {
    if (!indexLoaded) {

      if (indexDivisor < 0) {
        this.indexDivisor = -indexDivisor;
      } else {
        this.indexDivisor = indexDivisor;
      }

      Iterator<FieldIndexData> it = fields.values().iterator();
      while(it.hasNext()) {
        it.next().loadTermsIndex();
      }

      indexLoaded = true;
      in.close();
    }
  }

  @Override
  public FieldIndexEnum getFieldEnum(FieldInfo fieldInfo) {
    final FieldIndexData fieldData = fields.get(fieldInfo);
    if (fieldData.fst == null) {
      return null;
    } else {
      return new IndexEnum(fieldData.fst);
    }
  }

  public static void files(Directory dir, SegmentInfo info, String id, Collection<String> files) {
    files.add(IndexFileNames.segmentFileName(info.name, id, VariableGapTermsIndexWriter.TERMS_INDEX_EXTENSION));
  }

  public static void getIndexExtensions(Collection<String> extensions) {
    extensions.add(VariableGapTermsIndexWriter.TERMS_INDEX_EXTENSION);
  }

  @Override
  public void getExtensions(Collection<String> extensions) {
    getIndexExtensions(extensions);
  }

  @Override
  public void close() throws IOException {
    if (in != null && !indexLoaded) {
      in.close();
    }
  }

  protected void seekDir(IndexInput input, long dirOffset) throws IOException {
    input.seek(dirOffset);
  }
}
