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
import java.io.FileOutputStream;   // for toDot
import java.io.OutputStreamWriter; // for toDot
import java.io.Writer;             // for toDot
import java.util.HashMap;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util; // for toDot

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
  private long dirOffset;
  
  private final int version;

  final String segment;
  public VariableGapTermsIndexReader(Directory dir, FieldInfos fieldInfos, String segment, int indexDivisor, String segmentSuffix, IOContext context)
    throws IOException {
    in = dir.openInput(IndexFileNames.segmentFileName(segment, segmentSuffix, VariableGapTermsIndexWriter.TERMS_INDEX_EXTENSION), new IOContext(context, true));
    this.segment = segment;
    boolean success = false;
    assert indexDivisor == -1 || indexDivisor > 0;

    try {
      
      version = readHeader(in);
      this.indexDivisor = indexDivisor;

      seekDir(in, dirOffset);

      // Read directory
      final int numFields = in.readVInt();
      if (numFields < 0) {
        throw new CorruptIndexException("invalid numFields: " + numFields + " (resource=" + in + ")");
      }

      for(int i=0;i<numFields;i++) {
        final int field = in.readVInt();
        final long indexStart = in.readVLong();
        final FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
        FieldIndexData previous = fields.put(fieldInfo, new FieldIndexData(fieldInfo, indexStart));
        if (previous != null) {
          throw new CorruptIndexException("duplicate field: " + fieldInfo.name + " (resource=" + in + ")");
        }
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
  
  private int readHeader(IndexInput input) throws IOException {
    int version = CodecUtil.checkHeader(input, VariableGapTermsIndexWriter.CODEC_NAME,
      VariableGapTermsIndexWriter.VERSION_START, VariableGapTermsIndexWriter.VERSION_CURRENT);
    if (version < VariableGapTermsIndexWriter.VERSION_APPEND_ONLY) {
      dirOffset = input.readLong();
    }
    return version;
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

    private final long indexStart;
    // Set only if terms index is loaded:
    private volatile FST<Long> fst;

    public FieldIndexData(FieldInfo fieldInfo, long indexStart) throws IOException {
      this.indexStart = indexStart;

      if (indexDivisor > 0) {
        loadTermsIndex();
      }
    }

    private void loadTermsIndex() throws IOException {
      if (fst == null) {
        IndexInput clone = in.clone();
        clone.seek(indexStart);
        fst = new FST<Long>(clone, fstOutputs);
        clone.close();

        /*
        final String dotFileName = segment + "_" + fieldInfo.name + ".dot";
        Writer w = new OutputStreamWriter(new FileOutputStream(dotFileName));
        Util.toDot(fst, w, false, false);
        System.out.println("FST INDEX: SAVED to " + dotFileName);
        w.close();
        */

        if (indexDivisor > 1) {
          // subsample
          final IntsRef scratchIntsRef = new IntsRef();
          final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);
          final Builder<Long> builder = new Builder<Long>(FST.INPUT_TYPE.BYTE1, outputs);
          final BytesRefFSTEnum<Long> fstEnum = new BytesRefFSTEnum<Long>(fst);
          BytesRefFSTEnum.InputOutput<Long> result;
          int count = indexDivisor;
          while((result = fstEnum.next()) != null) {
            if (count == indexDivisor) {
              builder.add(Util.toIntsRef(result.input, scratchIntsRef), result.output);
              count = 0;
            }
            count++;
          }
          fst = builder.finish();
        }
      }
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

  @Override
  public void close() throws IOException {
    if (in != null && !indexLoaded) {
      in.close();
    }
  }

  private void seekDir(IndexInput input, long dirOffset) throws IOException {
    if (version >= VariableGapTermsIndexWriter.VERSION_APPEND_ONLY) {
      input.seek(input.length() - 8);
      dirOffset = input.readLong();
    }
    input.seek(dirOffset);
  }
}
