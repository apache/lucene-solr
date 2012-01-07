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

import static org.apache.lucene.codecs.simpletext.SimpleTextNormsConsumer.DOC;
import static org.apache.lucene.codecs.simpletext.SimpleTextNormsConsumer.END;
import static org.apache.lucene.codecs.simpletext.SimpleTextNormsConsumer.FIELD;
import static org.apache.lucene.codecs.simpletext.SimpleTextNormsConsumer.NORM;
import static org.apache.lucene.codecs.simpletext.SimpleTextNormsConsumer.NORMS_EXTENSION;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.PerDocProducer;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;

/**
 * Reads plain-text norms
 * <p>
 * <b><font color="red">FOR RECREATIONAL USE ONLY</font></B>
 * @lucene.experimental
 */
public class SimpleTextNormsProducer extends PerDocProducer {
  
  Map<String,NormsDocValues> norms = new HashMap<String,NormsDocValues>();
  
  public SimpleTextNormsProducer(Directory directory, SegmentInfo si, FieldInfos fields, IOContext context) throws IOException {
    if (fields.hasNorms()) {
      readNorms(directory.openInput(IndexFileNames.segmentFileName(si.name, "", NORMS_EXTENSION), context), si.docCount);
    }
  }
  
  // we read in all the norms up front into a hashmap
  private void readNorms(IndexInput in, int maxDoc) throws IOException {
    BytesRef scratch = new BytesRef();
    boolean success = false;
    try {
      SimpleTextUtil.readLine(in, scratch);
      while (!scratch.equals(END)) {
        assert StringHelper.startsWith(scratch, FIELD);
        final String fieldName = readString(FIELD.length, scratch);
        byte bytes[] = new byte[maxDoc];
        for (int i = 0; i < bytes.length; i++) {
          SimpleTextUtil.readLine(in, scratch);
          assert StringHelper.startsWith(scratch, DOC);
          SimpleTextUtil.readLine(in, scratch);
          assert StringHelper.startsWith(scratch, NORM);
          bytes[i] = scratch.bytes[scratch.offset + NORM.length];
        }
        norms.put(fieldName, new NormsDocValues(new Norm(bytes)));
        SimpleTextUtil.readLine(in, scratch);
        assert StringHelper.startsWith(scratch, FIELD) || scratch.equals(END);
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
  
  @Override
  public void close() throws IOException {
    norms = null;
  }
  
  static void files(Directory dir, SegmentInfo info, Set<String> files) throws IOException {
    // TODO: This is what SI always did... but we can do this cleaner?
    // like first FI that has norms but doesn't have separate norms?
    final String normsFileName = IndexFileNames.segmentFileName(info.name, "", SimpleTextNormsConsumer.NORMS_EXTENSION);
    if (dir.fileExists(normsFileName)) {
      files.add(normsFileName);
    }
  }
  
  private String readString(int offset, BytesRef scratch) {
    return new String(scratch.bytes, scratch.offset+offset, scratch.length-offset, IOUtils.CHARSET_UTF_8);
  }

  @Override
  public DocValues docValues(String field) throws IOException {
    return norms.get(field);
  }
  
  private class NormsDocValues extends DocValues {
    private final Source source;
    public NormsDocValues(Source source) {
      this.source = source;
    }

    @Override
    public Source load() throws IOException {
      return source;
    }

    @Override
    public Source getDirectSource() throws IOException {
      return getSource();
    }

    @Override
    public Type type() {
      return Type.BYTES_FIXED_STRAIGHT;
    }

    @Override
    public int getValueSize() {
      return 1;
    }
  }
  
  static final class Norm extends Source {
    protected Norm(byte[] bytes) {
      super(Type.BYTES_FIXED_STRAIGHT);
      this.bytes = bytes;
    }
    final byte bytes[];
    
    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      ref.bytes = bytes;
      ref.offset = docID;
      ref.length = 1;
      return ref;
    }

    @Override
    public boolean hasArray() {
      return true;
    }

    @Override
    public Object getArray() {
      return bytes;
    }
    
  }
}
