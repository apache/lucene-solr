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

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;

import static org.apache.lucene.codecs.simpletext.SimpleTextStoredFieldsWriter.*;

/**
 * reads plaintext stored fields
 * <p>
 * <b>FOR RECREATIONAL USE ONLY</b>
 * @lucene.experimental
 */
public class SimpleTextStoredFieldsReader extends StoredFieldsReader {

  private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(SimpleTextStoredFieldsReader.class)
      + RamUsageEstimator.shallowSizeOfInstance(BytesRef.class)
      + RamUsageEstimator.shallowSizeOfInstance(CharsRef.class);

  private long offsets[]; /* docid -> offset in .fld file */
  private IndexInput in;
  private BytesRefBuilder scratch = new BytesRefBuilder();
  private CharsRefBuilder scratchUTF16 = new CharsRefBuilder();
  private final FieldInfos fieldInfos;

  public SimpleTextStoredFieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
    this.fieldInfos = fn;
    boolean success = false;
    try {
      in = directory.openInput(IndexFileNames.segmentFileName(si.name, "", SimpleTextStoredFieldsWriter.FIELDS_EXTENSION), context);
      success = true;
    } finally {
      if (!success) {
        try {
          close();
        } catch (Throwable t) {} // ensure we throw our original exception
      }
    }
    readIndex(si.maxDoc());
  }
  
  // used by clone
  SimpleTextStoredFieldsReader(long offsets[], IndexInput in, FieldInfos fieldInfos) {
    this.offsets = offsets;
    this.in = in;
    this.fieldInfos = fieldInfos;
  }
  
  // we don't actually write a .fdx-like index, instead we read the 
  // stored fields file in entirety up-front and save the offsets 
  // so we can seek to the documents later.
  private void readIndex(int size) throws IOException {
    ChecksumIndexInput input = new BufferedChecksumIndexInput(in);
    offsets = new long[size];
    int upto = 0;
    while (!scratch.get().equals(END)) {
      SimpleTextUtil.readLine(input, scratch);
      if (StringHelper.startsWith(scratch.get(), DOC)) {
        offsets[upto] = input.getFilePointer();
        upto++;
      }
    }
    SimpleTextUtil.checkFooter(input);
    assert upto == offsets.length;
  }
  
  @Override
  public void visitDocument(int n, StoredFieldVisitor visitor) throws IOException {
    in.seek(offsets[n]);
    
    while (true) {
      readLine();
      if (StringHelper.startsWith(scratch.get(), FIELD) == false) {
        break;
      }
      int fieldNumber = parseIntAt(FIELD.length);
      FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber);
      readLine();
      assert StringHelper.startsWith(scratch.get(), NAME);
      readLine();
      assert StringHelper.startsWith(scratch.get(), TYPE);
      
      final BytesRef type;
      if (equalsAt(TYPE_STRING, scratch.get(), TYPE.length)) {
        type = TYPE_STRING;
      } else if (equalsAt(TYPE_BINARY, scratch.get(), TYPE.length)) {
        type = TYPE_BINARY;
      } else if (equalsAt(TYPE_INT, scratch.get(), TYPE.length)) {
        type = TYPE_INT;
      } else if (equalsAt(TYPE_LONG, scratch.get(), TYPE.length)) {
        type = TYPE_LONG;
      } else if (equalsAt(TYPE_FLOAT, scratch.get(), TYPE.length)) {
        type = TYPE_FLOAT;
      } else if (equalsAt(TYPE_DOUBLE, scratch.get(), TYPE.length)) {
        type = TYPE_DOUBLE;
      } else {
        throw new RuntimeException("unknown field type");
      }
      
      switch (visitor.needsField(fieldInfo)) {
        case YES:  
          readField(type, fieldInfo, visitor);
          break;
        case NO:   
          readLine();
          assert StringHelper.startsWith(scratch.get(), VALUE);
          break;
        case STOP: return;
      }
    }
  }
  
  private void readField(BytesRef type, FieldInfo fieldInfo, StoredFieldVisitor visitor) throws IOException {
    readLine();
    assert StringHelper.startsWith(scratch.get(), VALUE);
    if (type == TYPE_STRING) {
      byte[] bytes = new byte[scratch.length() - VALUE.length];
      System.arraycopy(scratch.bytes(), VALUE.length, bytes, 0, bytes.length);
      visitor.stringField(fieldInfo, bytes);
    } else if (type == TYPE_BINARY) {
      byte[] copy = new byte[scratch.length()-VALUE.length];
      System.arraycopy(scratch.bytes(), VALUE.length, copy, 0, copy.length);
      visitor.binaryField(fieldInfo, copy);
    } else if (type == TYPE_INT) {
      scratchUTF16.copyUTF8Bytes(scratch.bytes(), VALUE.length, scratch.length()-VALUE.length);
      visitor.intField(fieldInfo, Integer.parseInt(scratchUTF16.toString()));
    } else if (type == TYPE_LONG) {
      scratchUTF16.copyUTF8Bytes(scratch.bytes(), VALUE.length, scratch.length()-VALUE.length);
      visitor.longField(fieldInfo, Long.parseLong(scratchUTF16.toString()));
    } else if (type == TYPE_FLOAT) {
      scratchUTF16.copyUTF8Bytes(scratch.bytes(), VALUE.length, scratch.length()-VALUE.length);
      visitor.floatField(fieldInfo, Float.parseFloat(scratchUTF16.toString()));
    } else if (type == TYPE_DOUBLE) {
      scratchUTF16.copyUTF8Bytes(scratch.bytes(), VALUE.length, scratch.length()-VALUE.length);
      visitor.doubleField(fieldInfo, Double.parseDouble(scratchUTF16.toString()));
    }
  }

  @Override
  public StoredFieldsReader clone() {
    if (in == null) {
      throw new AlreadyClosedException("this FieldsReader is closed");
    }
    return new SimpleTextStoredFieldsReader(offsets, in.clone(), fieldInfos);
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
  
  private void readLine() throws IOException {
    SimpleTextUtil.readLine(in, scratch);
  }
  
  private int parseIntAt(int offset) {
    scratchUTF16.copyUTF8Bytes(scratch.bytes(), offset, scratch.length()-offset);
    return ArrayUtil.parseInt(scratchUTF16.chars(), 0, scratchUTF16.length());
  }
  
  private boolean equalsAt(BytesRef a, BytesRef b, int bOffset) {
    return a.length == b.length - bOffset && 
        ArrayUtil.equals(a.bytes, a.offset, b.bytes, b.offset + bOffset, b.length - bOffset);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(offsets)
        + RamUsageEstimator.sizeOf(scratch.bytes()) + RamUsageEstimator.sizeOf(scratchUTF16.chars());
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  @Override
  public void checkIntegrity() throws IOException {}
}
