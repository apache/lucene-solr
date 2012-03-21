package org.apache.lucene.codecs.simpletext;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
import java.io.IOException;

import org.apache.lucene.codecs.DocValuesArraySource;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.IOUtils;

/**
 * @lucene.experimental
 */
public class SimpleTextDocValuesConsumer extends DocValuesConsumer {

  static final BytesRef ZERO_DOUBLE = new BytesRef(Double.toString(0d));
  static final BytesRef ZERO_INT = new BytesRef(Integer.toString(0));
  static final BytesRef HEADER = new BytesRef("SimpleTextDocValues"); 

  static final BytesRef END = new BytesRef("END");
  static final BytesRef VALUE_SIZE = new BytesRef("valuesize ");
  static final BytesRef DOC = new BytesRef("  doc ");
  static final BytesRef VALUE = new BytesRef("    value ");
  protected BytesRef scratch = new BytesRef();
  protected int maxDocId = -1;
  protected final String segment;
  protected final Directory dir;
  protected final IOContext ctx;
  protected final Type type;
  protected final BytesRefHash hash;
  private int[] ords;
  private int fixedSize = Integer.MIN_VALUE;
  private BytesRef zeroBytes;
  private final String segmentSuffix;
  

  public SimpleTextDocValuesConsumer(String segment, Directory dir,
      IOContext ctx, Type type, String segmentSuffix) {
    this.ctx = ctx;
    this.dir = dir;
    this.segment = segment;
    this.type = type;
    hash = new BytesRefHash();
    ords = new int[0];
    this.segmentSuffix = segmentSuffix;
  }

  @Override
  public void add(int docID, IndexableField value) throws IOException {
    assert docID >= 0;
    final int ord, vSize;
    switch (type) {
    case BYTES_FIXED_DEREF:
    case BYTES_FIXED_SORTED:
    case BYTES_FIXED_STRAIGHT:
      vSize = value.binaryValue().length;
      ord = hash.add(value.binaryValue());
      break;
    case BYTES_VAR_DEREF:
    case BYTES_VAR_SORTED:
    case BYTES_VAR_STRAIGHT:
      vSize = -1;
      ord = hash.add(value.binaryValue());
      break;
    case FIXED_INTS_16:
      vSize = 2;
      scratch.grow(2);
      DocValuesArraySource.copyShort(scratch, value.numericValue().shortValue());
      ord = hash.add(scratch);
      break;
    case FIXED_INTS_32:
      vSize = 4;
      scratch.grow(4);
      DocValuesArraySource.copyInt(scratch, value.numericValue().intValue());
      ord = hash.add(scratch);
      break;
    case FIXED_INTS_8:
      vSize = 1;
      scratch.grow(1); 
      scratch.bytes[scratch.offset] = value.numericValue().byteValue();
      scratch.length = 1;
      ord = hash.add(scratch);
      break;
    case FIXED_INTS_64:
      vSize = 8;
      scratch.grow(8);
      DocValuesArraySource.copyLong(scratch, value.numericValue().longValue());
      ord = hash.add(scratch);
      break;
    case VAR_INTS:
      vSize = -1;
      scratch.grow(8);
      DocValuesArraySource.copyLong(scratch, value.numericValue().longValue());
      ord = hash.add(scratch);
      break;
    case FLOAT_32:
      vSize = 4;
      scratch.grow(4);
      DocValuesArraySource.copyInt(scratch,
          Float.floatToRawIntBits(value.numericValue().floatValue()));
      ord = hash.add(scratch);
      break;
    case FLOAT_64:
      vSize = 8;
      scratch.grow(8);
      DocValuesArraySource.copyLong(scratch,
          Double.doubleToRawLongBits(value.numericValue().doubleValue()));
      ord = hash.add(scratch);
      break;
    default:
      throw new RuntimeException("should not reach this line");
    }
    
    if (fixedSize == Integer.MIN_VALUE) {
      assert maxDocId == -1;
      fixedSize = vSize;
    } else {
      if (fixedSize != vSize) {
        throw new IllegalArgumentException("value size must be " + fixedSize + " but was: " + vSize);
      }
    }
    maxDocId = Math.max(docID, maxDocId);
    ords = grow(ords, docID);
    
    ords[docID] = (ord < 0 ? (-ord)-1 : ord) + 1;
  }
  
  protected BytesRef getHeader() {
    return HEADER;
  }

  private int[] grow(int[] array, int upto) {
    if (array.length <= upto) {
      return ArrayUtil.grow(array, 1 + upto);
    }
    return array;
  }

  private void prepareFlush(int docCount) {
    assert ords != null;
    ords = grow(ords, docCount);
  }

  @Override
  public void finish(int docCount) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segment, "",
        segmentSuffix);
    IndexOutput output = dir.createOutput(fileName, ctx);
    boolean success = false;
    BytesRef spare = new BytesRef();
    try {
      SimpleTextUtil.write(output, getHeader());
      SimpleTextUtil.writeNewline(output);
      SimpleTextUtil.write(output, VALUE_SIZE);
      SimpleTextUtil.write(output, Integer.toString(this.fixedSize), scratch);
      SimpleTextUtil.writeNewline(output);
      prepareFlush(docCount);
      for (int i = 0; i < docCount; i++) {
        SimpleTextUtil.write(output, DOC);
        SimpleTextUtil.write(output, Integer.toString(i), scratch);
        SimpleTextUtil.writeNewline(output);
        SimpleTextUtil.write(output, VALUE);
        writeDoc(output, i, spare);
        SimpleTextUtil.writeNewline(output);
      }
      SimpleTextUtil.write(output, END);
      SimpleTextUtil.writeNewline(output);
      success = true;
    } finally {
      hash.close();
      if (success) {
        IOUtils.close(output);
      } else {
        IOUtils.closeWhileHandlingException(output);
      }
    }
  }

  protected void writeDoc(IndexOutput output, int docId, BytesRef spare) throws IOException {
    int ord = ords[docId] - 1;
    if (ord != -1) {
      assert ord >= 0;
      hash.get(ord, spare);

      switch (type) {
      case BYTES_FIXED_DEREF:
      case BYTES_FIXED_SORTED:
      case BYTES_FIXED_STRAIGHT:
      case BYTES_VAR_DEREF:
      case BYTES_VAR_SORTED:
      case BYTES_VAR_STRAIGHT:
        SimpleTextUtil.write(output, spare);
        break;
      case FIXED_INTS_16:
        SimpleTextUtil.write(output,
            Short.toString(DocValuesArraySource.asShort(spare)), scratch);
        break;
      case FIXED_INTS_32:
        SimpleTextUtil.write(output,
            Integer.toString(DocValuesArraySource.asInt(spare)), scratch);
        break;
      case VAR_INTS:
      case FIXED_INTS_64:
        SimpleTextUtil.write(output,
            Long.toString(DocValuesArraySource.asLong(spare)), scratch);
        break;
      case FIXED_INTS_8:
        assert spare.length == 1 : spare.length;
        SimpleTextUtil.write(output,
            Integer.toString(spare.bytes[spare.offset]), scratch);
        break;
      case FLOAT_32:
        float valueFloat = Float.intBitsToFloat(DocValuesArraySource.asInt(spare));
        SimpleTextUtil.write(output, Float.toString(valueFloat), scratch);
        break;
      case FLOAT_64:
        double valueDouble = Double.longBitsToDouble(DocValuesArraySource
            .asLong(spare));
        SimpleTextUtil.write(output, Double.toString(valueDouble), scratch);
        break;
      default:
        throw new IllegalArgumentException("unsupported type: " + type);
      }
    } else {
      switch (type) {
      case BYTES_FIXED_DEREF:
      case BYTES_FIXED_SORTED:
      case BYTES_FIXED_STRAIGHT:
        if(zeroBytes == null) {
          assert fixedSize > 0;
          zeroBytes = new BytesRef(new byte[fixedSize]);
        }
        SimpleTextUtil.write(output, zeroBytes);
        break;
      case BYTES_VAR_DEREF:
      case BYTES_VAR_SORTED:
      case BYTES_VAR_STRAIGHT:
        scratch.length = 0;
        SimpleTextUtil.write(output, scratch);
        break;
      case FIXED_INTS_16:
      case FIXED_INTS_32:
      case FIXED_INTS_64:
      case FIXED_INTS_8:
      case VAR_INTS:
        SimpleTextUtil.write(output, ZERO_INT);
        break;
      case FLOAT_32:
      case FLOAT_64:
        SimpleTextUtil.write(output, ZERO_DOUBLE);
        break;
      default:
        throw new IllegalArgumentException("unsupported type: " + type);
      }
    }

  }

  @Override
  protected Type getType() {
    return type;
  }
  
  

}