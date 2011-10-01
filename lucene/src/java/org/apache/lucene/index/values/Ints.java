package org.apache.lucene.index.values;

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

import org.apache.lucene.index.values.IndexDocValuesArray.ByteValues;
import org.apache.lucene.index.values.IndexDocValuesArray.IntValues;
import org.apache.lucene.index.values.IndexDocValuesArray.LongValues;
import org.apache.lucene.index.values.IndexDocValuesArray.ShortValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;

/**
 * Stores ints packed and fixed with fixed-bit precision.
 * 
 * @lucene.experimental
 */
public final class Ints {

  private Ints() {
  }

  public static Writer getWriter(Directory dir, String id, Counter bytesUsed,
      ValueType type, IOContext context) throws IOException {
    return type == ValueType.VAR_INTS ? new PackedIntValues.PackedIntsWriter(dir, id,
        bytesUsed, context) : new IntsWriter(dir, id, bytesUsed, context, type);
  }

  public static IndexDocValues getValues(Directory dir, String id, int numDocs,
      ValueType type, IOContext context) throws IOException {
    return type == ValueType.VAR_INTS ? new PackedIntValues.PackedIntsReader(dir, id,
        numDocs, context) : new IntsReader(dir, id, numDocs, context);
  }

  static class IntsWriter extends FixedStraightBytesImpl.Writer {
    protected static final String CODEC_NAME = "Ints";
    protected static final int VERSION_START = 0;
    protected static final int VERSION_CURRENT = VERSION_START;

    private final ValueType valueType;

    public IntsWriter(Directory dir, String id, Counter bytesUsed,
        IOContext context, ValueType valueType) throws IOException {
      this(dir, id, CODEC_NAME, VERSION_CURRENT, bytesUsed, context, valueType);
    }

    protected IntsWriter(Directory dir, String id, String codecName,
        int version, Counter bytesUsed, IOContext context, ValueType valueType) throws IOException {
      super(dir, id, codecName, version, bytesUsed, context);
      this.valueType = valueType;
      final int expectedSize = getSize(valueType);
      this.bytesRef = new BytesRef(expectedSize);
      bytesRef.length = expectedSize;
    }
    
    private static int getSize(ValueType type) {
      switch (type) {
      case FIXED_INTS_16:
        return 2;
      case FIXED_INTS_32:
        return 4;
      case FIXED_INTS_64:
        return 8;
      case FIXED_INTS_8:
        return 1;
      default:
        throw new IllegalStateException("illegal type " + type);
      }
    }

    @Override
    public void add(int docID, long v) throws IOException {
      switch (valueType) {
      case FIXED_INTS_64:
        bytesRef.copy(v);
        break;
      case FIXED_INTS_32:
        bytesRef.copy((int) (0xFFFFFFFF & v));
        break;
      case FIXED_INTS_16:
        bytesRef.copy((short) (0xFFFFL & v));
        break;
      case FIXED_INTS_8:
        bytesRef.bytes[0] = (byte) (0xFFL & v);
        break;
      default:
        throw new IllegalStateException("illegal type " + valueType);
      }

      add(docID, bytesRef);
    }

    @Override
    public void add(int docID, PerDocFieldValues docValues) throws IOException {
      add(docID, docValues.getInt());
    }
  }

  final static class IntsReader extends FixedStraightBytesImpl.Reader {
    private final ValueType type;
    private final IndexDocValuesArray arrayTemplate;

    IntsReader(Directory dir, String id, int maxDoc, IOContext context)
        throws IOException {
      super(dir, id, IntsWriter.CODEC_NAME, IntsWriter.VERSION_CURRENT, maxDoc,
          context);
      switch (size) {
      case 8:
        type = ValueType.FIXED_INTS_64;
        arrayTemplate = new LongValues();
        break;
      case 4:
        type = ValueType.FIXED_INTS_32;
        arrayTemplate = new IntValues();
        break;
      case 2:
        type = ValueType.FIXED_INTS_16;
        arrayTemplate = new ShortValues();
        break;
      case 1:
        type = ValueType.FIXED_INTS_8;
        arrayTemplate = new ByteValues();
        break;
      default:
        throw new IllegalStateException("illegal size: " + size);
      }
    }

    @Override
    public Source load() throws IOException {
      boolean success = false;
      IndexInput input = null;
      try {
        input = cloneData();
        final Source source = arrayTemplate.newFromInput(input, maxDoc);
        success = true;
        return source;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(input, datIn);
        }
      }
    }

    @Override
    public ValuesEnum getEnum(AttributeSource source) throws IOException {
      final IndexInput input = cloneData();
      boolean success = false;
      try {
        final ValuesEnum valuesEnum = arrayTemplate.getDirectEnum(source,
            input, maxDoc);
        success = true;
        return valuesEnum;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(input);
        }
      }
    }

    @Override
    public ValueType type() {
      return type;
    }
  }
}
