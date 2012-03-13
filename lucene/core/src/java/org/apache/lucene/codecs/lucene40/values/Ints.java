package org.apache.lucene.codecs.lucene40.values;

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

import org.apache.lucene.codecs.DocValuesArraySource;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;

/**
 * Stores ints packed and fixed with fixed-bit precision.
 * 
 * @lucene.experimental
 */
public final class Ints {
  protected static final String CODEC_NAME = "Ints";
  protected static final int VERSION_START = 0;
  protected static final int VERSION_CURRENT = VERSION_START;

  private Ints() {
  }
  
  public static DocValuesConsumer getWriter(Directory dir, String id, Counter bytesUsed,
      Type type, IOContext context) throws IOException {
    return type == Type.VAR_INTS ? new PackedIntValues.PackedIntsWriter(dir, id,
        bytesUsed, context) : new IntsWriter(dir, id, bytesUsed, context, type);
  }

  public static DocValues getValues(Directory dir, String id, int numDocs,
      Type type, IOContext context) throws IOException {
    return type == Type.VAR_INTS ? new PackedIntValues.PackedIntsReader(dir, id,
        numDocs, context) : new IntsReader(dir, id, numDocs, context, type);
  }
  
  private static Type sizeToType(int size) {
    switch (size) {
    case 1:
      return Type.FIXED_INTS_8;
    case 2:
      return Type.FIXED_INTS_16;
    case 4:
      return Type.FIXED_INTS_32;
    case 8:
      return Type.FIXED_INTS_64;
    default:
      throw new IllegalStateException("illegal size " + size);
    }
  }
  
  private static int typeToSize(Type type) {
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


  static class IntsWriter extends FixedStraightBytesImpl.Writer {
    private final DocValuesArraySource template;

    public IntsWriter(Directory dir, String id, Counter bytesUsed,
        IOContext context, Type valueType) throws IOException {
      this(dir, id, CODEC_NAME, VERSION_CURRENT, bytesUsed, context, valueType);
    }

    protected IntsWriter(Directory dir, String id, String codecName,
        int version, Counter bytesUsed, IOContext context, Type valueType) throws IOException {
      super(dir, id, codecName, version, bytesUsed, context);
      size = typeToSize(valueType);
      this.bytesRef = new BytesRef(size);
      bytesRef.length = size;
      template = DocValuesArraySource.forType(valueType);
    }
    
    @Override
    protected void setMergeBytes(Source source, int sourceDoc) {
      final long value = source.getInt(sourceDoc);
      template.toBytes(value, bytesRef);
    }
    
    @Override
    public void add(int docID, IndexableField value) throws IOException {
      template.toBytes(value.numericValue().longValue(), bytesRef);
      bytesSpareField.setBytesValue(bytesRef);
      super.add(docID, bytesSpareField);
    }

    @Override
    protected boolean tryBulkMerge(DocValues docValues) {
      // only bulk merge if value type is the same otherwise size differs
      return super.tryBulkMerge(docValues) && docValues.getType() == template.getType();
    }
  }
  
  final static class IntsReader extends FixedStraightBytesImpl.FixedStraightReader {
    private final DocValuesArraySource arrayTemplate;

    IntsReader(Directory dir, String id, int maxDoc, IOContext context, Type type)
        throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, maxDoc,
          context, type);
      arrayTemplate = DocValuesArraySource.forType(type);
      assert arrayTemplate != null;
      assert type == sizeToType(size);
    }

    @Override
    public Source load() throws IOException {
      final IndexInput indexInput = cloneData();
      try {
        return arrayTemplate.newFromInput(indexInput, maxDoc);
      } finally {
        IOUtils.close(indexInput);
      }
    }
  }
}
