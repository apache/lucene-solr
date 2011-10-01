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

import org.apache.lucene.index.values.IndexDocValues.Source;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;

/**
 * Exposes {@link Writer} and reader ({@link Source}) for 32 bit and 64 bit
 * floating point values.
 * <p>
 * Current implementations store either 4 byte or 8 byte floating points with
 * full precision without any compression.
 * 
 * @lucene.experimental
 */
public class Floats {
  
  public static Writer getWriter(Directory dir, String id, int precisionBytes,
      Counter bytesUsed, IOContext context) throws IOException {
    if (precisionBytes != 4 && precisionBytes != 8) {
      throw new IllegalArgumentException("precisionBytes must be 4 or 8; got "
          + precisionBytes);
    }
    return new FloatsWriter(dir, id, bytesUsed, context, precisionBytes);

  }

  public static IndexDocValues getValues(Directory dir, String id, int maxDoc, IOContext context)
      throws IOException {
    return new FloatsReader(dir, id, maxDoc, context);
  }
  
  final static class FloatsWriter extends FixedStraightBytesImpl.Writer {
    private final int size; 
    public FloatsWriter(Directory dir, String id, Counter bytesUsed,
        IOContext context, int size) throws IOException {
      super(dir, id, bytesUsed, context);
      this.bytesRef = new BytesRef(size);
      this.size = size;
      bytesRef.length = size;
    }
    
    public void add(int docID, double v) throws IOException {
      if (size == 8) {
        bytesRef.copy(Double.doubleToRawLongBits(v));        
      } else {
        bytesRef.copy(Float.floatToRawIntBits((float)v));
      }
      add(docID, bytesRef);
    }
    
    @Override
    public void add(int docID, PerDocFieldValues docValues) throws IOException {
      add(docID, docValues.getFloat());
    }
  }

  
  final static class FloatsReader extends FixedStraightBytesImpl.Reader {
    final IndexDocValuesArray arrayTemplate;
    FloatsReader(Directory dir, String id, int maxDoc, IOContext context)
        throws IOException {
      super(dir, id, maxDoc, context);
      assert size == 4 || size == 8;
      if (size == 4) {
        arrayTemplate = new IndexDocValuesArray.FloatValues();
      } else {
        arrayTemplate = new IndexDocValuesArray.DoubleValues();
      }
    }
    
    @Override
    public Source load() throws IOException {
      final IndexInput indexInput = cloneData();
      try {
        return arrayTemplate.newFromInput(indexInput, maxDoc);
      } finally {
        indexInput.close();
      }
    }
    
    public ValuesEnum getEnum(AttributeSource source) throws IOException {
      IndexInput indexInput = (IndexInput) datIn.clone();
      return arrayTemplate.getDirectEnum(source, indexInput, maxDoc);
    }

    @Override
    public ValueType type() {
      return arrayTemplate.type();
    }
  }

}