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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValue;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;

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
  
  protected static final String CODEC_NAME = "Floats";
  protected static final int VERSION_START = 0;
  protected static final int VERSION_CURRENT = VERSION_START;
  
  public static Writer getWriter(Directory dir, String id, Counter bytesUsed,
      IOContext context, Type type) throws IOException {
    return new FloatsWriter(dir, id, bytesUsed, context, type);
  }

  public static DocValues getValues(Directory dir, String id, int maxDoc, IOContext context, Type type)
      throws IOException {
    return new FloatsReader(dir, id, maxDoc, context, type);
  }
  
  private static int typeToSize(Type type) {
    switch (type) {
    case FLOAT_32:
      return 4;
    case FLOAT_64:
      return 8;
    default:
      throw new IllegalStateException("illegal type " + type);
    }
  }
  
  final static class FloatsWriter extends FixedStraightBytesImpl.Writer {
   
    private final int size; 
    private final DocValuesArray template;
    public FloatsWriter(Directory dir, String id, Counter bytesUsed,
        IOContext context, Type type) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, bytesUsed, context);
      size = typeToSize(type);
      this.bytesRef = new BytesRef(size);
      bytesRef.length = size;
      template = DocValuesArray.TEMPLATES.get(type);
      assert template != null;
    }
    
    public void add(int docID, double v) throws IOException {
      template.toBytes(v, bytesRef);
      add(docID, bytesRef);
    }
    
    @Override
    public void add(int docID, DocValue docValue) throws IOException {
      add(docID, docValue.getFloat());
    }
    
    @Override
    protected boolean tryBulkMerge(DocValues docValues) {
      // only bulk merge if value type is the same otherwise size differs
      return super.tryBulkMerge(docValues) && docValues.type() == template.type();
    }
    
    @Override
    protected void setMergeBytes(int sourceDoc) {
      final double value = currentMergeSource.getFloat(sourceDoc);
      template.toBytes(value, bytesRef);
    }
  }
  
  final static class FloatsReader extends FixedStraightBytesImpl.FixedStraightReader {
    final DocValuesArray arrayTemplate;
    FloatsReader(Directory dir, String id, int maxDoc, IOContext context, Type type)
        throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, maxDoc, context, type);
      arrayTemplate = DocValuesArray.TEMPLATES.get(type);
      assert size == 4 || size == 8;
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