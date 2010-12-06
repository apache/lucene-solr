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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FloatsRef;
import org.apache.lucene.util.LongsRef;
/**
 * 
 * @lucene.experimental
 */
public abstract class ValuesEnum extends DocIdSetIterator {
  private AttributeSource source;
  private Values enumType;
  protected BytesRef bytesRef;
  protected FloatsRef floatsRef;
  protected LongsRef intsRef;

  protected ValuesEnum(Values enumType) {
    this(null, enumType);
  }

  protected ValuesEnum(AttributeSource source, Values enumType) {
    this.source = source;
    this.enumType = enumType;
    switch (enumType) {
    case BYTES_FIXED_DEREF:
    case BYTES_FIXED_SORTED:
    case BYTES_FIXED_STRAIGHT:
    case BYTES_VAR_DEREF:
    case BYTES_VAR_SORTED:
    case BYTES_VAR_STRAIGHT:
      bytesRef = new BytesRef();
      break;
    case PACKED_INTS:
      intsRef = new LongsRef(1);
      break;
    case SIMPLE_FLOAT_4BYTE:
    case SIMPLE_FLOAT_8BYTE:
      floatsRef = new FloatsRef(1);
      break;  
    }
  }

  public Values type() {
    return enumType;
  }

  public BytesRef bytes() {
    return bytesRef;
  }

  public FloatsRef getFloat() {
    return floatsRef;
  }

  public LongsRef getInt() {
    return intsRef;
  }
  
  protected void copyReferences(ValuesEnum valuesEnum) {
    intsRef = valuesEnum.intsRef;
    floatsRef = valuesEnum.floatsRef;
    bytesRef = valuesEnum.bytesRef;
  }

  public AttributeSource attributes() {
    if (source == null)
      source = new AttributeSource();
    return source;
  }

  public <T extends Attribute> T addAttribute(Class<T> attr) {
    return attributes().addAttribute(attr);
  }

  public <T extends Attribute> T getAttribute(Class<T> attr) {
    return attributes().getAttribute(attr);
  }

  public <T extends Attribute> boolean hasAttribute(Class<T> attr) {
    return attributes().hasAttribute(attr);
  }

  public abstract void close() throws IOException;

  public static ValuesEnum emptyEnum(Values type) {
    return new ValuesEnum(type) {
      @Override
      public int nextDoc() throws IOException {
        return NO_MORE_DOCS;
      }
      
      @Override
      public int docID() {
        return NO_MORE_DOCS;
      }
      
      @Override
      public int advance(int target) throws IOException {
        return NO_MORE_DOCS;
      }
      
      @Override
      public void close() throws IOException {
        
      }
    };
  }

}
