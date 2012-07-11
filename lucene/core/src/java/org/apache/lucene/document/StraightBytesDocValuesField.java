package org.apache.lucene.document;

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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.util.BytesRef;

/**
 * <p>
 * This class provides a {@link Field} that enables storing
 * of a per-document {@link BytesRef} value.  The values are
 * stored directly with no sharing, which is a good fit when
 * the fields don't share (many) values, such as a title
 * field.  If values may be shared it's better to use {@link
 * DerefBytesDocValuesField}.  Here's an example usage:
 * 
 * <pre>
 *   document.add(new StraightBytesDocValuesField(name, new BytesRef("hello")));
 * </pre>
 * 
 * <p>
 * If you also need to store the value, you should add a
 * separate {@link StoredField} instance.
 * 
 * @see DocValues for further information
 * */

public class StraightBytesDocValuesField extends Field {

  // TODO: ideally indexer figures out var vs fixed on its own!?
  public static final FieldType TYPE_FIXED_LEN = new FieldType();
  static {
    TYPE_FIXED_LEN.setDocValueType(DocValues.Type.BYTES_FIXED_STRAIGHT);
    TYPE_FIXED_LEN.freeze();
  }

  public static final FieldType TYPE_VAR_LEN = new FieldType();
  static {
    TYPE_VAR_LEN.setDocValueType(DocValues.Type.BYTES_VAR_STRAIGHT);
    TYPE_VAR_LEN.freeze();
  }

  public StraightBytesDocValuesField(String name, BytesRef bytes) {
    super(name, TYPE_VAR_LEN);
    fieldsData = bytes;
  }

  public StraightBytesDocValuesField(String name, BytesRef bytes, boolean isFixedLength) {
    super(name, isFixedLength ? TYPE_FIXED_LEN : TYPE_VAR_LEN);
    fieldsData = bytes;
  }
}
