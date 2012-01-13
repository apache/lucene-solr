package org.apache.lucene.document;

import org.apache.lucene.index.IndexReader; // javadocs
import org.apache.lucene.search.IndexSearcher; // javadocs
import org.apache.lucene.util.BytesRef;

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

/** A field whose value is stored so that {@link
 *  IndexSearcher#doc} and {@link IndexReader#document} will
 *  return the field and its value. */
public final class StoredField extends Field {

  public final static FieldType TYPE;
  static {
    TYPE = new FieldType();
    TYPE.setStored(true);
    TYPE.freeze();
  }

  public StoredField(String name, byte[] value) {
    super(name, value, TYPE);
  }
  
  public StoredField(String name, byte[] value, int offset, int length) {
    super(name, value, offset, length, TYPE);
  }

  public StoredField(String name, BytesRef value) {
    super(name, value, TYPE);
  }

  public StoredField(String name, String value) {
    super(name, value, TYPE);
  }

  public StoredField(String name, int value) {
    super(name, TYPE);
    fieldsData = value;
  }

  public StoredField(String name, float value) {
    super(name, TYPE);
    fieldsData = value;
  }

  public StoredField(String name, long value) {
    super(name, TYPE);
    fieldsData = value;
  }

  public StoredField(String name, double value) {
    super(name, TYPE);
    fieldsData = value;
  }
}
