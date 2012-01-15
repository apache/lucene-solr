package org.apache.lucene.document;

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

import java.io.Reader;

import org.apache.lucene.analysis.TokenStream;

/** A field that is indexed and tokenized, without term
 *  vectors.  For example this would be used on a 'body'
 *  field, that contains the bulk of a document's text.
 * 
 *  This field's value is not stored by default; use the
 *  {@link TextField#TYPE_STORED} type (pass it to <code>new
 *  Field</code>) to store the value. */

public final class TextField extends Field {

  /* Indexed, tokenized, not stored. */
  public static final FieldType TYPE_UNSTORED = new FieldType();

  /* Indexed, tokenized, stored. */
  public static final FieldType TYPE_STORED = new FieldType();

  static {
    TYPE_UNSTORED.setIndexed(true);
    TYPE_UNSTORED.setTokenized(true);
    TYPE_UNSTORED.freeze();

    TYPE_STORED.setIndexed(true);
    TYPE_STORED.setStored(true);
    TYPE_STORED.setTokenized(true);
    TYPE_STORED.freeze();
  }

  // TODO: add sugar for term vectors...?

  /** Creates a new un-stored TextField */
  public TextField(String name, Reader reader) {
    super(name, reader, TextField.TYPE_UNSTORED);
  }

  /** Creates a new un-stored TextField */
  public TextField(String name, String value) {
    super(name, value, TextField.TYPE_UNSTORED);
  }
  
  /** Creates a new un-stored TextField */
  public TextField(String name, TokenStream stream) {
    super(name, stream, TextField.TYPE_UNSTORED);
  }
}
