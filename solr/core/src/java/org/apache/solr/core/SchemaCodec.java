package org.apache.solr.core;

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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.codecs.lucene40.Lucene40Codec;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

/**
 * Selects a codec that implements different postingsformats based 
 * on a {@link IndexSchema}. This {@link Codec} also supports dynamic 
 * fields such that not all field formats need to be known in advance
 */
public final class SchemaCodec extends Lucene40Codec {
  private final IndexSchema schema;

  public SchemaCodec(IndexSchema schema) {
    this.schema = schema;
  }

  @Override
  public String getPostingsFormatForField(FieldInfo field) {
    final SchemaField fieldOrNull = schema.getFieldOrNull(field.name);
    if (fieldOrNull == null) {
      throw new IllegalArgumentException("no such field " + field.name);
    }
    String codecName = fieldOrNull.getType().getCodec();
    if (codecName != null) {
      return codecName;
    }
    return super.getPostingsFormatForField(field);
  }
}
