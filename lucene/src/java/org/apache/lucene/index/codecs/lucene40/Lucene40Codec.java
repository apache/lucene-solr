package org.apache.lucene.index.codecs.lucene40;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.DefaultDocValuesFormat;
import org.apache.lucene.index.codecs.DefaultFieldsFormat;
import org.apache.lucene.index.codecs.DocValuesFormat;
import org.apache.lucene.index.codecs.FieldsFormat;
import org.apache.lucene.index.codecs.PostingsFormat;
import org.apache.lucene.index.codecs.memory.MemoryPostingsFormat;
import org.apache.lucene.index.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.codecs.pulsing.PulsingPostingsFormat;
import org.apache.lucene.index.codecs.simpletext.SimpleTextPostingsFormat;

/**
 * Implements the Lucene 4.0 index format, with configurable per-field postings formats
 * and using {@link DefaultFieldsFormat} for stored fields and {@link
 * DefaultDocValuesFormat} for doc values.
 *
 * @lucene.experimental
 */
// TODO: which postings formats will we actually support for backwards compatibility?
public class Lucene40Codec extends Codec {
  private final FieldsFormat fieldsFormat = new DefaultFieldsFormat();
  private final DocValuesFormat docValuesFormat = new DefaultDocValuesFormat();
  private final PostingsFormat postingsFormat;
  private final String defaultPostingsFormat;

  public Lucene40Codec() {
    this(Collections.<String,String>emptyMap());
  }
  
  public Lucene40Codec(Map<String,String> perFieldMap) {
    this("Lucene40", perFieldMap);
  }
  
 public Lucene40Codec(final String defaultPostingsFormat, final Map<String,String> perFieldMap) {
    super("Lucene40");
    this.defaultPostingsFormat = defaultPostingsFormat;

    postingsFormat = new PerFieldPostingsFormat() {

      @Override
      protected String getPostingsFormatForField(FieldInfo field) {
        String format = perFieldMap.get(field.name);
        if (format == null) {
          format = defaultPostingsFormat;
        }
        return format;
      }

      @Override
      protected PostingsFormat getPostingsFormat(String formatName) {
        return CORE_FORMATS.get(formatName);
      }
    };
  }
  
  @Override
  public FieldsFormat fieldsFormat() {
    return fieldsFormat;
  }

  @Override
  public DocValuesFormat docValuesFormat() {
    return docValuesFormat;
  }

  @Override
  public PostingsFormat postingsFormat() {
    return postingsFormat;
  }
  
  // postings formats
  private static final Map<String,PostingsFormat> CORE_FORMATS = new HashMap<String,PostingsFormat>();
  static {
    CORE_FORMATS.put("Lucene40", new Lucene40PostingsFormat());
    CORE_FORMATS.put("Pulsing", new PulsingPostingsFormat());
    CORE_FORMATS.put("SimpleText", new SimpleTextPostingsFormat());
    CORE_FORMATS.put("Memory", new MemoryPostingsFormat());
  }
}
