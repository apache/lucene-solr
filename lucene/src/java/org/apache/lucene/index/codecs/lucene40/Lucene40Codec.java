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

import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.DefaultDocValuesFormat;
import org.apache.lucene.index.codecs.DefaultFieldsFormat;
import org.apache.lucene.index.codecs.DocValuesFormat;
import org.apache.lucene.index.codecs.FieldsFormat;
import org.apache.lucene.index.codecs.PostingsFormat;
import org.apache.lucene.index.codecs.perfield.PerFieldPostingsFormat;

/**
 * Implements the Lucene 4.0 index format, with configurable per-field postings formats
 * and using {@link DefaultFieldsFormat} for stored fields and {@link
 * DefaultDocValuesFormat} for doc values.
 *
 * @lucene.experimental
 */
// nocommit maybe rename to Lucene4XCodec?  This is supposed
// to "last" through all 4.x releases...?  or maybe not?
public class Lucene40Codec extends Codec {
  private final FieldsFormat fieldsFormat = new DefaultFieldsFormat();
  private final DocValuesFormat docValuesFormat = new DefaultDocValuesFormat();
  private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
      return Lucene40Codec.this.getPostingsFormatForField(field);
    }
  };

  public Lucene40Codec() {
    super("Lucene40");
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
  
  /** Returns the postings format that should be used for writing 
   *  new segments of <code>field</code>.
   *  
   *  The default implementation always returns "Lucene40"
   */
  public PostingsFormat getPostingsFormatForField(String field) {
    return defaultFormat;
  }
  
  // TODO: PostingsFormat.forName("Lucene40") does not work because of Chicken-and-Egg problem:
  private final PostingsFormat defaultFormat = new Lucene40PostingsFormat();
}
