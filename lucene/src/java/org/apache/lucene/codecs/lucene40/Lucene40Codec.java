package org.apache.lucene.codecs.lucene40;

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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfosFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;

/**
 * Implements the Lucene 4.0 index format, with configurable per-field postings formats.
 *
 * @lucene.experimental
 */
// NOTE: if we make largish changes in a minor release, easier to just make Lucene42Codec or whatever
// if they are backwards compatible or smallish we can probably do the backwards in the postingsreader
// (it writes a minor version, etc).
public class Lucene40Codec extends Codec {
  private final StoredFieldsFormat fieldsFormat = new Lucene40StoredFieldsFormat();
  private final TermVectorsFormat vectorsFormat = new Lucene40TermVectorsFormat();
  private final FieldInfosFormat fieldInfosFormat = new Lucene40FieldInfosFormat();
  private final DocValuesFormat docValuesFormat = new Lucene40DocValuesFormat();
  private final SegmentInfosFormat infosFormat = new Lucene40SegmentInfosFormat();
  private final NormsFormat normsFormat = new Lucene40NormsFormat();
  private final LiveDocsFormat liveDocsFormat = new Lucene40LiveDocsFormat();
  
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
  public StoredFieldsFormat storedFieldsFormat() {
    return fieldsFormat;
  }
  
  @Override
  public TermVectorsFormat termVectorsFormat() {
    return vectorsFormat;
  }

  @Override
  public DocValuesFormat docValuesFormat() {
    return docValuesFormat;
  }

  @Override
  public PostingsFormat postingsFormat() {
    return postingsFormat;
  }
  
  @Override
  public FieldInfosFormat fieldInfosFormat() {
    return fieldInfosFormat;
  }
  
  @Override
  public SegmentInfosFormat segmentInfosFormat() {
    return infosFormat;
  }

  @Override
  public NormsFormat normsFormat() {
    return normsFormat;
  }
  
  @Override
  public LiveDocsFormat liveDocsFormat() {
    return liveDocsFormat;
  }

  /** Returns the postings format that should be used for writing 
   *  new segments of <code>field</code>.
   *  
   *  The default implementation always returns "Lucene40"
   */
  public PostingsFormat getPostingsFormatForField(String field) {
    return defaultFormat;
  }
  
  private final PostingsFormat defaultFormat = PostingsFormat.forName("Lucene40");
}
