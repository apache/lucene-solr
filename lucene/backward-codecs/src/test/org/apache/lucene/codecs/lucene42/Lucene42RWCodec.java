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
package org.apache.lucene.codecs.lucene42;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40RWSegmentInfoFormat;
import org.apache.lucene.codecs.lucene41.Lucene41RWPostingsFormat;
import org.apache.lucene.codecs.lucene41.Lucene41RWStoredFieldsFormat;

/**
 * Read-Write version of 4.2 codec for testing
 * @deprecated for test purposes only
 */
@Deprecated
public final class Lucene42RWCodec extends Lucene42Codec {

  private static final DocValuesFormat dv = new Lucene42RWDocValuesFormat();
  private static final NormsFormat norms = new Lucene42RWNormsFormat();
  private static final StoredFieldsFormat storedFields = new Lucene41RWStoredFieldsFormat();
  private static final FieldInfosFormat fieldInfosFormat = new Lucene42RWFieldInfosFormat();
  
  private final PostingsFormat postings = new Lucene41RWPostingsFormat();
  
  @Override
  public PostingsFormat getPostingsFormatForField(String field) {
    return postings;
  }

  @Override
  public DocValuesFormat getDocValuesFormatForField(String field) {
    return dv;
  }

  @Override
  public NormsFormat normsFormat() {
    return norms;
  }
  
  @Override
  public FieldInfosFormat fieldInfosFormat() {
    return fieldInfosFormat;
  }  
  
  private static final SegmentInfoFormat segmentInfos = new Lucene40RWSegmentInfoFormat();

  @Override
  public SegmentInfoFormat segmentInfoFormat() {
    return segmentInfos;
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return storedFields;
  }
  
  private final TermVectorsFormat vectorsFormat = new Lucene42RWTermVectorsFormat();

  @Override
  public TermVectorsFormat termVectorsFormat() {
    return vectorsFormat;
  }
}
