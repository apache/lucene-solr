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
package org.apache.lucene.codecs.lucene40;

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;

/**
 * Read-write version of 4.0 codec for testing
 * @deprecated for test purposes only
 */
@Deprecated
public final class Lucene40RWCodec extends Lucene40Codec {
  
  private final FieldInfosFormat fieldInfos = new Lucene40RWFieldInfosFormat();
  private final DocValuesFormat docValues = new Lucene40RWDocValuesFormat();
  private final NormsFormat norms = new Lucene40RWNormsFormat();
  private final StoredFieldsFormat stored = new Lucene40RWStoredFieldsFormat();
  private final TermVectorsFormat vectors = new Lucene40RWTermVectorsFormat();
  private final PostingsFormat postings = new Lucene40RWPostingsFormat();
  
  @Override
  public FieldInfosFormat fieldInfosFormat() {
    return fieldInfos;
  }

  @Override
  public DocValuesFormat docValuesFormat() {
    return docValues;
  }

  @Override
  public NormsFormat normsFormat() {
    return norms;
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return stored;
  }

  @Override
  public TermVectorsFormat termVectorsFormat() {
    return vectors;
  }

  @Override
  public PostingsFormat getPostingsFormatForField(String field) {
    return postings;
  }
  
  private static final SegmentInfoFormat segmentInfos = new Lucene40RWSegmentInfoFormat();

  @Override
  public SegmentInfoFormat segmentInfoFormat() {
    return segmentInfos;
  }
}
