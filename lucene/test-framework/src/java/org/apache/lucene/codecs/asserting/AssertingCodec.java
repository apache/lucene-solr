package org.apache.lucene.codecs.asserting;

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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40Codec; // javadocs @link
import org.apache.lucene.codecs.lucene40.Lucene40DocValuesFormat;
import org.apache.lucene.codecs.lucene40.Lucene40FieldInfosFormat;
import org.apache.lucene.codecs.lucene40.Lucene40LiveDocsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40NormsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40SegmentInfoFormat;
import org.apache.lucene.codecs.lucene40.Lucene40StoredFieldsFormat;

/**
 * Acts like {@link Lucene40Codec} but with additional asserts.
 */
public class AssertingCodec extends Codec {

  private final PostingsFormat postings = new AssertingPostingsFormat();
  private final SegmentInfoFormat infos = new Lucene40SegmentInfoFormat();
  private final StoredFieldsFormat fields = new Lucene40StoredFieldsFormat();
  private final FieldInfosFormat fieldInfos = new Lucene40FieldInfosFormat();
  private final TermVectorsFormat vectors = new AssertingTermVectorsFormat();
  private final DocValuesFormat docValues = new Lucene40DocValuesFormat();
  private final NormsFormat norms = new Lucene40NormsFormat();
  private final LiveDocsFormat liveDocs = new Lucene40LiveDocsFormat();
  
  public AssertingCodec() {
    super("Asserting");
  }

  @Override
  public PostingsFormat postingsFormat() {
    return postings;
  }

  @Override
  public DocValuesFormat docValuesFormat() {
    return docValues;
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return fields;
  }

  @Override
  public TermVectorsFormat termVectorsFormat() {
    return vectors;
  }

  @Override
  public FieldInfosFormat fieldInfosFormat() {
    return fieldInfos;
  }

  @Override
  public SegmentInfoFormat segmentInfoFormat() {
    return infos;
  }

  @Override
  public NormsFormat normsFormat() {
    return norms;
  }

  @Override
  public LiveDocsFormat liveDocsFormat() {
    return liveDocs;
  }
}
