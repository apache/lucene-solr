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
package org.apache.lucene.codecs.asserting;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.util.TestUtil;

/**
 * Acts like the default codec but with additional asserts.
 */
public class AssertingCodec extends FilterCodec {

  private final PostingsFormat postings = new PerFieldPostingsFormat() {
    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
      return AssertingCodec.this.getPostingsFormatForField(field);
    }
  };
  
  private final DocValuesFormat docValues = new PerFieldDocValuesFormat() {
    @Override
    public DocValuesFormat getDocValuesFormatForField(String field) {
      return AssertingCodec.this.getDocValuesFormatForField(field);
    }
  };
  
  private final TermVectorsFormat vectors = new AssertingTermVectorsFormat();
  private final StoredFieldsFormat storedFields = new AssertingStoredFieldsFormat();
  private final NormsFormat norms = new AssertingNormsFormat();
  private final LiveDocsFormat liveDocs = new AssertingLiveDocsFormat();
  private final PostingsFormat defaultFormat = new AssertingPostingsFormat();
  private final DocValuesFormat defaultDVFormat = new AssertingDocValuesFormat();
  private final PointsFormat pointsFormat = new AssertingPointsFormat();

  public AssertingCodec() {
    super("Asserting", TestUtil.getDefaultCodec());
  }

  @Override
  public PostingsFormat postingsFormat() {
    return postings;
  }

  @Override
  public TermVectorsFormat termVectorsFormat() {
    return vectors;
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return storedFields;
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
  public LiveDocsFormat liveDocsFormat() {
    return liveDocs;
  }

  @Override
  public PointsFormat pointsFormat() {
    return pointsFormat;
  }

  @Override
  public String toString() {
    return "Asserting(" + delegate + ")";
  }
  
  /** Returns the postings format that should be used for writing 
   *  new segments of <code>field</code>.
   *  
   *  The default implementation always returns "Asserting"
   */
  public PostingsFormat getPostingsFormatForField(String field) {
    return defaultFormat;
  }
  
  /** Returns the docvalues format that should be used for writing 
   *  new segments of <code>field</code>.
   *  
   *  The default implementation always returns "Asserting"
   */
  public DocValuesFormat getDocValuesFormatForField(String field) {
    return defaultDVFormat;
  }
}
