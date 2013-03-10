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

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;

/**
 * Acts like {@link Lucene42Codec} but with additional asserts.
 */
public final class AssertingCodec extends FilterCodec {

  private final PostingsFormat postings = new AssertingPostingsFormat();
  private final TermVectorsFormat vectors = new AssertingTermVectorsFormat();
  private final StoredFieldsFormat storedFields = new AssertingStoredFieldsFormat();
  private final DocValuesFormat docValues = new AssertingDocValuesFormat();
  private final NormsFormat norms = new AssertingNormsFormat();

  public AssertingCodec() {
    super("Asserting", new Lucene42Codec());
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
}
