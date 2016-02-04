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
package org.apache.lucene.codecs.cranky;

import java.util.Random;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;

/** Codec for testing that throws random IOExceptions */
public class CrankyCodec extends FilterCodec {
  final Random random;
  
  /** 
   * Wrap the provided codec with crankiness.
   * Try passing Asserting for the most fun.
   */
  public CrankyCodec(Codec delegate, Random random) {
    // we impersonate the passed-in codec, so we don't need to be in SPI,
    // and so we dont change file formats
    super(delegate.getName(), delegate);
    this.random = random;
  }

  @Override
  public DocValuesFormat docValuesFormat() {
    return new CrankyDocValuesFormat(delegate.docValuesFormat(), random);
  }

  @Override
  public FieldInfosFormat fieldInfosFormat() {
    return new CrankyFieldInfosFormat(delegate.fieldInfosFormat(), random);
  }

  @Override
  public LiveDocsFormat liveDocsFormat() {
    return new CrankyLiveDocsFormat(delegate.liveDocsFormat(), random);
  }

  @Override
  public NormsFormat normsFormat() {
    return new CrankyNormsFormat(delegate.normsFormat(), random);
  }

  @Override
  public PostingsFormat postingsFormat() {
    return new CrankyPostingsFormat(delegate.postingsFormat(), random);
  }

  @Override
  public SegmentInfoFormat segmentInfoFormat() {
    return new CrankySegmentInfoFormat(delegate.segmentInfoFormat(), random);
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return new CrankyStoredFieldsFormat(delegate.storedFieldsFormat(), random);
  }

  @Override
  public TermVectorsFormat termVectorsFormat() {
    return new CrankyTermVectorsFormat(delegate.termVectorsFormat(), random);
  }

  @Override
  public CompoundFormat compoundFormat() {
    return new CrankyCompoundFormat(delegate.compoundFormat(), random);
  }

  @Override
  public String toString() {
    return "Cranky(" + delegate + ")";
  }
}
