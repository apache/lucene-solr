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

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.asserting.AssertingDocValuesFormat.AssertingDocValuesConsumer;
import org.apache.lucene.codecs.asserting.AssertingDocValuesFormat.AssertingDocValuesProducer;
import org.apache.lucene.codecs.lucene42.Lucene42NormsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Just like {@link Lucene42NormsFormat} but with additional asserts.
 */
public class AssertingNormsFormat extends NormsFormat {
  private final NormsFormat in = new Lucene42NormsFormat();
  
  @Override
  public DocValuesConsumer normsConsumer(SegmentWriteState state) throws IOException {
    DocValuesConsumer consumer = in.normsConsumer(state);
    assert consumer != null;
    return new AssertingDocValuesConsumer(consumer, state.segmentInfo.getDocCount());
  }

  @Override
  public DocValuesProducer normsProducer(SegmentReadState state) throws IOException {
    assert state.fieldInfos.hasNorms();
    DocValuesProducer producer = in.normsProducer(state);
    assert producer != null;
    return new AssertingDocValuesProducer(producer, state.segmentInfo.getDocCount());
  }
}
