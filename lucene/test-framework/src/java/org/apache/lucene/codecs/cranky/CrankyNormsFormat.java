package org.apache.lucene.codecs.cranky;

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
import java.util.Random;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

class CrankyNormsFormat extends NormsFormat {
  final NormsFormat delegate;
  final Random random;
  
  CrankyNormsFormat(NormsFormat delegate, Random random) {
    this.delegate = delegate;
    this.random = random;
  }

  @Override
  public DocValuesConsumer normsConsumer(SegmentWriteState state) throws IOException {
    if (random.nextInt(100) == 0) {
      throw new IOException("Fake IOException from NormsFormat.fieldsConsumer()");
    }
    return new CrankyDocValuesFormat.CrankyDocValuesConsumer(delegate.normsConsumer(state), random);
  }

  @Override
  public DocValuesProducer normsProducer(SegmentReadState state) throws IOException {
    return delegate.normsProducer(state);
  }
}
