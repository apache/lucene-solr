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

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.FieldInfo;
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
  public NormsConsumer normsConsumer(SegmentWriteState state) throws IOException {
    if (random.nextInt(100) == 0) {
      throw new IOException("Fake IOException from NormsFormat.normsConsumer()");
    }
    return new CrankyNormsConsumer(delegate.normsConsumer(state), random);
  }

  @Override
  public NormsProducer normsProducer(SegmentReadState state) throws IOException {
    return delegate.normsProducer(state);
  }
  
  static class CrankyNormsConsumer extends NormsConsumer {
    final NormsConsumer delegate;
    final Random random;
    
    CrankyNormsConsumer(NormsConsumer delegate, Random random) {
      this.delegate = delegate;
      this.random = random;
    }
    
    @Override
    public void close() throws IOException {
      delegate.close();
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from NormsConsumer.close()");
      }
    }

    @Override
    public void addNormsField(FieldInfo field, NormsProducer valuesProducer) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from NormsConsumer.addNormsField()");
      }
      delegate.addNormsField(field, valuesProducer);
    }
  }
}
