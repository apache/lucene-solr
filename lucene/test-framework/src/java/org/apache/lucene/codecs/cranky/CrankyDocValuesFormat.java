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

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

class CrankyDocValuesFormat extends DocValuesFormat {
  final DocValuesFormat delegate;
  final Random random;
  
  CrankyDocValuesFormat(DocValuesFormat delegate, Random random) {
    // we impersonate the passed-in codec, so we don't need to be in SPI,
    // and so we dont change file formats
    super(delegate.getName());
    this.delegate = delegate;
    this.random = random;
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    if (random.nextInt(100) == 0) {
      throw new IOException("Fake IOException from DocValuesFormat.fieldsConsumer()");
    }
    return new CrankyDocValuesConsumer(delegate.fieldsConsumer(state), random);
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    return delegate.fieldsProducer(state);
  }
  
  static class CrankyDocValuesConsumer extends DocValuesConsumer {
    final DocValuesConsumer delegate;
    final Random random;
    
    CrankyDocValuesConsumer(DocValuesConsumer delegate, Random random) {
      this.delegate = delegate;
      this.random = random;
    }
    
    @Override
    public void close() throws IOException {
      delegate.close();
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from DocValuesConsumer.close()");
      }
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from DocValuesConsumer.addNumericField()");
      }
      delegate.addNumericField(field, valuesProducer);
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from DocValuesConsumer.addBinaryField()");
      }
      delegate.addBinaryField(field, valuesProducer);
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from DocValuesConsumer.addSortedField()");
      }
      delegate.addSortedField(field, valuesProducer);
    }
    
    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from DocValuesConsumer.addSortedNumericField()");
      }
      delegate.addSortedNumericField(field, valuesProducer);
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from DocValuesConsumer.addSortedSetField()");
      }
      delegate.addSortedSetField(field, valuesProducer);
    }
  }
}
