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

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

class CrankyPostingsFormat extends PostingsFormat {
  final PostingsFormat delegate;
  final Random random;
  
  CrankyPostingsFormat(PostingsFormat delegate, Random random) {
    // we impersonate the passed-in codec, so we don't need to be in SPI,
    // and so we dont change file formats
    super(delegate.getName());
    this.delegate = delegate;
    this.random = random;
  }
  
  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    if (random.nextInt(100) == 0) {
      throw new IOException("Fake IOException from PostingsFormat.fieldsConsumer()");
    }  
    return new CrankyFieldsConsumer(delegate.fieldsConsumer(state), random);
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    return delegate.fieldsProducer(state);
  }
  
  static class CrankyFieldsConsumer extends FieldsConsumer {
    final FieldsConsumer delegate;
    final Random random;
    
    CrankyFieldsConsumer(FieldsConsumer delegate, Random random) {
      this.delegate = delegate;
      this.random = random;
    }
    
    @Override
    public void write(Fields fields) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from FieldsConsumer.write()");
      }  
      delegate.write(fields);
    }

    @Override
    public void close() throws IOException {
      delegate.close();
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from FieldsConsumer.close()");
      }  
    }
  }
}
