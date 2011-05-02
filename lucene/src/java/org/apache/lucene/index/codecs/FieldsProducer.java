package org.apache.lucene.index.codecs;

/**
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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.values.DocValues;

/** Abstract API that consumes terms, doc, freq, prox and
 *  payloads postings.  Concrete implementations of this
 *  actually do "something" with the postings (write it into
 *  the index in a specific format).
 *
 * @lucene.experimental
 */

public abstract class FieldsProducer extends Fields implements Closeable {
  public abstract void close() throws IOException;
  public abstract void loadTermsIndex(int indexDivisor) throws IOException;

  public static final FieldsProducer EMPTY = new FieldsProducer() {
    
    @Override
    public Terms terms(String field) throws IOException {
      return null;
    }
    
    @Override
    public FieldsEnum iterator() throws IOException {
      return FieldsEnum.EMPTY;
    }
    
    @Override
    public void loadTermsIndex(int indexDivisor) throws IOException {
      
    }
    
    @Override
    public void close() throws IOException {
      
    }
  };
  
}
