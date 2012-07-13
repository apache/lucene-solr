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

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40PostingsFormat;
import org.apache.lucene.index.AssertingAtomicReader;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;

/**
 * Just like {@link Lucene40PostingsFormat} but with additional asserts.
 */
public class AssertingPostingsFormat extends PostingsFormat {
  private final PostingsFormat in = new Lucene40PostingsFormat();
  
  public AssertingPostingsFormat() {
    super("Asserting");
  }
  
  // TODO: we could add some useful checks here?
  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return in.fieldsConsumer(state);
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new AssertingFieldsProducer(in.fieldsProducer(state));
  }
  
  static class AssertingFieldsProducer extends FieldsProducer {
    private final FieldsProducer in;
    
    AssertingFieldsProducer(FieldsProducer in) {
      this.in = in;
    }
    
    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public FieldsEnum iterator() throws IOException {
      FieldsEnum iterator = in.iterator();
      assert iterator != null;
      return new AssertingAtomicReader.AssertingFieldsEnum(iterator);
    }

    @Override
    public Terms terms(String field) throws IOException {
      Terms terms = in.terms(field);
      return terms == null ? null : new AssertingAtomicReader.AssertingTerms(terms);
    }

    @Override
    public int size() throws IOException {
      return in.size();
    }

    @Override
    public long getUniqueTermCount() throws IOException {
      return in.getUniqueTermCount();
    }
  }
}
