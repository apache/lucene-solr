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

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.AssertingLeafReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.TestUtil;

/**
 * Just like the default but with additional asserts.
 */
public class AssertingNormsFormat extends NormsFormat {
  private final NormsFormat in = TestUtil.getDefaultCodec().normsFormat();
  
  @Override
  public NormsConsumer normsConsumer(SegmentWriteState state) throws IOException {
    NormsConsumer consumer = in.normsConsumer(state);
    assert consumer != null;
    return new AssertingNormsConsumer(consumer, state.segmentInfo.maxDoc());
  }

  @Override
  public NormsProducer normsProducer(SegmentReadState state) throws IOException {
    assert state.fieldInfos.hasNorms();
    NormsProducer producer = in.normsProducer(state);
    assert producer != null;
    return new AssertingNormsProducer(producer, state.segmentInfo.maxDoc());
  }
  
  static class AssertingNormsConsumer extends NormsConsumer {
    private final NormsConsumer in;
    private final int maxDoc;
    
    AssertingNormsConsumer(NormsConsumer in, int maxDoc) {
      this.in = in;
      this.maxDoc = maxDoc;
    }

    @Override
    public void addNormsField(FieldInfo field, Iterable<Number> values) throws IOException {
      int count = 0;
      for (Number v : values) {
        assert v != null;
        count++;
      }
      assert count == maxDoc;
      TestUtil.checkIterator(values.iterator(), maxDoc, false);
      in.addNormsField(field, values);
    }
    
    @Override
    public void close() throws IOException {
      in.close();
      in.close(); // close again
    }
  }
  
  static class AssertingNormsProducer extends NormsProducer {
    private final NormsProducer in;
    private final int maxDoc;
    
    AssertingNormsProducer(NormsProducer in, int maxDoc) {
      this.in = in;
      this.maxDoc = maxDoc;
      // do a few simple checks on init
      assert toString() != null;
      assert ramBytesUsed() >= 0;
      assert getChildResources() != null;
    }

    @Override
    public NumericDocValues getNorms(FieldInfo field) throws IOException {
      assert field.hasNorms();
      NumericDocValues values = in.getNorms(field);
      assert values != null;
      return new AssertingLeafReader.AssertingNumericDocValues(values, maxDoc);
    }

    @Override
    public void close() throws IOException {
      in.close();
      in.close(); // close again
    }

    @Override
    public long ramBytesUsed() {
      long v = in.ramBytesUsed();
      assert v >= 0;
      return v;
    }
    
    @Override
    public Collection<Accountable> getChildResources() {
      Collection<Accountable> res = in.getChildResources();
      TestUtil.checkReadOnly(res);
      return res;
    }

    @Override
    public void checkIntegrity() throws IOException {
      in.checkIntegrity();
    }
    
    @Override
    public NormsProducer getMergeInstance() throws IOException {
      return new AssertingNormsProducer(in.getMergeInstance(), maxDoc);
    }
    
    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + in.toString() + ")";
    }
  }
}
