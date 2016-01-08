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
import java.util.Collection;

import org.apache.lucene.codecs.DimensionalFormat;
import org.apache.lucene.codecs.DimensionalReader;
import org.apache.lucene.codecs.DimensionalWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.TestUtil;

/**
 * Just like the default dimensional format but with additional asserts.
 */

public final class AssertingDimensionalFormat extends DimensionalFormat {
  private final DimensionalFormat in = TestUtil.getDefaultCodec().dimensionalFormat();
  
  @Override
  public DimensionalWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new AssertingDimensionalWriter(state, in.fieldsWriter(state));
  }

  @Override
  public DimensionalReader fieldsReader(SegmentReadState state) throws IOException {
    return new AssertingDimensionalReader(in.fieldsReader(state));
  }
  
  static class AssertingDimensionalReader extends DimensionalReader {
    private final DimensionalReader in;
    
    AssertingDimensionalReader(DimensionalReader in) {
      this.in = in;
      // do a few simple checks on init
      assert toString() != null;
      assert ramBytesUsed() >= 0;
      assert getChildResources() != null;
    }
    
    @Override
    public void close() throws IOException {
      in.close();
      in.close(); // close again
    }

    @Override
    public void intersect(String fieldName, IntersectVisitor visitor) throws IOException {
      // TODO: wrap the visitor and make sure things are being reasonable
      in.intersect(fieldName, visitor);
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
    public DimensionalReader getMergeInstance() throws IOException {
      return new AssertingDimensionalReader(in.getMergeInstance());
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + in.toString() + ")";
    }

    @Override
    public byte[] getMinPackedValue(String fieldName) throws IOException {
      return in.getMinPackedValue(fieldName);
    }

    @Override
    public byte[] getMaxPackedValue(String fieldName) throws IOException {
      return in.getMaxPackedValue(fieldName);
    }

    @Override
    public int getNumDimensions(String fieldName) throws IOException {
      return in.getNumDimensions(fieldName);
    }

    @Override
    public int getBytesPerDimension(String fieldName) throws IOException {
      return in.getBytesPerDimension(fieldName);
    }
  }

  static class AssertingDimensionalWriter extends DimensionalWriter {
    private final DimensionalWriter in;

    AssertingDimensionalWriter(SegmentWriteState writeState, DimensionalWriter in) {
      this.in = in;
    }
    
    @Override
    public void writeField(FieldInfo fieldInfo, DimensionalReader values) throws IOException {
      if (fieldInfo.getDimensionCount() == 0) {
        throw new IllegalArgumentException("writing field=\"" + fieldInfo.name + "\" but dimensionalCount is 0");
      }
      in.writeField(fieldInfo, values);
    }

    @Override
    public void merge(MergeState mergeState) throws IOException {
      in.merge(mergeState);
    }

    @Override
    public void close() throws IOException {
      in.close();
      in.close(); // close again
    }
  }
}
