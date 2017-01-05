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

import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.index.AssertingLeafReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.TestUtil;

/**
 * Just like the default point format but with additional asserts.
 */

public final class AssertingPointsFormat extends PointsFormat {
  private final PointsFormat in;

  /** Create a new AssertingPointsFormat */
  public AssertingPointsFormat() {
    this(TestUtil.getDefaultCodec().pointsFormat());
  }

  /**
   * Expert: Create an AssertingPointsFormat.
   * This is only intended to pass special parameters for testing.
   */
  // TODO: can we randomize this a cleaner way? e.g. stored fields and vectors do
  // this with a separate codec...
  public AssertingPointsFormat(PointsFormat in) {
    this.in = in;
  }
  
  @Override
  public PointsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new AssertingPointsWriter(state, in.fieldsWriter(state));
  }

  @Override
  public PointsReader fieldsReader(SegmentReadState state) throws IOException {
    return new AssertingPointsReader(state.segmentInfo.maxDoc(), in.fieldsReader(state));
  }

  
  static class AssertingPointsReader extends PointsReader {
    private final PointsReader in;
    private final int maxDoc;
    
    AssertingPointsReader(int maxDoc, PointsReader in) {
      this.in = in;
      this.maxDoc = maxDoc;
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
    public PointValues getValues(String field) throws IOException {
      PointValues values = this.in.getValues(field);
      if (values == null) {
        return null;
      }
      return new AssertingLeafReader.AssertingPointValues(values, maxDoc);
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
    public PointsReader getMergeInstance() throws IOException {
      return new AssertingPointsReader(maxDoc, in.getMergeInstance());
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + in.toString() + ")";
    }
  }

  static class AssertingPointsWriter extends PointsWriter {
    private final PointsWriter in;

    AssertingPointsWriter(SegmentWriteState writeState, PointsWriter in) {
      this.in = in;
    }
    
    @Override
    public void writeField(FieldInfo fieldInfo, PointsReader values) throws IOException {
      if (fieldInfo.getPointDimensionCount() == 0) {
        throw new IllegalArgumentException("writing field=\"" + fieldInfo.name + "\" but pointDimensionalCount is 0");
      }
      in.writeField(fieldInfo, values);
    }

    @Override
    public void merge(MergeState mergeState) throws IOException {
      in.merge(mergeState);
    }

    @Override
    public void finish() throws IOException {
      in.finish();
    }

    @Override
    public void close() throws IOException {
      in.close();
      in.close(); // close again
    }
  }
}
