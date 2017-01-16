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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
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

  /** Validates in the 1D case that all points are visited in order, and point values are in bounds of the last cell checked */
  static class AssertingIntersectVisitor implements IntersectVisitor {
    final IntersectVisitor in;
    final int numDims;
    final int bytesPerDim;
    final byte[] lastDocValue;
    final byte[] lastMinPackedValue;
    final byte[] lastMaxPackedValue;
    private Relation lastCompareResult;
    private int lastDocID = -1;
    private int docBudget;

    public AssertingIntersectVisitor(int numDims, int bytesPerDim, IntersectVisitor in) {
      this.in = in;
      this.numDims = numDims;
      this.bytesPerDim = bytesPerDim;
      lastMaxPackedValue = new byte[numDims*bytesPerDim];
      lastMinPackedValue = new byte[numDims*bytesPerDim];
      if (numDims == 1) {
        lastDocValue = new byte[bytesPerDim];
      } else {
        lastDocValue = null;
      }
    }

    @Override
    public void visit(int docID) throws IOException {
      assert --docBudget >= 0 : "called add() more times than the last call to grow() reserved";

      // This method, not filtering each hit, should only be invoked when the cell is inside the query shape:
      assert lastCompareResult == Relation.CELL_INSIDE_QUERY;
      in.visit(docID);
    }

    @Override
    public void visit(int docID, byte[] packedValue) throws IOException {
      assert --docBudget >= 0 : "called add() more times than the last call to grow() reserved";

      // This method, to filter each doc's value, should only be invoked when the cell crosses the query shape:
      assert lastCompareResult == PointValues.Relation.CELL_CROSSES_QUERY;

      // This doc's packed value should be contained in the last cell passed to compare:
      for(int dim=0;dim<numDims;dim++) {
        assert StringHelper.compare(bytesPerDim, lastMinPackedValue, dim*bytesPerDim, packedValue, dim*bytesPerDim) <= 0: "dim=" + dim + " of " +  numDims + " value=" + new BytesRef(packedValue);
        assert StringHelper.compare(bytesPerDim, lastMaxPackedValue, dim*bytesPerDim, packedValue, dim*bytesPerDim) >= 0: "dim=" + dim + " of " +  numDims + " value=" + new BytesRef(packedValue);
      }

      // TODO: we should assert that this "matches" whatever relation the last call to compare had returned
      assert packedValue.length == numDims * bytesPerDim;
      if (numDims == 1) {
        int cmp = StringHelper.compare(bytesPerDim, lastDocValue, 0, packedValue, 0);
        if (cmp < 0) {
          // ok
        } else if (cmp == 0) {
          assert lastDocID <= docID: "doc ids are out of order when point values are the same!";
        } else {
          // out of order!
          assert false: "point values are out of order";
        }
        System.arraycopy(packedValue, 0, lastDocValue, 0, bytesPerDim);
        lastDocID = docID;
      }
      in.visit(docID, packedValue);
    }

    @Override
    public void grow(int count) {
      in.grow(count);
      docBudget = count;
    }

    @Override
    public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
      for(int dim=0;dim<numDims;dim++) {
        assert StringHelper.compare(bytesPerDim, minPackedValue, dim*bytesPerDim, maxPackedValue, dim*bytesPerDim) <= 0;
      }
      System.arraycopy(maxPackedValue, 0, lastMaxPackedValue, 0, numDims*bytesPerDim);
      System.arraycopy(minPackedValue, 0, lastMinPackedValue, 0, numDims*bytesPerDim);
      lastCompareResult = in.compare(minPackedValue, maxPackedValue);
      return lastCompareResult;
    }
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
    public void intersect(String fieldName, IntersectVisitor visitor) throws IOException {
      in.intersect(fieldName,
                   new AssertingIntersectVisitor(in.getNumDimensions(fieldName), in.getBytesPerDimension(fieldName), visitor));
    }

    @Override
    public long estimatePointCount(String fieldName, IntersectVisitor visitor) {
      final long value = in.estimatePointCount(fieldName, visitor);
      assert value >= 0;
      return value;
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

    @Override
    public byte[] getMinPackedValue(String fieldName) throws IOException {
      assertStats(fieldName);
      return in.getMinPackedValue(fieldName);
    }

    @Override
    public byte[] getMaxPackedValue(String fieldName) throws IOException {
      assertStats(fieldName);
      return in.getMaxPackedValue(fieldName);
    }

    @Override
    public int getNumDimensions(String fieldName) throws IOException {
      assertStats(fieldName);
      return in.getNumDimensions(fieldName);
    }

    @Override
    public int getBytesPerDimension(String fieldName) throws IOException {
      assertStats(fieldName);
      return in.getBytesPerDimension(fieldName);
    }

    @Override
    public long size(String fieldName) {
      assertStats(fieldName);
      return in.size(fieldName);
    }

    @Override
    public int getDocCount(String fieldName) {
      assertStats(fieldName);
      return in.getDocCount(fieldName);
    }

    private void assertStats(String fieldName) {
      assert in.size(fieldName) >= 0;
      assert in.getDocCount(fieldName) >= 0;
      assert in.getDocCount(fieldName) <= in.size(fieldName);
      assert in.getDocCount(fieldName) <= maxDoc;
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
