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

import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

class CrankyPointsFormat extends PointsFormat {
  PointsFormat delegate;
  Random random;
  
  CrankyPointsFormat(PointsFormat delegate, Random random) {
    this.delegate = delegate;
    this.random = random;
  }

  @Override
  public PointsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new CrankyPointsWriter(delegate.fieldsWriter(state), random);
  }

  @Override
  public PointsReader fieldsReader(SegmentReadState state) throws IOException {
    return new CrankyPointsReader(delegate.fieldsReader(state), random);
  }

  static class CrankyPointsWriter extends PointsWriter {
    final PointsWriter delegate;
    final Random random;

    public CrankyPointsWriter(PointsWriter delegate, Random random) {
      this.delegate = delegate;
      this.random = random;
    }

    @Override
    public void writeField(FieldInfo fieldInfo, PointsReader values) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException");
      }  
      delegate.writeField(fieldInfo, values);
    }

    @Override
    public void finish() throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException");
      }  
      delegate.finish();
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException");
      }  
    }

    @Override
    public void merge(MergeState mergeState) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException");
      }  
      delegate.merge(mergeState);
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException");
      }  
    }

    @Override
    public void close() throws IOException {
      delegate.close();
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException");
      }  
    }
  }

  static class CrankyPointsReader extends PointsReader {
    final PointsReader delegate;
    final Random random;
    public CrankyPointsReader(PointsReader delegate, Random random) {
      this.delegate = delegate;
      this.random = random;
    }

    @Override
    public void checkIntegrity() throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException");
      }
      delegate.checkIntegrity();
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException");
      }  
    }

    @Override
    public void intersect(String fieldName, IntersectVisitor visitor) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException");
      }
      delegate.intersect(fieldName, visitor);
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException");
      }  
    }

    @Override
    public long estimatePointCount(String fieldName, IntersectVisitor visitor) {
      return delegate.estimatePointCount(fieldName, visitor);
    }

    @Override
    public byte[] getMinPackedValue(String fieldName) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException");
      }
      return delegate.getMinPackedValue(fieldName);
    }

    @Override
    public byte[] getMaxPackedValue(String fieldName) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException");
      }
      return delegate.getMaxPackedValue(fieldName);
    }

    @Override
    public int getNumDimensions(String fieldName) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException");
      }
      return delegate.getNumDimensions(fieldName);
    }

    @Override
    public int getBytesPerDimension(String fieldName) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException");
      }
      return delegate.getBytesPerDimension(fieldName);
    }

    @Override
    public void close() throws IOException {
      delegate.close();
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException");
      }  
    }

    @Override
    public long ramBytesUsed() {
      return delegate.ramBytesUsed();
    }

    @Override
    public long size(String fieldName) {
      return delegate.size(fieldName);
    }

    @Override
    public int getDocCount(String fieldName) {
      return delegate.getDocCount(fieldName);
    }
  }
}
