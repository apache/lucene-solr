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
package org.apache.lucene.codecs;


import java.io.IOException;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/** 
 * Encodes/decodes indexed points.
 *
 * @lucene.experimental */
public abstract class PointsFormat {

  /**
   * Creates a new point format.
   */
  protected PointsFormat() {
  }

  /** Writes a new segment */
  public abstract PointsWriter fieldsWriter(SegmentWriteState state) throws IOException;

  /** Reads a segment.  NOTE: by the time this call
   *  returns, it must hold open any files it will need to
   *  use; else, those files may be deleted. 
   *  Additionally, required files may be deleted during the execution of 
   *  this call before there is a chance to open them. Under these 
   *  circumstances an IOException should be thrown by the implementation. 
   *  IOExceptions are expected and will automatically cause a retry of the 
   *  segment opening logic with the newly revised segments.
   *  */
  public abstract PointsReader fieldsReader(SegmentReadState state) throws IOException;

  /** A {@code PointsFormat} that has nothing indexed */
  public static final PointsFormat EMPTY = new PointsFormat() {
      @Override
      public PointsWriter fieldsWriter(SegmentWriteState state) {
        throw new UnsupportedOperationException();
      }

      @Override
      public PointsReader fieldsReader(SegmentReadState state) {
        return new PointsReader() {
          @Override
          public void close() {
          }

          @Override
          public long ramBytesUsed() {
            return 0L;
          }

          @Override
          public void checkIntegrity() {
          }

          @Override
          public PointValues getValues(String field) {
            throw new IllegalArgumentException("field=\"" + field + "\" was not indexed with points");
          }
        };
      }
    };
}
