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


import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;

/** Abstract API to write points
 *
 * @lucene.experimental
 */

public abstract class PointsWriter implements Closeable {
  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected PointsWriter() {
  }

  /** Write all values contained in the provided reader */
  public abstract void writeField(FieldInfo fieldInfo, PointsReader values) throws IOException;

  /** Default naive merge implementation for one field: it just re-indexes all the values
   *  from the incoming segment.  The default codec overrides this for 1D fields and uses
   *  a faster but more complex implementation. */
  protected void mergeOneField(MergeState mergeState, FieldInfo fieldInfo) throws IOException {
    long maxPointCount = 0;
    int docCount = 0;
    for (int i=0;i<mergeState.pointsReaders.length;i++) {
      PointsReader pointsReader = mergeState.pointsReaders[i];
      if (pointsReader != null) {
        FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(fieldInfo.name);
        if (readerFieldInfo != null && readerFieldInfo.getPointDimensionCount() > 0) {
          maxPointCount += pointsReader.size(fieldInfo.name);
          docCount += pointsReader.getDocCount(fieldInfo.name);
        }
      }
    }
    final long finalMaxPointCount = maxPointCount;
    final int finalDocCount = docCount;
    writeField(fieldInfo,
               new PointsReader() {
                 @Override
                 public void intersect(String fieldName, IntersectVisitor mergedVisitor) throws IOException {
                   if (fieldName.equals(fieldInfo.name) == false) {
                     throw new IllegalArgumentException("field name must match the field being merged");
                   }
                   
                   for (int i=0;i<mergeState.pointsReaders.length;i++) {
                     PointsReader pointsReader = mergeState.pointsReaders[i];
                     if (pointsReader == null) {
                       // This segment has no points
                       continue;
                     }
                     FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(fieldName);
                     if (readerFieldInfo == null) {
                       // This segment never saw this field
                       continue;
                     }

                     if (readerFieldInfo.getPointDimensionCount() == 0) {
                       // This segment saw this field, but the field did not index points in it:
                       continue;
                     }

                     MergeState.DocMap docMap = mergeState.docMaps[i];
                     pointsReader.intersect(fieldInfo.name,
                                            new IntersectVisitor() {
                                              @Override
                                              public void visit(int docID) {
                                                // Should never be called because our compare method never returns Relation.CELL_INSIDE_QUERY
                                                throw new IllegalStateException();
                                              }

                                              @Override
                                              public void visit(int docID, byte[] packedValue) throws IOException {
                                                int newDocID = docMap.get(docID);
                                                if (newDocID != -1) {
                                                  // Not deleted:
                                                  mergedVisitor.visit(newDocID, packedValue);
                                                }
                                              }

                                              @Override
                                              public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                                                // Forces this segment's PointsReader to always visit all docs + values:
                                                return Relation.CELL_CROSSES_QUERY;
                                              }
                                            });
                   }
                 }

                 @Override
                 public void checkIntegrity() {
                   throw new UnsupportedOperationException();
                 }

                 @Override
                 public long ramBytesUsed() {
                   return 0L;
                 }

                 @Override
                 public void close() {
                 }

                 @Override
                 public long estimatePointCount(String fieldName, IntersectVisitor visitor) {
                   throw new UnsupportedOperationException();
                 }

                 @Override
                 public byte[] getMinPackedValue(String fieldName) {
                   throw new UnsupportedOperationException();
                 }

                 @Override
                 public byte[] getMaxPackedValue(String fieldName) {
                   throw new UnsupportedOperationException();
                 }

                 @Override
                 public int getNumDimensions(String fieldName) {
                   throw new UnsupportedOperationException();
                 }

                 @Override
                 public int getBytesPerDimension(String fieldName) {
                   throw new UnsupportedOperationException();
                 }

                 @Override
                 public long size(String fieldName) {
                   return finalMaxPointCount;
                 }

                 @Override
                 public int getDocCount(String fieldName) {
                   return finalDocCount;
                 }
               });
  }

  /** Default merge implementation to merge incoming points readers by visiting all their points and
   *  adding to this writer */
  public void merge(MergeState mergeState) throws IOException {
    // check each incoming reader
    for (PointsReader reader : mergeState.pointsReaders) {
      if (reader != null) {
        reader.checkIntegrity();
      }
    }
    // merge field at a time
    for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
      if (fieldInfo.getPointDimensionCount() != 0) {
        mergeOneField(mergeState, fieldInfo);
      }
    }
    finish();
  }

  /** Called once at the end before close */
  public abstract void finish() throws IOException;
}
