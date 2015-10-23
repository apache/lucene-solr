package org.apache.lucene.codecs;

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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;

/** Abstract API to write dimensional values
 *
 * @lucene.experimental
 */

public abstract class DimensionalWriter implements Closeable {
  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected DimensionalWriter() {
  }

  /** Write all values contained in the provided reader */
  public abstract void writeField(FieldInfo fieldInfo, DimensionalReader values) throws IOException;

  /** Default merge implementation to merge incoming dimensional readers by visiting all their points and
   *  adding to this writer */
  public void merge(MergeState mergeState) throws IOException {
    for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
      if (fieldInfo.getDimensionCount() != 0) {
        writeField(fieldInfo,
                   new DimensionalReader() {
                     @Override
                     public void intersect(String fieldName, IntersectVisitor mergedVisitor) throws IOException {
                       if (fieldName.equals(fieldInfo.name) == false) {
                         throw new IllegalArgumentException("field name must match the field being merged");
                       }
                       for (int i=0;i<mergeState.dimensionalReaders.length;i++) {
                         DimensionalReader dimensionalReader = mergeState.dimensionalReaders[i];
                         MergeState.DocMap docMap = mergeState.docMaps[i];
                         int docBase = mergeState.docBase[i];
                         dimensionalReader.intersect(fieldInfo.name,
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
                                                           mergedVisitor.visit(docBase + newDocID, packedValue);
                                                         }
                                                       }

                                                       @Override
                                                       public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                                                         // Forces this segment's DimensionalReader to always visit all docs + values:
                                                         return Relation.QUERY_CROSSES_CELL;
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
                   });
      }
    }
  }
}
