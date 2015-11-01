package org.apache.lucene.index;

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
import java.util.ArrayList;
import java.util.List;

class MultiDimensionalValues extends DimensionalValues {

  private final List<DimensionalValues> subs;
  private final List<Integer> docBases;

  private MultiDimensionalValues(List<DimensionalValues> subs, List<Integer> docBases) {
    this.subs = subs;
    this.docBases = docBases;
  }

  public static DimensionalValues get(IndexReader r) {
    final List<LeafReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getDimensionalValues();
    }

    List<DimensionalValues> values = new ArrayList<>();
    List<Integer> docBases = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = leaves.get(i);
      DimensionalValues v = context.reader().getDimensionalValues();
      if (v != null) {
        values.add(v);
        docBases.add(context.docBase);
      }
    }

    if (values.isEmpty()) {
      return null;
    }

    return new MultiDimensionalValues(values, docBases);
  }

  /** Finds all documents and points matching the provided visitor */
  public void intersect(String fieldName, IntersectVisitor visitor) throws IOException {
    for(int i=0;i<subs.size();i++) {
      int docBase = docBases.get(i);
      subs.get(i).intersect(fieldName,
                        new IntersectVisitor() {
                          @Override
                          public void visit(int docID) throws IOException {
                            visitor.visit(docBase+docID);
                          }
                          @Override
                          public void visit(int docID, byte[] packedValue) throws IOException {
                            visitor.visit(docBase+docID, packedValue);
                          }
                          @Override
                          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                            return visitor.compare(minPackedValue, maxPackedValue);
                          }
                        });
    }
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("MultiDimensionalValues(");
    for(int i=0;i<subs.size();i++) {
      if (i > 0) {
        b.append(", ");
      }
      b.append("docBase=");
      b.append(docBases.get(i));
      b.append(" sub=" + subs.get(i));
    }
    b.append(')');
    return b.toString();
  }
}
