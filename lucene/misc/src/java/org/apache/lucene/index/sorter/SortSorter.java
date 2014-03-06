package org.apache.lucene.index.sorter;

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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

// nocommit: temporary class to engage the cutover!
class SortSorter extends Sorter {
  final Sort sort;
  
  public SortSorter(Sort sort) {
    this.sort = sort;
  }

  @Override
  public DocMap sort(AtomicReader reader) throws IOException {
    SortField fields[] = sort.getSort();
    final int reverseMul[] = new int[fields.length];
    final FieldComparator<?> comparators[] = new FieldComparator[fields.length];
    
    for (int i = 0; i < fields.length; i++) {
      reverseMul[i] = fields[i].getReverse() ? -1 : 1;
      comparators[i] = fields[i].getComparator(1, i);
      comparators[i].setNextReader(reader.getContext());
      comparators[i].setScorer(FAKESCORER);
    }
    final DocComparator comparator = new DocComparator() {
      @Override
      public int compare(int docID1, int docID2) {
        try {
          for (int i = 0; i < comparators.length; i++) {
            // TODO: would be better if copy() didnt cause a term lookup in TermOrdVal & co,
            // the segments are always the same here...
            comparators[i].copy(0, docID1);
            comparators[i].setBottom(0);
            int comp = reverseMul[i] * comparators[i].compareBottom(docID2);
            if (comp != 0) {
              return comp;
            }
          }
          return Integer.compare(docID1, docID2); // docid order tiebreak
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
    return sort(reader.maxDoc(), comparator);
  }

  @Override
  public String getID() {
    return sort.toString();
  }
  
  static final Scorer FAKESCORER = new Scorer(null) {
    
    @Override
    public float score() throws IOException { throw new UnsupportedOperationException(); }
    
    @Override
    public int freq() throws IOException { throw new UnsupportedOperationException(); }

    @Override
    public int docID() { throw new UnsupportedOperationException(); }

    @Override
    public int nextDoc() throws IOException { throw new UnsupportedOperationException(); }

    @Override
    public int advance(int target) throws IOException { throw new UnsupportedOperationException(); }

    @Override
    public long cost() { throw new UnsupportedOperationException(); }
  };
}
