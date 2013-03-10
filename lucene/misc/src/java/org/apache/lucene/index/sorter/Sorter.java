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
import java.util.List;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.util.SorterTemplate;

/**
 * Sorts documents in a given index by returning a permutation on the docs.
 * Implementations can call {@link #compute(int[], List)} to compute the
 * old-to-new permutation over the given documents and values.
 * 
 * @lucene.experimental
 */
public abstract class Sorter {
  
  /** Sorts documents in reverse order. */
  public static final Sorter REVERSE_DOCS = new Sorter() {
    @Override
    public int[] oldToNew(final AtomicReader reader) throws IOException {
      final int maxDoc = reader.maxDoc();
      int[] reverseDocs = new int[maxDoc];
      for (int i = 0; i < maxDoc; i++) {
        reverseDocs[i] = maxDoc - (i + 1);
      }
      return reverseDocs;
    }
  };

  private static final class DocValueSorterTemplate<T extends Comparable<? super T>> extends SorterTemplate {
    
    private final int[] docs;
    private final List<T> values;
    
    private T pivot;
    
    public DocValueSorterTemplate(int[] docs, List<T> values) {
      this.docs = docs;
      this.values = values;
    }
    
    @Override
    protected int compare(int i, int j) {
      return values.get(docs[i]).compareTo(values.get(docs[j]));
    }
    
    @Override
    protected int comparePivot(int j) {
      return pivot.compareTo(values.get(docs[j]));
    }
    
    @Override
    protected void setPivot(int i) {
      pivot = values.get(docs[i]);
    }
    
    @Override
    protected void swap(int i, int j) {
      int tmpDoc = docs[i];
      docs[i] = docs[j];
      docs[j] = tmpDoc;
    }
  }

  /** Computes the old-to-new permutation over the given documents and values. */
  protected static <T extends Comparable<? super T>> int[] compute(int[] docs, List<T> values) {
    SorterTemplate sorter = new DocValueSorterTemplate<T>(docs, values);
    sorter.quickSort(0, docs.length - 1);
    
    final int[] oldToNew = new int[docs.length];
    for (int i = 0; i < docs.length; i++) {
      oldToNew[docs[i]] = i;
    }
    return oldToNew;
  }
  
  /**
   * Returns a mapping from the old document ID to its new location in the
   * sorted index. Implementations can use the auxiliary
   * {@link #compute(int[], List)} to compute the old-to-new permutation
   * given an array of documents and their corresponding values.
   * <p>
   * <b>NOTE:</b> deleted documents are expected to appear in the mapping as
   * well, they will however be dropped when the index is actually sorted.
   */
  public abstract int[] oldToNew(AtomicReader reader) throws IOException;
  
}
