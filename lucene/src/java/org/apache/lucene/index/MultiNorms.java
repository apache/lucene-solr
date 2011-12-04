package org.apache.lucene.index;

/**
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
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.util.ReaderUtil;

/**
 * Exposes flex API, merged from flex API of sub-segments.
 * This is useful when you're interacting with an {@link
 * IndexReader} implementation that consists of sequential
 * sub-readers (eg DirectoryReader or {@link
 * MultiReader}).
 *
 * <p><b>NOTE</b>: for multi readers, you'll get better
 * performance by gathering the sub readers using {@link
 * ReaderUtil#gatherSubReaders} and then operate per-reader,
 * instead of using this class.
 *
 * @lucene.experimental
 */
public class MultiNorms {
  // no need to instantiate this
  private MultiNorms() { }
  
  /**
   * Warning: this is heavy! Do not use in a loop, or implement norms()
   * in your own reader with this (you should likely cache the result).
   */
  public static byte[] norms(IndexReader r, String field) throws IOException {
    final IndexReader[] subs = r.getSequentialSubReaders();
    if (subs == null) {
      // already an atomic reader
      return r.norms(field);
    } else if (subs.length == 0 || !r.hasNorms(field)) {
      // no norms
      return null;
    } else if (subs.length == 1) {
      return norms(subs[0], field);
    } else {
      // TODO: optimize more maybe
      byte norms[] = new byte[r.maxDoc()];
      final List<IndexReader> leaves = new ArrayList<IndexReader>();
      ReaderUtil.gatherSubReaders(leaves, r);
      int end = 0;
      for (IndexReader leaf : leaves) {
        Fields fields = leaf.fields();
        boolean hasField = (fields != null && fields.terms(field) != null);
        
        int start = end;
        byte leafNorms[] = leaf.norms(field);
        if (leafNorms == null) {
          if (hasField) { // omitted norms
            return null;
          }
          // doesn't have field, fill bytes
          leafNorms = new byte[leaf.maxDoc()];
          Arrays.fill(leafNorms, (byte) 0);
        }
        
        System.arraycopy(leafNorms, 0, norms, start, leafNorms.length);
        end += leaf.maxDoc();
      }
      return norms;
    }
  }
}
