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
package org.apache.lucene.index;

import java.util.Arrays;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.TestUtil;
import org.junit.BeforeClass;

public class SortingLeafReaderTest extends SorterTestBase {
  
  @BeforeClass
  public static void beforeClassSortingLeafReaderTest() throws Exception {
    // NOTE: index was created by by super's @BeforeClass
    
    // sort the index by id (as integer, in NUMERIC_DV_FIELD)
    Sort sort = new Sort(new SortField(NUMERIC_DV_FIELD, SortField.Type.INT));
    final Sorter.DocMap docMap = new Sorter(sort).sort(unsortedReader);
 
    // Sorter.compute also sorts the values
    NumericDocValues dv = unsortedReader.getNumericDocValues(NUMERIC_DV_FIELD);
    sortedValues = new Integer[unsortedReader.maxDoc()];
    for (int i = 0; i < unsortedReader.maxDoc(); ++i) {
      sortedValues[docMap.oldToNew(i)] = (int)dv.get(i);
    }
    if (VERBOSE) {
      System.out.println("docMap: " + docMap);
      System.out.println("sortedValues: " + Arrays.toString(sortedValues));
    }
    
    // sort the index by id (as integer, in NUMERIC_DV_FIELD)
    sortedReader = SortingLeafReader.wrap(unsortedReader, sort);
    
    if (VERBOSE) {
      System.out.print("mapped-deleted-docs: ");
      Bits mappedLiveDocs = sortedReader.getLiveDocs();
      for (int i = 0; i < mappedLiveDocs.length(); i++) {
        if (!mappedLiveDocs.get(i)) {
          System.out.print(i + " ");
        }
      }
      System.out.println();
    }
    
    TestUtil.checkReader(sortedReader);
  }
  
  public void testBadSort() throws Exception {
    try {
      SortingLeafReader.wrap(sortedReader, Sort.RELEVANCE);
      fail("Didn't get expected exception");
    } catch (IllegalArgumentException e) {
      assertEquals("Cannot sort an index with a Sort that refers to the relevance score", e.getMessage());
    }
  }

}
