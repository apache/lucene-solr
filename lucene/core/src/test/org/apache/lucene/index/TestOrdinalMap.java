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


import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.MultiDocValues.MultiSortedDocValues;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.MultiDocValues.OrdinalMap;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.RamUsageTester;
import org.apache.lucene.util.TestUtil;

public class TestOrdinalMap extends LuceneTestCase {

  private static final Field ORDINAL_MAP_OWNER_FIELD;
  static {
    try {
      ORDINAL_MAP_OWNER_FIELD = OrdinalMap.class.getDeclaredField("owner");
    } catch (Exception e) {
      throw new Error();
    }
  }

  private static final RamUsageTester.Accumulator ORDINAL_MAP_ACCUMULATOR = new RamUsageTester.Accumulator() {

    public long accumulateObject(Object o, long shallowSize, java.util.Map<Field,Object> fieldValues, java.util.Collection<Object> queue) {
      if (o == LongValues.IDENTITY) {
        return 0L;
      }
      if (o instanceof OrdinalMap) {
        fieldValues = new HashMap<>(fieldValues);
        fieldValues.remove(ORDINAL_MAP_OWNER_FIELD);
      }
      return super.accumulateObject(o, shallowSize, fieldValues, queue);
    }

  };

  public void testRamBytesUsed() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig cfg = new IndexWriterConfig(new MockAnalyzer(random())).setCodec(TestUtil.alwaysDocValuesFormat(TestUtil.getDefaultDocValuesFormat()));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, cfg);
    final int maxDoc = TestUtil.nextInt(random(), 10, 1000);
    final int maxTermLength = TestUtil.nextInt(random(), 1, 4);
    for (int i = 0; i < maxDoc; ++i) {
      Document d = new Document();
      if (random().nextBoolean()) {
        d.add(new SortedDocValuesField("sdv", new BytesRef(TestUtil.randomSimpleString(random(), maxTermLength))));
      }
      final int numSortedSet = random().nextInt(3);
      for (int j = 0; j < numSortedSet; ++j) {
        d.add(new SortedSetDocValuesField("ssdv", new BytesRef(TestUtil.randomSimpleString(random(), maxTermLength))));
      }
      iw.addDocument(d);
      if (rarely()) {
        iw.getReader().close();
      }
    }
    iw.commit();
    DirectoryReader r = iw.getReader();
    LeafReader ar = SlowCompositeReaderWrapper.wrap(r);
    SortedDocValues sdv = ar.getSortedDocValues("sdv");
    if (sdv instanceof MultiSortedDocValues) {
      OrdinalMap map = ((MultiSortedDocValues) sdv).mapping;
      assertEquals(RamUsageTester.sizeOf(map, ORDINAL_MAP_ACCUMULATOR), map.ramBytesUsed());
    }
    SortedSetDocValues ssdv = ar.getSortedSetDocValues("ssdv");
    if (ssdv instanceof MultiSortedSetDocValues) {
      OrdinalMap map = ((MultiSortedSetDocValues) ssdv).mapping;
      assertEquals(RamUsageTester.sizeOf(map, ORDINAL_MAP_ACCUMULATOR), map.ramBytesUsed());
    }
    iw.close();
    r.close();
    dir.close();
  }

}
