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
import java.lang.reflect.Field;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.lucene49.Lucene49DocValuesFormat;
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

  private static final RamUsageTester.Filter ORDINAL_MAP_FILTER = new RamUsageTester.Filter() {
    @Override
    public boolean accept(Field field) {
      if (field.getDeclaringClass().equals(OrdinalMap.class) && field.getName().equals("owner")) {
        return false;
      }
      return true;
    }

    public boolean accept(Object o) {
      return o != LongValues.IDENTITY;
    }
  };

  public void testRamBytesUsed() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig cfg = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setCodec(TestUtil.alwaysDocValuesFormat(new Lucene49DocValuesFormat()));
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
    AtomicReader ar = SlowCompositeReaderWrapper.wrap(r);
    SortedDocValues sdv = ar.getSortedDocValues("sdv");
    if (sdv instanceof MultiSortedDocValues) {
      OrdinalMap map = ((MultiSortedDocValues) sdv).mapping;
      assertEquals(RamUsageTester.sizeOf(map, ORDINAL_MAP_FILTER), map.ramBytesUsed());
    }
    SortedSetDocValues ssdv = ar.getSortedSetDocValues("ssdv");
    if (ssdv instanceof MultiSortedSetDocValues) {
      OrdinalMap map = ((MultiSortedSetDocValues) ssdv).mapping;
      assertEquals(RamUsageTester.sizeOf(map, ORDINAL_MAP_FILTER), map.ramBytesUsed());
    }
    iw.close();
    r.close();
    dir.close();
  }

}
