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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.packed.PackedInts;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

/** Extends {@link BaseDocValuesFormatTestCase} to add compression checks. */
public abstract class BaseCompressingDocValuesFormatTestCase extends BaseDocValuesFormatTestCase {

  static long dirSize(Directory d) throws IOException {
    long size = 0;
    for (String file : d.listAll()) {
      size += d.fileLength(file);
    }
    return size;
  }

  public void testUniqueValuesCompression() throws IOException {
    final Directory dir = new RAMDirectory();
    final IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    final IndexWriter iwriter = new IndexWriter(dir, iwc);

    final int uniqueValueCount = TestUtil.nextInt(random(), 1, 256);
    final List<Long> values = new ArrayList<>();

    final Document doc = new Document();
    final NumericDocValuesField dvf = new NumericDocValuesField("dv", 0);
    doc.add(dvf);
    for (int i = 0; i < 300; ++i) {
      final long value;
      if (values.size() < uniqueValueCount) {
        value = random().nextLong();
        values.add(value);
      } else {
        value = RandomPicks.randomFrom(random(), values);
      }
      dvf.setLongValue(value);
      iwriter.addDocument(doc);
    }
    iwriter.forceMerge(1);
    final long size1 = dirSize(dir);
    for (int i = 0; i < 20; ++i) {
      dvf.setLongValue(RandomPicks.randomFrom(random(), values));
      iwriter.addDocument(doc);
    }
    iwriter.forceMerge(1);
    final long size2 = dirSize(dir);
    // make sure the new longs did not cost 8 bytes each
    assertTrue(size2 < size1 + 8 * 20);
  }

  public void testDateCompression() throws IOException {
    final Directory dir = new RAMDirectory();
    final IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    final IndexWriter iwriter = new IndexWriter(dir, iwc);

    final long base = 13; // prime
    final long day = 1000L * 60 * 60 * 24;

    final Document doc = new Document();
    final NumericDocValuesField dvf = new NumericDocValuesField("dv", 0);
    doc.add(dvf);
    for (int i = 0; i < 300; ++i) {
      dvf.setLongValue(base + random().nextInt(1000) * day);
      iwriter.addDocument(doc);
    }
    iwriter.forceMerge(1);
    final long size1 = dirSize(dir);
    for (int i = 0; i < 50; ++i) {
      dvf.setLongValue(base + random().nextInt(1000) * day);
      iwriter.addDocument(doc);
    }
    iwriter.forceMerge(1);
    final long size2 = dirSize(dir);
    // make sure the new longs costed less than if they had only been packed
    assertTrue(size2 < size1 + (PackedInts.bitsRequired(day) * 50) / 8);
  }

  public void testSingleBigValueCompression() throws IOException {
    final Directory dir = new RAMDirectory();
    final IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    final IndexWriter iwriter = new IndexWriter(dir, iwc);

    final Document doc = new Document();
    final NumericDocValuesField dvf = new NumericDocValuesField("dv", 0);
    doc.add(dvf);
    for (int i = 0; i < 20000; ++i) {
      dvf.setLongValue(i & 1023);
      iwriter.addDocument(doc);
    }
    iwriter.forceMerge(1);
    final long size1 = dirSize(dir);
    dvf.setLongValue(Long.MAX_VALUE);
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    final long size2 = dirSize(dir);
    // make sure the new value did not grow the bpv for every other value
    assertTrue(size2 < size1 + (20000 * (63 - 10)) / 8);
  }

}
