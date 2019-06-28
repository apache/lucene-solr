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

package org.apache.lucene.luke.models.documents;

import java.io.IOException;
import java.util.Collections;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

public class DocValuesAdapterTest extends DocumentsTestBase {

  @Override
  protected void createIndex() throws IOException {
    indexDir = createTempDir("testIndex");

    Directory dir = newFSDirectory(indexDir);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, new MockAnalyzer(random()));

    Document doc = new Document();
    doc.add(new BinaryDocValuesField("dv_binary", new BytesRef("lucene")));
    doc.add(new SortedDocValuesField("dv_sorted", new BytesRef("abc")));
    doc.add(new SortedSetDocValuesField("dv_sortedset", new BytesRef("python")));
    doc.add(new SortedSetDocValuesField("dv_sortedset", new BytesRef("java")));
    doc.add(new NumericDocValuesField("dv_numeric", 42L));
    doc.add(new SortedNumericDocValuesField("dv_sortednumeric", 22L));
    doc.add(new SortedNumericDocValuesField("dv_sortednumeric", 11L));
    doc.add(newStringField("no_dv", "aaa", Field.Store.NO));
    writer.addDocument(doc);

    writer.commit();
    writer.close();
    dir.close();
  }

  @Test
  public void testGetDocValues_binary() throws Exception {
    DocValuesAdapter adapterImpl = new DocValuesAdapter(reader);
    DocValues values = adapterImpl.getDocValues(0, "dv_binary").orElseThrow(IllegalStateException::new);
    assertEquals(DocValuesType.BINARY, values.getDvType());
    assertEquals(new BytesRef("lucene"), values.getValues().get(0));
    assertEquals(Collections.emptyList(), values.getNumericValues());
  }

  @Test
  public void testGetDocValues_sorted() throws Exception {
    DocValuesAdapter adapterImpl = new DocValuesAdapter(reader);
    DocValues values = adapterImpl.getDocValues(0, "dv_sorted").orElseThrow(IllegalStateException::new);
    assertEquals(DocValuesType.SORTED, values.getDvType());
    assertEquals(new BytesRef("abc"), values.getValues().get(0));
    assertEquals(Collections.emptyList(), values.getNumericValues());
  }

  @Test
  public void testGetDocValues_sorted_set() throws Exception {
    DocValuesAdapter adapterImpl = new DocValuesAdapter(reader);
    DocValues values = adapterImpl.getDocValues(0, "dv_sortedset").orElseThrow(IllegalStateException::new);
    assertEquals(DocValuesType.SORTED_SET, values.getDvType());
    assertEquals(new BytesRef("java"), values.getValues().get(0));
    assertEquals(new BytesRef("python"), values.getValues().get(1));
    assertEquals(Collections.emptyList(), values.getNumericValues());
  }

  @Test
  public void testGetDocValues_numeric() throws Exception {
    DocValuesAdapter adapterImpl = new DocValuesAdapter(reader);
    DocValues values = adapterImpl.getDocValues(0, "dv_numeric").orElseThrow(IllegalStateException::new);
    assertEquals(DocValuesType.NUMERIC, values.getDvType());
    assertEquals(Collections.emptyList(), values.getValues());
    assertEquals(42L, values.getNumericValues().get(0).longValue());
  }

  @Test
  public void testGetDocValues_sorted_numeric() throws Exception {
    DocValuesAdapter adapterImpl = new DocValuesAdapter(reader);
    DocValues values = adapterImpl.getDocValues(0, "dv_sortednumeric").orElseThrow(IllegalStateException::new);
    assertEquals(DocValuesType.SORTED_NUMERIC, values.getDvType());
    assertEquals(Collections.emptyList(), values.getValues());
    assertEquals(11L, values.getNumericValues().get(0).longValue());
    assertEquals(22L, values.getNumericValues().get(1).longValue());
  }

  @Test
  public void testGetDocValues_notAvailable() throws Exception {
    DocValuesAdapter adapterImpl = new DocValuesAdapter(reader);
    assertFalse(adapterImpl.getDocValues(0, "no_dv").isPresent());
  }
}
