package org.apache.lucene.search;

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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;

// nocommit more

public class TestNumericRangeFilter extends LuceneTestCase {
  public void testBasicDoubleRange() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = w.newDocument();
    doc.addDouble("number", -103.0);
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();
    IndexSearcher s = newSearcher(r);
    Query q = new ConstantScoreQuery(fieldTypes.newDoubleRangeFilter("number", -110d, true, 400d, false));
    assertEquals(1, s.search(q, 1).totalHits);
    NumericDocValues ndv = MultiDocValues.getNumericValues(r, "number");
    assertEquals(-103.0, NumericUtils.longToDouble(ndv.get(0)), .0000000001);
    r.close();
    w.close();
    dir.close();
  }

  public void testBasicIntRange() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = w.newDocument();
    doc.addInt("number", -103);
    w.addDocument(doc);
    doc = w.newDocument();
    doc.addInt("number", 170);
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();
    IndexSearcher s = newSearcher(r);
    Query q = new ConstantScoreQuery(fieldTypes.newIntRangeFilter("number", -110, true, 17, false));
    assertEquals(1, s.search(q, 1).totalHits);
    NumericDocValues ndv = MultiDocValues.getNumericValues(r, "number");
    assertEquals(-103, ndv.get(0));
    r.close();
    w.close();
    dir.close();
  }

  public void testHalfFloatRange() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = w.newDocument();
    doc.addHalfFloat("number", -103.0f);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addHalfFloat("number", 17.0f);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addHalfFloat("number", 10000.0f);
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();
    IndexSearcher s = newSearcher(r);
    assertEquals(1, s.search(new ConstantScoreQuery(fieldTypes.newHalfFloatRangeFilter("number", -10f, true, 20f, false)), 1).totalHits);
    assertEquals(1, s.search(new ConstantScoreQuery(fieldTypes.newDocValuesRangeFilter("number", -10f, true, 20f, false)), 1).totalHits);

    assertEquals(2, s.search(new ConstantScoreQuery(fieldTypes.newHalfFloatRangeFilter("number", 0f, true, 20000f, false)), 1).totalHits);
    assertEquals(2, s.search(new ConstantScoreQuery(fieldTypes.newDocValuesRangeFilter("number", 0f, true, 20000f, false)), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }
}
