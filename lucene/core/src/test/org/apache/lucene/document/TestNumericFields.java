package org.apache.lucene.document;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;

public class TestNumericFields extends LuceneTestCase {

  public void testSortedNumericDocValues() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setDocValuesType("sortednumeric", DocValuesType.SORTED_NUMERIC);
    fieldTypes.setMultiValued("sortednumeric");

    Document doc = w.newDocument();
    doc.addInt("sortednumeric", 3);
    doc.addInt("sortednumeric", 1);
    doc.addInt("sortednumeric", 2);
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    SortedNumericDocValues sndv = MultiDocValues.getSortedNumericValues(r, "sortednumeric");
    sndv.setDocument(0);

    assertEquals(3, sndv.count());
    assertEquals(1, sndv.valueAt(0));
    assertEquals(2, sndv.valueAt(1));
    assertEquals(3, sndv.valueAt(2));
    w.close();
    r.close();
    dir.close();
  }

  // Cannot change a field from INT to DOUBLE
  public void testInvalidNumberTypeChange() throws Exception {
    IndexWriter w = newIndexWriter();
    Document doc = w.newDocument();
    doc.addInt("int", 3);
    shouldFail(() -> doc.addDouble("int", 4d),
               "field \"int\": cannot change from value type INT to DOUBLE");
    w.close();
  }
}
