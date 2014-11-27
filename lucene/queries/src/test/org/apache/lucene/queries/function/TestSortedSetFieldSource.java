package org.apache.lucene.queries.function;

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

import java.util.Collections;

import org.apache.lucene.document.Document2;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.queries.function.valuesource.SortedSetFieldSource;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestSortedSetFieldSource extends LuceneTestCase {
  public void testSimple() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(null));
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.setMultiValued("value");
    Document2 doc = writer.newDocument();
    doc.addBinary("value", new BytesRef("baz"));
    doc.addAtom("id", "2");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addBinary("value", new BytesRef("foo"));
    doc.addBinary("value", new BytesRef("bar"));
    doc.addAtom("id", "1");
    writer.addDocument(doc);
    writer.forceMerge(1);
    writer.close();

    DirectoryReader ir = DirectoryReader.open(dir);
    LeafReader ar = getOnlySegmentReader(ir);
    
    ValueSource vs = new SortedSetFieldSource("value");
    FunctionValues values = vs.getValues(Collections.emptyMap(), ar.getContext());
    assertEquals("baz", values.strVal(0));
    assertEquals("bar", values.strVal(1)); 
    ir.close();
    dir.close();
  }
}
