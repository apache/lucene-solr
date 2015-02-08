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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestFieldTypes extends LuceneTestCase {

  public void testExcAddAll() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addInt("field", 17);
    w.addDocument(doc);
    w.close();

    Directory dir2 = newDirectory();
    w = newIndexWriter(dir2);
    doc = w.newDocument();
    doc.addAtom("field", "foo");
    doc.addAtom("a", "foo");
    doc.addAtom("z", "foo");
    w.addDocument(doc);
    w.close();

    FieldTypes ft1 = FieldTypes.getFieldTypes(dir, null);
    FieldTypes ft2 = FieldTypes.getFieldTypes(dir2, null);
    try {
      ft1.addAll(ft2);
      fail("did not hit exception");
    } catch (IllegalStateException ise) {
      // expected
      assertEquals("field \"field\": cannot change value type from INT to ATOM",
                   ise.getMessage());
    }

    ft1.getFieldType("field");
    try {
      ft1.getFieldType("a");
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    try {
      ft1.getFieldType("z");
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    dir2.close();
  }

  public void testInconsistentMultiReaderFieldTypes() throws Exception {
    IndexWriter w = newIndexWriter();
    Document doc = w.newDocument();
    doc.addInt("field", 5);
    w.addDocument(doc);
    w.close();
    IndexReader sub1 = DirectoryReader.open(dir);

    w = newIndexWriter(newIndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE));
    doc = w.newDocument();
    doc.addShortText("field", "hello");
    w.addDocument(doc);
    w.close();
    IndexReader sub2 = DirectoryReader.open(dir);

    try {
      new MultiReader(sub1, sub2);
      fail("did not hit exception");
    } catch (IllegalStateException ise) {
      assertEquals("field \"field\": cannot change value type from INT to SHORT_TEXT", ise.getMessage());
    }

    sub1.close();
    sub2.close();
  }

  public void testInconsistentAddIndexesFieldTypes() throws Exception {
    IndexWriter w = newIndexWriter();
    Document doc = w.newDocument();
    doc.addInt("field", 5);
    w.addDocument(doc);
    w.close();
    DirectoryReader sub = DirectoryReader.open(dir);

    w = newIndexWriter(newIndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE));
    doc = w.newDocument();
    doc.addShortText("field", "hello");
    w.addDocument(doc);

    try {
      TestUtil.addIndexesSlowly(w, sub);
      fail("did not hit exception");
    } catch (IllegalStateException ise) {
      assertEquals("field \"field\": cannot change value type from SHORT_TEXT to INT", ise.getMessage());
    }

    w.close();
    sub.close();
  }
}
