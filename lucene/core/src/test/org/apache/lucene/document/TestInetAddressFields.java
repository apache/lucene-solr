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

import java.net.InetAddress;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestInetAddressFields extends LuceneTestCase {

  public void testInetAddressSort() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    Document doc = w.newDocument();
    InetAddress inet0 = InetAddress.getByName("10.17.4.10");
    doc.addInetAddress("inet", inet0);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    InetAddress inet1 = InetAddress.getByName("10.17.4.22");
    doc.addInetAddress("inet", inet1);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 10, fieldTypes.newSort("inet"));
    assertEquals(2, hits.totalHits);
    Document hit = s.doc(hits.scoreDocs[0].doc);
    assertEquals("0", hit.getString("id"));
    assertEquals(inet0, hit.getInetAddress("inet"));
    hit = s.doc(hits.scoreDocs[1].doc);
    assertEquals("1", hit.getString("id"));
    assertEquals(inet1, hit.getInetAddress("inet"));
    r.close();
    w.close();
    dir.close();
  }

  public void testInetAddressRangeFilter() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    Document doc = w.newDocument();
    InetAddress inet0 = InetAddress.getByName("10.17.4.10");
    doc.addInetAddress("inet", inet0);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    InetAddress inet1 = InetAddress.getByName("10.17.4.22");
    doc.addInetAddress("inet", inet1);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    assertEquals(2, s.search(new MatchAllDocsQuery(), fieldTypes.newRangeFilter("inet", inet0, true, inet1, true), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newRangeFilter("inet", inet0, true, inet1, false), 1).totalHits);
    assertEquals(0, s.search(new MatchAllDocsQuery(), fieldTypes.newRangeFilter("inet", inet0, false, inet1, false), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newRangeFilter("inet", InetAddress.getByName("10.17.0.0"), true, InetAddress.getByName("10.17.4.20"), false), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testInetV6AddressRangeFilter() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    Document doc = w.newDocument();
    InetAddress inet0 = InetAddress.getByName("1080:0:0:0:8:700:200C:417A");
    doc.addInetAddress("inet", inet0);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    InetAddress inet1 = InetAddress.getByName("1080:0:0:0:8:800:200C:417A");
    doc.addInetAddress("inet", inet1);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    assertEquals(2, s.search(new MatchAllDocsQuery(), fieldTypes.newRangeFilter("inet", inet0, true, inet1, true), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newRangeFilter("inet", inet0, true, inet1, false), 1).totalHits);
    assertEquals(0, s.search(new MatchAllDocsQuery(), fieldTypes.newRangeFilter("inet", inet0, false, inet1, false), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newRangeFilter("inet", InetAddress.getByName("1080:0:0:0:0:0:0:0"), true, InetAddress.getByName("1080:0:0:0:8:750:0:0"), false), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testMixed() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    Document doc = w.newDocument();
    InetAddress inet0 = InetAddress.getByName("10.17.5.22");
    doc.addInetAddress("inet", inet0);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    InetAddress inet1 = InetAddress.getByName("1080:0:0:0:8:800:200C:417A");
    doc.addInetAddress("inet", inet1);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    assertEquals(2, s.search(new MatchAllDocsQuery(), fieldTypes.newRangeFilter("inet", inet0, true, inet1, true), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newRangeFilter("inet", inet0, true, inet1, false), 1).totalHits);
    assertEquals(0, s.search(new MatchAllDocsQuery(), fieldTypes.newRangeFilter("inet", inet0, false, inet1, false), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newRangeFilter("inet", InetAddress.getByName("10.17.5.0"), true, InetAddress.getByName("1080:0:0:0:8:750:0:0"), false), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testExcIndexedThenStored() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addInetAddress("num", InetAddress.getByName("10.17.5.22"));
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addStoredInetAddress("num", InetAddress.getByName("10.17.5.22")),
               "field \"num\": cannot addStored: field was already added non-stored");
    w.close();
  }

  public void testExcStoredThenIndexed() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addStoredInetAddress("num", InetAddress.getByName("10.17.5.22"));
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addInetAddress("num", InetAddress.getByName("10.17.5.22")),
               "field \"num\": this field is only stored; use addStoredXXX instead");
    w.close();
  }
}
