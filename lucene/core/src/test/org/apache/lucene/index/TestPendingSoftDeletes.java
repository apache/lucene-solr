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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

public class TestPendingSoftDeletes extends TestPendingDeletes {

  @Override
  protected PendingSoftDeletes newPendingDeletes(SegmentCommitInfo commitInfo) {
    return new PendingSoftDeletes("_soft_deletes", commitInfo);
  }

  public void testDeleteSoft() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig().setSoftDeletesField("_soft_deletes"));
    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    writer.softUpdateDocument(new Term("id", "1"), doc,
        new NumericDocValuesField("_soft_deletes", 1));
    doc = new Document();
    doc.add(new StringField("id", "2", Field.Store.YES));
    writer.softUpdateDocument(new Term("id", "2"), doc,
        new NumericDocValuesField("_soft_deletes", 1));
    doc = new Document();
    doc.add(new StringField("id", "2", Field.Store.YES));
    writer.softUpdateDocument(new Term("id", "2"), doc,
        new NumericDocValuesField("_soft_deletes", 1));
    writer.commit();
    DirectoryReader reader = writer.getReader();
    assertEquals(1, reader.leaves().size());
    SegmentReader segmentReader = (SegmentReader) reader.leaves().get(0).reader();
    SegmentCommitInfo segmentInfo = segmentReader.getSegmentInfo();
    PendingSoftDeletes pendingSoftDeletes = newPendingDeletes(segmentInfo);
    pendingSoftDeletes.onNewReader(segmentReader, segmentInfo);
    assertEquals(1, pendingSoftDeletes.numPendingDeletes());
    assertTrue(pendingSoftDeletes.getLiveDocs().get(0));
    assertFalse(pendingSoftDeletes.getLiveDocs().get(1));
    assertTrue(pendingSoftDeletes.getLiveDocs().get(2));
    // pass reader again
    Bits liveDocs = pendingSoftDeletes.getLiveDocs();
    pendingSoftDeletes.liveDocsShared();
    pendingSoftDeletes.onNewReader(segmentReader, segmentInfo);
    assertEquals(1, pendingSoftDeletes.numPendingDeletes());
    assertSame(liveDocs, pendingSoftDeletes.getLiveDocs());

    // now apply a hard delete
    writer.deleteDocuments(new Term("id", "1"));
    writer.commit();
    IOUtils.close(reader);
    reader = DirectoryReader.open(dir);
    assertEquals(1, reader.leaves().size());
    segmentReader = (SegmentReader) reader.leaves().get(0).reader();
    segmentInfo = segmentReader.getSegmentInfo();
    pendingSoftDeletes = newPendingDeletes(segmentInfo);
    pendingSoftDeletes.onNewReader(segmentReader, segmentInfo);
    assertEquals(1, pendingSoftDeletes.numPendingDeletes());
    assertFalse(pendingSoftDeletes.getLiveDocs().get(0));
    assertFalse(pendingSoftDeletes.getLiveDocs().get(1));
    assertTrue(pendingSoftDeletes.getLiveDocs().get(2));
    IOUtils.close(reader, writer, dir);
  }

  public void testApplyUpdates() throws IOException {
    RAMDirectory dir = new RAMDirectory();
    SegmentInfo si = new SegmentInfo(dir, Version.LATEST, Version.LATEST, "test", 10, false, Codec.getDefault(),
        Collections.emptyMap(), StringHelper.randomId(), new HashMap<>(), null);
    SegmentCommitInfo commitInfo = new SegmentCommitInfo(si, 0, -1, -1, -1);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
    for (int i = 0; i < si.maxDoc(); i++) {
      writer.addDocument(new Document());
    }
    writer.forceMerge(1);
    writer.commit();
    DirectoryReader reader = writer.getReader();
    assertEquals(1, reader.leaves().size());
    SegmentReader segmentReader = (SegmentReader) reader.leaves().get(0).reader();
    PendingSoftDeletes deletes = newPendingDeletes(commitInfo);
    deletes.onNewReader(segmentReader, commitInfo);
    reader.close();
    writer.close();
    FieldInfo fieldInfo = new FieldInfo("_soft_deletes", 1, false, false, false, IndexOptions.NONE, DocValuesType.NUMERIC, 0, Collections.emptyMap(), 0, 0);
    List<Integer> docsDeleted = Arrays.asList(1, 3, 7, 8, DocIdSetIterator.NO_MORE_DOCS);
    List<DocValuesFieldUpdates> updates = Arrays.asList(singleUpdate(docsDeleted, 10));
    for (DocValuesFieldUpdates update : updates) {
      deletes.onDocValuesUpdate(update.field, update.iterator());
    }
    deletes.onDocValuesUpdate(fieldInfo);
    assertEquals(4, deletes.numPendingDeletes());
    assertTrue(deletes.getLiveDocs().get(0));
    assertFalse(deletes.getLiveDocs().get(1));
    assertTrue(deletes.getLiveDocs().get(2));
    assertFalse(deletes.getLiveDocs().get(3));
    assertTrue(deletes.getLiveDocs().get(4));
    assertTrue(deletes.getLiveDocs().get(5));
    assertTrue(deletes.getLiveDocs().get(6));
    assertFalse(deletes.getLiveDocs().get(7));
    assertFalse(deletes.getLiveDocs().get(8));
    assertTrue(deletes.getLiveDocs().get(9));

    docsDeleted = Arrays.asList(1, 2, DocIdSetIterator.NO_MORE_DOCS);
    updates = Arrays.asList(singleUpdate(docsDeleted, 10));
    fieldInfo = new FieldInfo("_soft_deletes", 1, false, false, false, IndexOptions.NONE, DocValuesType.NUMERIC, 1, Collections.emptyMap(), 0, 0);
    for (DocValuesFieldUpdates update : updates) {
      deletes.onDocValuesUpdate(update.field, update.iterator());
    }
    deletes.onDocValuesUpdate(fieldInfo);
    assertEquals(5, deletes.numPendingDeletes());
    assertTrue(deletes.getLiveDocs().get(0));
    assertFalse(deletes.getLiveDocs().get(1));
    assertFalse(deletes.getLiveDocs().get(2));
    assertFalse(deletes.getLiveDocs().get(3));
    assertTrue(deletes.getLiveDocs().get(4));
    assertTrue(deletes.getLiveDocs().get(5));
    assertTrue(deletes.getLiveDocs().get(6));
    assertFalse(deletes.getLiveDocs().get(7));
    assertFalse(deletes.getLiveDocs().get(8));
    assertTrue(deletes.getLiveDocs().get(9));
  }

  public void testUpdateAppliedOnlyOnce() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig()
        .setSoftDeletesField("_soft_deletes")
        .setMaxBufferedDocs(3) // make sure we write one segment
        .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH));
    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    writer.softUpdateDocument(new Term("id", "1"), doc,
        new NumericDocValuesField("_soft_deletes", 1));
    doc = new Document();
    doc.add(new StringField("id", "2", Field.Store.YES));
    writer.softUpdateDocument(new Term("id", "2"), doc,
        new NumericDocValuesField("_soft_deletes", 1));
    doc = new Document();
    doc.add(new StringField("id", "2", Field.Store.YES));
    writer.softUpdateDocument(new Term("id", "2"), doc,
        new NumericDocValuesField("_soft_deletes", 1));
    writer.commit();
    DirectoryReader reader = writer.getReader();
    assertEquals(1, reader.leaves().size());
    SegmentReader segmentReader = (SegmentReader) reader.leaves().get(0).reader();
    SegmentCommitInfo segmentInfo = segmentReader.getSegmentInfo();
    PendingDeletes deletes = newPendingDeletes(segmentInfo);
    deletes.onNewReader(segmentReader, segmentInfo);
    FieldInfo fieldInfo = new FieldInfo("_soft_deletes", 1, false, false, false, IndexOptions.NONE, DocValuesType.NUMERIC, segmentInfo.getNextDocValuesGen(), Collections.emptyMap(), 0, 0);
    List<Integer> docsDeleted = Arrays.asList(1, DocIdSetIterator.NO_MORE_DOCS);
    List<DocValuesFieldUpdates> updates = Arrays.asList(singleUpdate(docsDeleted, 3));
    for (DocValuesFieldUpdates update : updates) {
      deletes.onDocValuesUpdate(update.field, update.iterator());
    }
    deletes.onDocValuesUpdate(fieldInfo);
    assertEquals(1, deletes.numPendingDeletes());
    assertTrue(deletes.getLiveDocs().get(0));
    assertFalse(deletes.getLiveDocs().get(1));
    assertTrue(deletes.getLiveDocs().get(2));
    deletes.liveDocsShared();
    Bits liveDocs = deletes.getLiveDocs();
    deletes.onNewReader(segmentReader, segmentInfo);
    // no changes we don't apply updates twice
    assertSame(liveDocs, deletes.getLiveDocs());
    assertTrue(deletes.getLiveDocs().get(0));
    assertFalse(deletes.getLiveDocs().get(1));
    assertTrue(deletes.getLiveDocs().get(2));
    assertEquals(1, deletes.numPendingDeletes());
    IOUtils.close(reader, writer, dir);
  }

  private DocValuesFieldUpdates singleUpdate(List<Integer> docsDeleted, int maxDoc) {
    return new DocValuesFieldUpdates(maxDoc, 0, "_soft_deletes", DocValuesType.NUMERIC) {
      @Override
      public void add(int doc, Object value) {
      }

      @Override
      public Iterator iterator() {
        return new Iterator() {
          java.util.Iterator<Integer> iter = docsDeleted.iterator();
          int doc = -1;

          @Override
          int nextDoc() {
            return doc = iter.next();
          }

          @Override
          int doc() {
            return doc;
          }

          @Override
          Object value() {
            return 1;
          }

          @Override
          long delGen() {
            return 0;
          }
        };
      }

      @Override
      public void finish() {
      }

      @Override
      public boolean any() {
        return true;
      }

      @Override
      public long ramBytesUsed() {
        return 0;
      }

      @Override
      public int size() {
        return 1;
      }
    };
  }
}
