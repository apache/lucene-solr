package org.apache.lucene.index;
/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.store.Directory;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.util.Random;


public class TestIndexWriterMerging extends LuceneTestCase
{

  /**
   * Tests that index merging (specifically addIndexes(Directory...)) doesn't
   * change the index order of documents.
   */
  public void testLucene() throws IOException {
    int num=100;

    Directory indexA = newDirectory();
    Directory indexB = newDirectory();

    fillIndex(random, indexA, 0, num);
    boolean fail = verifyIndex(indexA, 0);
    if (fail)
    {
      fail("Index a is invalid");
    }

    fillIndex(random, indexB, num, num);
    fail = verifyIndex(indexB, num);
    if (fail)
    {
      fail("Index b is invalid");
    }

    Directory merged = newDirectory();

    IndexWriter writer = new IndexWriter(
        merged,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
            setMergePolicy(newLogMergePolicy(2))
    );
    writer.setInfoStream(VERBOSE ? System.out : null);
    writer.addIndexes(indexA, indexB);
    writer.optimize();
    writer.close();

    fail = verifyIndex(merged, 0);

    assertFalse("The merged index is invalid", fail);
    indexA.close();
    indexB.close();
    merged.close();
  }

  private boolean verifyIndex(Directory directory, int startAt) throws IOException
  {
    boolean fail = false;
    IndexReader reader = IndexReader.open(directory, true);

    int max = reader.maxDoc();
    for (int i = 0; i < max; i++)
    {
      Document temp = reader.document(i);
      //System.out.println("doc "+i+"="+temp.getField("count").stringValue());
      //compare the index doc number to the value that it should be
      if (!temp.getField("count").stringValue().equals((i + startAt) + ""))
      {
        fail = true;
        System.out.println("Document " + (i + startAt) + " is returning document " + temp.getField("count").stringValue());
      }
    }
    reader.close();
    return fail;
  }

  private void fillIndex(Random random, Directory dir, int start, int numDocs) throws IOException {

    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
            setOpenMode(OpenMode.CREATE).
            setMaxBufferedDocs(2).
            setMergePolicy(newLogMergePolicy(2))
    );

    for (int i = start; i < (start + numDocs); i++)
    {
      Document temp = new Document();
      temp.add(newField("count", (""+i), Field.Store.YES, Field.Index.NOT_ANALYZED));

      writer.addDocument(temp);
    }
    writer.close();
  }
  
  // LUCENE-325: test expungeDeletes, when 2 singular merges
  // are required
  public void testExpungeDeletes() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random))
        .setMaxBufferedDocs(2).setRAMBufferSizeMB(
                                                  IndexWriterConfig.DISABLE_AUTO_FLUSH));
    writer.setInfoStream(VERBOSE ? System.out : null);
    Document document = new Document();

    document = new Document();
    Field storedField = newField("stored", "stored", Field.Store.YES,
                                  Field.Index.NO);
    document.add(storedField);
    Field termVectorField = newField("termVector", "termVector",
                                      Field.Store.NO, Field.Index.NOT_ANALYZED,
                                      Field.TermVector.WITH_POSITIONS_OFFSETS);
    document.add(termVectorField);
    for(int i=0;i<10;i++)
      writer.addDocument(document);
    writer.close();

    IndexReader ir = IndexReader.open(dir, false);
    assertEquals(10, ir.maxDoc());
    assertEquals(10, ir.numDocs());
    ir.deleteDocument(0);
    ir.deleteDocument(7);
    assertEquals(8, ir.numDocs());
    ir.close();

    writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));
    assertEquals(8, writer.numDocs());
    assertEquals(10, writer.maxDoc());
    writer.expungeDeletes();
    assertEquals(8, writer.numDocs());
    writer.close();
    ir = IndexReader.open(dir, true);
    assertEquals(8, ir.maxDoc());
    assertEquals(8, ir.numDocs());
    ir.close();
    dir.close();
  }

  // LUCENE-325: test expungeDeletes, when many adjacent merges are required
  public void testExpungeDeletes2() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
            setMaxBufferedDocs(2).
            setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH).
            setMergePolicy(newLogMergePolicy(50))
    );

    Document document = new Document();

    document = new Document();
    Field storedField = newField("stored", "stored", Store.YES,
                                  Index.NO);
    document.add(storedField);
    Field termVectorField = newField("termVector", "termVector",
                                      Store.NO, Index.NOT_ANALYZED,
                                      TermVector.WITH_POSITIONS_OFFSETS);
    document.add(termVectorField);
    for(int i=0;i<98;i++)
      writer.addDocument(document);
    writer.close();

    IndexReader ir = IndexReader.open(dir, false);
    assertEquals(98, ir.maxDoc());
    assertEquals(98, ir.numDocs());
    for(int i=0;i<98;i+=2)
      ir.deleteDocument(i);
    assertEquals(49, ir.numDocs());
    ir.close();

    writer = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
            setMergePolicy(newLogMergePolicy(3))
    );
    assertEquals(49, writer.numDocs());
    writer.expungeDeletes();
    writer.close();
    ir = IndexReader.open(dir, true);
    assertEquals(49, ir.maxDoc());
    assertEquals(49, ir.numDocs());
    ir.close();
    dir.close();
  }

  // LUCENE-325: test expungeDeletes without waiting, when
  // many adjacent merges are required
  public void testExpungeDeletes3() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
            setMaxBufferedDocs(2).
            setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH).
            setMergePolicy(newLogMergePolicy(50))
    );

    writer.setInfoStream(VERBOSE ? System.out : null);

    Document document = new Document();

    document = new Document();
    Field storedField = newField("stored", "stored", Field.Store.YES,
                                  Field.Index.NO);
    document.add(storedField);
    Field termVectorField = newField("termVector", "termVector",
                                      Field.Store.NO, Field.Index.NOT_ANALYZED,
                                      Field.TermVector.WITH_POSITIONS_OFFSETS);
    document.add(termVectorField);
    for(int i=0;i<98;i++)
      writer.addDocument(document);
    writer.close();

    IndexReader ir = IndexReader.open(dir, false);
    assertEquals(98, ir.maxDoc());
    assertEquals(98, ir.numDocs());
    for(int i=0;i<98;i+=2)
      ir.deleteDocument(i);
    assertEquals(49, ir.numDocs());
    ir.close();

    writer = new IndexWriter(
        dir,
        newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).
            setMergePolicy(newLogMergePolicy(3))
    );
    writer.expungeDeletes(false);
    writer.close();
    ir = IndexReader.open(dir, true);
    assertEquals(49, ir.maxDoc());
    assertEquals(49, ir.numDocs());
    ir.close();
    dir.close();
  }
  
  // Just intercepts all merges & verifies that we are never
  // merging a segment with >= 20 (maxMergeDocs) docs
  private class MyMergeScheduler extends MergeScheduler {
    @Override
    synchronized public void merge(IndexWriter writer)
      throws CorruptIndexException, IOException {

      while(true) {
        MergePolicy.OneMerge merge = writer.getNextMerge();
        if (merge == null) {
          break;
        }
        for(int i=0;i<merge.segments.size();i++) {
          assert merge.segments.get(i).docCount < 20;
        }
        writer.merge(merge);
      }
    }

    @Override
    public void close() {}
  }

  // LUCENE-1013
  public void testSetMaxMergeDocs() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random))
      .setMergeScheduler(new MyMergeScheduler()).setMaxBufferedDocs(2).setMergePolicy(newLogMergePolicy());
    LogMergePolicy lmp = (LogMergePolicy) conf.getMergePolicy();
    lmp.setMaxMergeDocs(20);
    lmp.setMergeFactor(2);
    IndexWriter iw = new IndexWriter(dir, conf);
    iw.setInfoStream(VERBOSE ? System.out : null);
    Document document = new Document();
    document.add(newField("tvtest", "a b c", Field.Store.NO, Field.Index.ANALYZED,
                           Field.TermVector.YES));
    for(int i=0;i<177;i++)
      iw.addDocument(document);
    iw.close();
    dir.close();
  }
}
