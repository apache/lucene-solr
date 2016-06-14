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
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestIndexWriterMerging extends LuceneTestCase {

  /**
   * Tests that index merging (specifically addIndexes(Directory...)) doesn't
   * change the index order of documents.
   */
  public void testLucene() throws IOException {
    int num=100;

    Directory indexA = newDirectory();
    Directory indexB = newDirectory();

    fillIndex(random(), indexA, 0, num);
    boolean fail = verifyIndex(indexA, 0);
    if (fail)
    {
      fail("Index a is invalid");
    }

    fillIndex(random(), indexB, num, num);
    fail = verifyIndex(indexB, num);
    if (fail)
    {
      fail("Index b is invalid");
    }

    Directory merged = newDirectory();

    IndexWriter writer = new IndexWriter(
        merged,
        newIndexWriterConfig(new MockAnalyzer(random()))
          .setMergePolicy(newLogMergePolicy(2))
    );
    writer.addIndexes(indexA, indexB);
    writer.forceMerge(1);
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
    IndexReader reader = DirectoryReader.open(directory);

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
        newIndexWriterConfig(new MockAnalyzer(random))
            .setOpenMode(OpenMode.CREATE)
            .setMaxBufferedDocs(2)
            .setMergePolicy(newLogMergePolicy(2))
    );

    for (int i = start; i < (start + numDocs); i++)
    {
      Document temp = new Document();
      temp.add(newStringField("count", (""+i), Field.Store.YES));

      writer.addDocument(temp);
    }
    writer.close();
  }
  
  // LUCENE-325: test forceMergeDeletes, when 2 singular merges
  // are required
  public void testForceMergeDeletes() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                .setMaxBufferedDocs(2)
                                                .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH));
    Document document = new Document();

    FieldType customType = new FieldType();
    customType.setStored(true);

    FieldType customType1 = new FieldType(TextField.TYPE_STORED);
    customType1.setTokenized(false);
    customType1.setStoreTermVectors(true);
    customType1.setStoreTermVectorPositions(true);
    customType1.setStoreTermVectorOffsets(true);
    
    Field idField = newStringField("id", "", Field.Store.NO);
    document.add(idField);
    Field storedField = newField("stored", "stored", customType);
    document.add(storedField);
    Field termVectorField = newField("termVector", "termVector", customType1);
    document.add(termVectorField);
    for(int i=0;i<10;i++) {
      idField.setStringValue("" + i);
      writer.addDocument(document);
    }
    writer.close();

    IndexReader ir = DirectoryReader.open(dir);
    assertEquals(10, ir.maxDoc());
    assertEquals(10, ir.numDocs());
    ir.close();

    IndexWriterConfig dontMergeConfig = new IndexWriterConfig(new MockAnalyzer(random()))
      .setMergePolicy(NoMergePolicy.INSTANCE);
    writer = new IndexWriter(dir, dontMergeConfig);
    writer.deleteDocuments(new Term("id", "0"));
    writer.deleteDocuments(new Term("id", "7"));
    writer.close();
    
    ir = DirectoryReader.open(dir);
    assertEquals(8, ir.numDocs());
    ir.close();

    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                    .setMergePolicy(newLogMergePolicy()));
    assertEquals(8, writer.numDocs());
    assertEquals(10, writer.maxDoc());
    writer.forceMergeDeletes();
    assertEquals(8, writer.numDocs());
    writer.close();
    ir = DirectoryReader.open(dir);
    assertEquals(8, ir.maxDoc());
    assertEquals(8, ir.numDocs());
    ir.close();
    dir.close();
  }

  // LUCENE-325: test forceMergeDeletes, when many adjacent merges are required
  public void testForceMergeDeletes2() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(new MockAnalyzer(random()))
          .setMaxBufferedDocs(2)
          .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
          .setMergePolicy(newLogMergePolicy(50))
    );

    Document document = new Document();

    FieldType customType = new FieldType();
    customType.setStored(true);

    FieldType customType1 = new FieldType(TextField.TYPE_NOT_STORED);
    customType1.setTokenized(false);
    customType1.setStoreTermVectors(true);
    customType1.setStoreTermVectorPositions(true);
    customType1.setStoreTermVectorOffsets(true);
    
    Field storedField = newField("stored", "stored", customType);
    document.add(storedField);
    Field termVectorField = newField("termVector", "termVector", customType1);
    document.add(termVectorField);
    Field idField = newStringField("id", "", Field.Store.NO);
    document.add(idField);
    for(int i=0;i<98;i++) {
      idField.setStringValue("" + i);
      writer.addDocument(document);
    }
    writer.close();

    IndexReader ir = DirectoryReader.open(dir);
    assertEquals(98, ir.maxDoc());
    assertEquals(98, ir.numDocs());
    ir.close();
    
    IndexWriterConfig dontMergeConfig = new IndexWriterConfig(new MockAnalyzer(random()))
      .setMergePolicy(NoMergePolicy.INSTANCE);
    writer = new IndexWriter(dir, dontMergeConfig);
    for(int i=0;i<98;i+=2) {
      writer.deleteDocuments(new Term("id", "" + i));
    }
    writer.close();
    
    ir = DirectoryReader.open(dir);
    assertEquals(49, ir.numDocs());
    ir.close();

    writer = new IndexWriter(
        dir,
        newIndexWriterConfig(new MockAnalyzer(random()))
          .setMergePolicy(newLogMergePolicy(3))
    );
    assertEquals(49, writer.numDocs());
    writer.forceMergeDeletes();
    writer.close();
    ir = DirectoryReader.open(dir);
    assertEquals(49, ir.maxDoc());
    assertEquals(49, ir.numDocs());
    ir.close();
    dir.close();
  }

  // LUCENE-325: test forceMergeDeletes without waiting, when
  // many adjacent merges are required
  public void testForceMergeDeletes3() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(new MockAnalyzer(random()))
            .setMaxBufferedDocs(2)
            .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            .setMergePolicy(newLogMergePolicy(50))
    );

    FieldType customType = new FieldType();
    customType.setStored(true);

    FieldType customType1 = new FieldType(TextField.TYPE_NOT_STORED);
    customType1.setTokenized(false);
    customType1.setStoreTermVectors(true);
    customType1.setStoreTermVectorPositions(true);
    customType1.setStoreTermVectorOffsets(true);
    
    Document document = new Document();
    Field storedField = newField("stored", "stored", customType);
    document.add(storedField);
    Field termVectorField = newField("termVector", "termVector", customType1);
    document.add(termVectorField);
    Field idField = newStringField("id", "", Field.Store.NO);
    document.add(idField);
    for(int i=0;i<98;i++) {
      idField.setStringValue("" + i);
      writer.addDocument(document);
    }
    writer.close();

    IndexReader ir = DirectoryReader.open(dir);
    assertEquals(98, ir.maxDoc());
    assertEquals(98, ir.numDocs());
    ir.close();
    
    IndexWriterConfig dontMergeConfig = new IndexWriterConfig(new MockAnalyzer(random()))
      .setMergePolicy(NoMergePolicy.INSTANCE);
    writer = new IndexWriter(dir, dontMergeConfig);
    for(int i=0;i<98;i+=2) {
      writer.deleteDocuments(new Term("id", "" + i));
    }
    writer.close();
    ir = DirectoryReader.open(dir);
    assertEquals(49, ir.numDocs());
    ir.close();

    writer = new IndexWriter(
        dir,
        newIndexWriterConfig(new MockAnalyzer(random()))
           .setMergePolicy(newLogMergePolicy(3))
    );
    writer.forceMergeDeletes(false);
    writer.close();
    ir = DirectoryReader.open(dir);
    assertEquals(49, ir.maxDoc());
    assertEquals(49, ir.numDocs());
    ir.close();
    dir.close();
  }
  
  // Just intercepts all merges & verifies that we are never
  // merging a segment with >= 20 (maxMergeDocs) docs
  private class MyMergeScheduler extends MergeScheduler {
    @Override
    synchronized public void merge(IndexWriter writer, MergeTrigger trigger, boolean newMergesFound) throws IOException {

      while(true) {
        MergePolicy.OneMerge merge = writer.getNextMerge();
        if (merge == null) {
          break;
        }
        int numDocs = 0;
        for(int i=0;i<merge.segments.size();i++) {
          int maxDoc = merge.segments.get(i).info.maxDoc();
          numDocs += maxDoc;
          assertTrue(maxDoc < 20);
        }
        writer.merge(merge);
        assertEquals(numDocs, merge.getMergeInfo().info.maxDoc());
      }
    }

    @Override
    public void close() {}
  }

  // LUCENE-1013
  public void testSetMaxMergeDocs() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()))
      .setMergeScheduler(new MyMergeScheduler())
      .setMaxBufferedDocs(2)
      .setMergePolicy(newLogMergePolicy());
    LogMergePolicy lmp = (LogMergePolicy) conf.getMergePolicy();
    lmp.setMaxMergeDocs(20);
    lmp.setMergeFactor(2);
    IndexWriter iw = new IndexWriter(dir, conf);
    Document document = new Document();

    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    
    document.add(newField("tvtest", "a b c", customType));
    for(int i=0;i<177;i++)
      iw.addDocument(document);
    iw.close();
    dir.close();
  }
  
  @Slow
  public void testNoWaitClose() throws Throwable {
    Directory directory = newDirectory();

    final Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setTokenized(false);

    Field idField = newField("id", "", customType);
    doc.add(idField);

    for(int pass=0;pass<2;pass++) {
      if (VERBOSE) {
        System.out.println("TEST: pass=" + pass);
      }

      IndexWriterConfig conf =  newIndexWriterConfig(new MockAnalyzer(random())).
              setOpenMode(OpenMode.CREATE).
              setMaxBufferedDocs(2).
              setMergePolicy(newLogMergePolicy()).
              setCommitOnClose(false);
      if (pass == 2) {
        conf.setMergeScheduler(new SerialMergeScheduler());
      }

      IndexWriter writer = new IndexWriter(directory, conf);
      ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(100);          

      for(int iter=0;iter<10;iter++) {
        if (VERBOSE) {
          System.out.println("TEST: iter=" + iter);
        }
        for(int j=0;j<199;j++) {
          idField.setStringValue(Integer.toString(iter*201+j));
          writer.addDocument(doc);
        }

        int delID = iter*199;
        for(int j=0;j<20;j++) {
          writer.deleteDocuments(new Term("id", Integer.toString(delID)));
          delID += 5;
        }

        writer.commit();

        // Force a bunch of merge threads to kick off so we
        // stress out aborting them on close:
        ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(2);

        final IndexWriter finalWriter = writer;
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread t1 = new Thread() {
            @Override
            public void run() {
              boolean done = false;
              while(!done) {
                for(int i=0;i<100;i++) {
                  try {
                    finalWriter.addDocument(doc);
                  } catch (AlreadyClosedException e) {
                    done = true;
                    break;
                  } catch (NullPointerException e) {
                    done = true;
                    break;
                  } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    failure.set(e);
                    done = true;
                    break;
                  }
                }
                Thread.yield();
              }

            }
          };

        t1.start();

        writer.close();
        t1.join();

        if (failure.get() != null) {
          throw failure.get();
        }

        // Make sure reader can read
        IndexReader reader = DirectoryReader.open(directory);
        reader.close();

        // Reopen
        writer = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random()))
                                              .setOpenMode(OpenMode.APPEND)
                                              .setMergePolicy(newLogMergePolicy())
                                              .setCommitOnClose(false));
      }
      writer.close();
    }

    directory.close();
  }
}
