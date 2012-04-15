package org.apache.lucene;

/**
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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;

/**
 * Holds tests cases to verify external APIs are accessible
 * while not being in org.apache.lucene.index package.
 */
public class TestMergeSchedulerExternal extends LuceneTestCase {

  volatile boolean mergeCalled;
  volatile boolean mergeThreadCreated;
  volatile boolean excCalled;

  private class MyMergeScheduler extends ConcurrentMergeScheduler {

    private class MyMergeThread extends ConcurrentMergeScheduler.MergeThread {
      public MyMergeThread(IndexWriter writer, MergePolicy.OneMerge merge) throws IOException {
        super(writer, merge);
        mergeThreadCreated = true;
      }
    }

    @Override
    protected MergeThread getMergeThread(IndexWriter writer, MergePolicy.OneMerge merge) throws IOException {
      MergeThread thread = new MyMergeThread(writer, merge);
      thread.setThreadPriority(getMergeThreadPriority());
      thread.setDaemon(true);
      thread.setName("MyMergeThread");
      return thread;
    }

    @Override
    protected void handleMergeException(Throwable t) {
      excCalled = true;
    }

    @Override
    protected void doMerge(MergePolicy.OneMerge merge) throws IOException {
      mergeCalled = true;
      super.doMerge(merge);
    }
  }

  private static class FailOnlyOnMerge extends MockDirectoryWrapper.Failure {
    @Override
    public void eval(MockDirectoryWrapper dir)  throws IOException {
      StackTraceElement[] trace = new Exception().getStackTrace();
      for (int i = 0; i < trace.length; i++) {
        if ("doMerge".equals(trace[i].getMethodName()))
          throw new IOException("now failing during merge");
      }
    }
  }

  public void testSubclassConcurrentMergeScheduler() throws IOException {
    MockDirectoryWrapper dir = newDirectory();
    dir.failOn(new FailOnlyOnMerge());

    Document doc = new Document();
    Field idField = newField("id", "", StringField.TYPE_STORED);
    doc.add(idField);
    
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergeScheduler(new MyMergeScheduler())
        .setMaxBufferedDocs(2).setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setMergePolicy(newLogMergePolicy()));
    LogMergePolicy logMP = (LogMergePolicy) writer.getConfig().getMergePolicy();
    logMP.setMergeFactor(10);
    for(int i=0;i<20;i++)
      writer.addDocument(doc);

    ((MyMergeScheduler) writer.getConfig().getMergeScheduler()).sync();
    writer.close();
    
    assertTrue(mergeThreadCreated);
    assertTrue(mergeCalled);
    assertTrue(excCalled);
    dir.close();
  }
  
  private static class ReportingMergeScheduler extends MergeScheduler {

    @Override
    public void merge(IndexWriter writer) throws CorruptIndexException, IOException {
      OneMerge merge = null;
      while ((merge = writer.getNextMerge()) != null) {
        if (VERBOSE) {
          System.out.println("executing merge " + merge.segString(writer.getDirectory()));
        }
        writer.merge(merge);
      }
    }

    @Override
    public void close() throws CorruptIndexException, IOException {}
    
  }

  public void testCustomMergeScheduler() throws Exception {
    // we don't really need to execute anything, just to make sure the custom MS
    // compiles. But ensure that it can be used as well, e.g., no other hidden
    // dependencies or something. Therefore, don't use any random API !
    Directory dir = new RAMDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, null);
    conf.setMergeScheduler(new ReportingMergeScheduler());
    IndexWriter writer = new IndexWriter(dir, conf);
    writer.addDocument(new Document());
    writer.commit(); // trigger flush
    writer.addDocument(new Document());
    writer.commit(); // trigger flush
    writer.forceMerge(1);
    writer.close();
    dir.close();
  }
  
}
