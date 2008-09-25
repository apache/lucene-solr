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
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;


/**
 * Holds tests cases to verify external APIs are accessible
 * while not being in org.apache.lucene.index package.
 */
public class TestMergeSchedulerExternal extends LuceneTestCase {

  volatile boolean mergeCalled;
  volatile boolean mergeThreadCreated;
  volatile boolean excCalled;

  private class MyMergeException extends RuntimeException {
    Directory dir;
    public MyMergeException(Throwable exc, Directory dir) {
      super(exc);
      this.dir = dir;
    }
  }

  private class MyMergeScheduler extends ConcurrentMergeScheduler {

    private class MyMergeThread extends ConcurrentMergeScheduler.MergeThread {
      public MyMergeThread(IndexWriter writer, MergePolicy.OneMerge merge) throws IOException {
        super(writer, merge);
        mergeThreadCreated = true;
      }
    }

    protected MergeThread getMergeThread(IndexWriter writer, MergePolicy.OneMerge merge) throws IOException {
      MergeThread thread = new MyMergeThread(writer, merge);
      thread.setThreadPriority(getMergeThreadPriority());
      thread.setDaemon(true);
      thread.setName("MyMergeThread");
      return thread;
    }

    protected void handleMergeException(Throwable t) {
      excCalled = true;
    }

    protected void doMerge(MergePolicy.OneMerge merge) throws IOException {
      mergeCalled = true;
      super.doMerge(merge);
    }
  }

  private static class FailOnlyOnMerge extends MockRAMDirectory.Failure {
    public void eval(MockRAMDirectory dir)  throws IOException {
      StackTraceElement[] trace = new Exception().getStackTrace();
      for (int i = 0; i < trace.length; i++) {
        if ("doMerge".equals(trace[i].getMethodName()))
          throw new IOException("now failing during merge");
      }
    }
  }

  public void testSubclassConcurrentMergeScheduler() throws IOException {
    MockRAMDirectory dir = new MockRAMDirectory();
    dir.failOn(new FailOnlyOnMerge());

    Document doc = new Document();
    Field idField = new Field("id", "", Field.Store.YES, Field.Index.NOT_ANALYZED);
    doc.add(idField);
    
    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    MyMergeScheduler ms = new MyMergeScheduler();
    writer.setMergeScheduler(ms);
    writer.setMaxBufferedDocs(2);
    writer.setRAMBufferSizeMB(writer.DISABLE_AUTO_FLUSH);
    for(int i=0;i<20;i++)
      writer.addDocument(doc);

    ms.sync();
    writer.close();
    
    assertTrue(mergeThreadCreated);
    assertTrue(mergeCalled);
    assertTrue(excCalled);
    dir.close();
    assertTrue(ConcurrentMergeScheduler.anyUnhandledExceptions());
  }
}
