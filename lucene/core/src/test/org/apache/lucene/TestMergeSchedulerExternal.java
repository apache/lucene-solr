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

package org.apache.lucene;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.junit.AfterClass;

/**
 * Holds tests cases to verify external APIs are accessible
 * while not being in org.apache.lucene.index package.
 */
public class TestMergeSchedulerExternal extends LuceneTestCase {

  volatile boolean mergeCalled;
  volatile boolean mergeThreadCreated;
  volatile boolean excCalled;
  volatile static InfoStream infoStream;

  private class MyMergeScheduler extends ConcurrentMergeScheduler {

    private class MyMergeThread extends ConcurrentMergeScheduler.MergeThread {
      public MyMergeThread(MergeSource mergeSource, MergePolicy.OneMerge merge) {
        super(mergeSource, merge);
        mergeThreadCreated = true;
      }
    }

    @Override
    protected MergeThread getMergeThread(MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
      MergeThread thread = new MyMergeThread(mergeSource, merge);
      thread.setDaemon(true);
      thread.setName("MyMergeThread");
      return thread;
    }

    @Override
    protected void handleMergeException(Throwable t) {
      excCalled = true;
      if (infoStream.isEnabled("IW")) {
        infoStream.message("IW", "TEST: now handleMergeException");
      }
    }

    @Override
    protected void doMerge(MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
      mergeCalled = true;
      super.doMerge(mergeSource, merge);
    }
  }

  private static class FailOnlyOnMerge extends MockDirectoryWrapper.Failure {
    @Override
    public void eval(MockDirectoryWrapper dir)  throws IOException {
      if (callStackContainsAnyOf("doMerge")) {
        IOException ioe = new IOException("now failing during merge");
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        ioe.printStackTrace(pw);
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "TEST: now throw exc:\n" + sw.toString());
        }
        throw ioe;
      }
    }
  }

  @AfterClass
  public static void afterClass() {
    infoStream = null;
  }

  public void testSubclassConcurrentMergeScheduler() throws IOException {
    MockDirectoryWrapper dir = newMockDirectory();
    dir.failOn(new FailOnlyOnMerge());

    Document doc = new Document();
    Field idField = newStringField("id", "", Field.Store.YES);
    doc.add(idField);

    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()))
      .setMergeScheduler(new MyMergeScheduler())
      .setMaxBufferedDocs(2).setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
      .setMergePolicy(newLogMergePolicy());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    infoStream = new PrintStreamInfoStream(new PrintStream(baos, true, IOUtils.UTF_8));
    iwc.setInfoStream(infoStream);

    IndexWriter writer = new IndexWriter(dir, iwc);
    LogMergePolicy logMP = (LogMergePolicy) writer.getConfig().getMergePolicy();
    logMP.setMergeFactor(10);
    for(int i=0;i<20;i++) {
      writer.addDocument(doc);
    }

    try {
      ((MyMergeScheduler) writer.getConfig().getMergeScheduler()).sync();
    } catch (IllegalStateException ise) {
      // OK
    }
    writer.rollback();

    try {
      assertTrue(mergeThreadCreated);
      assertTrue(mergeCalled);
      assertTrue(excCalled);
    } catch (AssertionError ae) {
      System.out.println("TEST FAILED; IW infoStream output:");
      System.out.println(baos.toString(IOUtils.UTF_8));
      throw ae;
    }
    dir.close();
  }
  
  private static class ReportingMergeScheduler extends MergeScheduler {

    @Override
    public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
      OneMerge merge = null;
      while ((merge = mergeSource.getNextMerge()) != null) {
        if (VERBOSE) {
          System.out.println("executing merge " + merge.segString());
        }
        mergeSource.merge(merge);
      }
    }

    @Override
    public void close() throws IOException {}
    
  }

  public void testCustomMergeScheduler() throws Exception {
    // we don't really need to execute anything, just to make sure the custom MS
    // compiles. But ensure that it can be used as well, e.g., no other hidden
    // dependencies or something. Therefore, don't use any random API !
    Directory dir = new RAMDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(null);
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
