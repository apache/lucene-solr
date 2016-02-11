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


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.MockVariableLengthPayloadFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.MockDirectoryWrapper.Failure;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.Rethrow;
import org.apache.lucene.util.TestUtil;

/** 
 * Causes a bunch of fake VM errors and checks that no other exceptions are delivered instead,
 * no index corruption is ever created.
 */
@SuppressCodecs("SimpleText")
public class TestIndexWriterOnVMError extends LuceneTestCase {
  
  // just one thread, serial merge policy, hopefully debuggable
  private void doTest(MockDirectoryWrapper.Failure failOn) throws Exception {   
    // log all exceptions we hit, in case we fail (for debugging)
    ByteArrayOutputStream exceptionLog = new ByteArrayOutputStream();
    PrintStream exceptionStream = new PrintStream(exceptionLog, true, "UTF-8");
    //PrintStream exceptionStream = System.out;
    
    final long analyzerSeed = random().nextLong();
    final Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        tokenizer.setEnableChecks(false); // we are gonna make it angry
        TokenStream stream = tokenizer;
        // emit some payloads
        if (fieldName.contains("payloads")) {
          stream = new MockVariableLengthPayloadFilter(new Random(analyzerSeed), stream);
        }
        return new TokenStreamComponents(tokenizer, stream);
      }
    };
    
    MockDirectoryWrapper dir = null;
    
    final int numIterations = TEST_NIGHTLY ? atLeast(100) : atLeast(5);
    
    STARTOVER:
    for (int iter = 0; iter < numIterations; iter++) {
      try {
        // close from last run
        if (dir != null) {
          dir.close();
        }
        // disable slow things: we don't rely upon sleeps here.
        dir = newMockDirectory();
        dir.setThrottling(MockDirectoryWrapper.Throttling.NEVER);
        dir.setUseSlowOpenClosers(false);
      
        IndexWriterConfig conf = newIndexWriterConfig(analyzer);
        // just for now, try to keep this test reproducible
        conf.setMergeScheduler(new SerialMergeScheduler());
      
        // test never makes it this far...
        int numDocs = atLeast(2000);
      
        IndexWriter iw = new IndexWriter(dir, conf);
        iw.commit(); // ensure there is always a commit

        dir.failOn(failOn);
        
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          doc.add(newStringField("id", Integer.toString(i), Field.Store.NO));
          doc.add(new NumericDocValuesField("dv", i));
          doc.add(new BinaryDocValuesField("dv2", new BytesRef(Integer.toString(i))));
          doc.add(new SortedDocValuesField("dv3", new BytesRef(Integer.toString(i))));
          doc.add(new SortedSetDocValuesField("dv4", new BytesRef(Integer.toString(i))));
          doc.add(new SortedSetDocValuesField("dv4", new BytesRef(Integer.toString(i-1))));
          doc.add(new SortedNumericDocValuesField("dv5", i));
          doc.add(new SortedNumericDocValuesField("dv5", i-1));
          doc.add(newTextField("text1", TestUtil.randomAnalysisString(random(), 20, true), Field.Store.NO));
          // ensure we store something
          doc.add(new StoredField("stored1", "foo"));
          doc.add(new StoredField("stored1", "bar"));    
          // ensure we get some payloads
          doc.add(newTextField("text_payloads", TestUtil.randomAnalysisString(random(), 6, true), Field.Store.NO));
          // ensure we get some vectors
          FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
          ft.setStoreTermVectors(true);
          doc.add(newField("text_vectors", TestUtil.randomAnalysisString(random(), 6, true), ft));
          doc.add(new IntPoint("point", random().nextInt()));
          doc.add(new IntPoint("point2d", random().nextInt(), random().nextInt()));
          
          if (random().nextInt(10) > 0) {
            // single doc
            try {
              iw.addDocument(doc);
              // we made it, sometimes delete our doc, or update a dv
              int thingToDo = random().nextInt(4);
              if (thingToDo == 0) {
                iw.deleteDocuments(new Term("id", Integer.toString(i)));
              } else if (thingToDo == 1) {
                iw.updateNumericDocValue(new Term("id", Integer.toString(i)), "dv", i+1L);
              } else if (thingToDo == 2) {
                iw.updateBinaryDocValue(new Term("id", Integer.toString(i)), "dv2", new BytesRef(Integer.toString(i+1)));
              }
            } catch (VirtualMachineError | AlreadyClosedException disaster) {
              getTragedy(disaster, iw, exceptionStream);
              continue STARTOVER;
            }
          } else {
            // block docs
            Document doc2 = new Document();
            doc2.add(newStringField("id", Integer.toString(-i), Field.Store.NO));
            doc2.add(newTextField("text1", TestUtil.randomAnalysisString(random(), 20, true), Field.Store.NO));
            doc2.add(new StoredField("stored1", "foo"));
            doc2.add(new StoredField("stored1", "bar"));
            doc2.add(newField("text_vectors", TestUtil.randomAnalysisString(random(), 6, true), ft));
            
            try {
              iw.addDocuments(Arrays.asList(doc, doc2));
              // we made it, sometimes delete our docs
              if (random().nextBoolean()) {
                iw.deleteDocuments(new Term("id", Integer.toString(i)), new Term("id", Integer.toString(-i)));
              }
            } catch (VirtualMachineError | AlreadyClosedException disaster) {
              getTragedy(disaster, iw, exceptionStream);
              continue STARTOVER;
            }
          }
          
          if (random().nextInt(10) == 0) {
            // trigger flush:
            try {
              if (random().nextBoolean()) {
                DirectoryReader ir = null;
                try {
                  ir = DirectoryReader.open(iw, random().nextBoolean(), false);
                  TestUtil.checkReader(ir);
                } finally {
                  IOUtils.closeWhileHandlingException(ir);
                }
              } else {
                iw.commit();
              }
              if (DirectoryReader.indexExists(dir)) {
                TestUtil.checkIndex(dir);
              }
            } catch (VirtualMachineError | AlreadyClosedException disaster) {
              getTragedy(disaster, iw, exceptionStream);
              continue STARTOVER;
            }
          }
        }
        
        try {
          iw.close();
        } catch (VirtualMachineError | AlreadyClosedException disaster) {
          getTragedy(disaster, iw, exceptionStream);
          continue STARTOVER;
        }
      } catch (Throwable t) {
        System.out.println("Unexpected exception: dumping fake-exception-log:...");
        exceptionStream.flush();
        System.out.println(exceptionLog.toString("UTF-8"));
        System.out.flush();
        Rethrow.rethrow(t);
      }
    }
    dir.close();
    if (VERBOSE) {
      System.out.println("TEST PASSED: dumping fake-exception-log:...");
      System.out.println(exceptionLog.toString("UTF-8"));
    }
  }
  
  private VirtualMachineError getTragedy(Throwable disaster, IndexWriter writer, PrintStream log) {
    Throwable e = disaster;
    if (e instanceof AlreadyClosedException) {
      e = e.getCause();
    }
    
    if (e instanceof VirtualMachineError && e.getMessage() != null && e.getMessage().startsWith("Fake")) {
      log.println("\nTEST: got expected fake exc:" + e.getMessage());
      e.printStackTrace(log);
      // TODO: remove rollback here, and add this assert to ensure "full OOM protection" anywhere IW does writes
      // assertTrue("hit OOM but writer is still open, WTF: ", writer.isClosed());
      try {
        writer.rollback();
      } catch (Throwable t) {}
      return (VirtualMachineError) e;
    } else {
      Rethrow.rethrow(disaster);
      return null; // dead
    }
  }

  public void testOOM() throws Exception {
    final Random r = new Random(random().nextLong());
    doTest(new Failure() {
      @Override
      public void eval(MockDirectoryWrapper dir) throws IOException {
        if (r.nextInt(3000) == 0) {
          StackTraceElement stack[] = Thread.currentThread().getStackTrace();
          boolean ok = false;
          for (int i = 0; i < stack.length; i++) {
            if (stack[i].getClassName().equals(IndexWriter.class.getName())) {
              ok = true;
            }
          }
          if (ok) {
            throw new OutOfMemoryError("Fake OutOfMemoryError");
          }
        }
      }
    });
  }
  
  public void testUnknownError() throws Exception {
    final Random r = new Random(random().nextLong());
    doTest(new Failure() {
      @Override
      public void eval(MockDirectoryWrapper dir) throws IOException {
        if (r.nextInt(3000) == 0) {
          StackTraceElement stack[] = Thread.currentThread().getStackTrace();
          boolean ok = false;
          for (int i = 0; i < stack.length; i++) {
            if (stack[i].getClassName().equals(IndexWriter.class.getName())) {
              ok = true;
            }
          }
          if (ok) {
            throw new UnknownError("Fake UnknownError");
          }
        }
      }
    });
  }
  
  @Nightly
  public void testCheckpoint() throws Exception {
    final Random r = new Random(random().nextLong());
    doTest(new Failure() {
      @Override
      public void eval(MockDirectoryWrapper dir) throws IOException {
        StackTraceElement stack[] = Thread.currentThread().getStackTrace();
        boolean ok = false;
        for (int i = 0; i < stack.length; i++) {
          if (stack[i].getClassName().equals(IndexFileDeleter.class.getName()) && stack[i].getMethodName().equals("checkpoint")) {
            ok = true;
          }
        }
        if (ok && r.nextInt(4) == 0) {
          throw new OutOfMemoryError("Fake OutOfMemoryError");
        }
      }
    });
  }
}
