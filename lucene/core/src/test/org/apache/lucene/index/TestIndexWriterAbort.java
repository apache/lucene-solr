package org.apache.lucene.index;

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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.cranky.CrankyCodec;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Rethrow;

/** 
 * Creates aborting exceptions and checks that files are cleaned up
 */
@SuppressCodecs("SimpleText")
public class TestIndexWriterAbort extends LuceneTestCase {
  
  // just one thread, serial merge policy, hopefully debuggable
  public void testBasics() throws Exception {
    // use an explicit RAMDirectory, we will act-like windows,
    // but no virus scanner (real or fake).
    // TODO: use MDW's check instead of TestIW.assertNoUnreferencedFiles,
    // then turn it all back on

    MockDirectoryWrapper dir = new MockDirectoryWrapper(random(), new RAMDirectory());
    dir.setEnableVirusScanner(false); // assert that we actually delete files.
    
    // log all exceptions we hit, in case we fail (for debugging)
    ByteArrayOutputStream exceptionLog = new ByteArrayOutputStream();
    PrintStream exceptionStream = new PrintStream(exceptionLog, true, "UTF-8");
    //PrintStream exceptionStream = System.out;
    
    Analyzer analyzer = new MockAnalyzer(random());
    
    // wrap the actual codec with one that throws random exceptions
    Codec codec = new CrankyCodec(Codec.getDefault(), new Random(random().nextLong()));
    
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    // just for now, try to keep this test reproducible
    conf.setMergeScheduler(new SerialMergeScheduler());
    conf.setCodec(codec);
    
    int numDocs = atLeast(500);
    
    IndexWriter iw = new IndexWriter(dir, conf);
    try {
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
          } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().startsWith("Fake IOException")) {
              exceptionStream.println("\nTEST: got expected fake exc:" + e.getMessage());
              e.printStackTrace(exceptionStream);
            } else {
              Rethrow.rethrow(e);
            }
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
          } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().startsWith("Fake IOException")) {
              exceptionStream.println("\nTEST: got expected fake exc:" + e.getMessage());
              e.printStackTrace(exceptionStream);
            } else {
              Rethrow.rethrow(e);
            }
          }
        }

        if (random().nextInt(10) == 0) {
          // trigger flush:
          try {
            if (random().nextBoolean()) {
              DirectoryReader ir = null;
              try {
                ir = DirectoryReader.open(iw, random().nextBoolean());
              } finally {
                IOUtils.closeWhileHandlingException(ir);
              }
            } else {
              iw.commit();
            }
          } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().startsWith("Fake IOException")) {
              exceptionStream.println("\nTEST: got expected fake exc:" + e.getMessage());
              e.printStackTrace(exceptionStream);
            } else {
              Rethrow.rethrow(e);
            }
          }
        }
      }
      
      try {
        iw.close();
      } catch (Exception e) {
        if (e.getMessage() != null && e.getMessage().startsWith("Fake IOException")) {
          exceptionStream.println("\nTEST: got expected fake exc:" + e.getMessage());
          e.printStackTrace(exceptionStream);
        } else {
          Rethrow.rethrow(e);
        }
      }
      
      // test that there are no unreferenced files: any deletes we make should have succeeded
      // TODO: don't use this method, use the MDW one!!!!
      TestIndexWriter.assertNoUnreferencedFiles(dir, "unreferenced files remain after close()");
      // checkindex
      dir.close();
    } catch (Throwable t) {
      System.out.println("Unexpected exception: dumping fake-exception-log:...");
      exceptionStream.flush();
      System.out.println(exceptionLog.toString("UTF-8"));
      System.out.flush();
      Rethrow.rethrow(t);
    }
    
    if (VERBOSE) {
      System.out.println("TEST PASSED: dumping fake-exception-log:...");
      System.out.println(exceptionLog.toString("UTF-8"));
    }
  }
}
