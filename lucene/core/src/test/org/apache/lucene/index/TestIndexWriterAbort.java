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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Rethrow;
import org.apache.lucene.util.TestUtil;

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
    FieldTypes fieldTypes = iw.getFieldTypes();
    fieldTypes.disableSorting("dv2");
    fieldTypes.setMultiValued("dv4");
    fieldTypes.setMultiValued("dv5");
    fieldTypes.setMultiValued("stored1");
    // ensure we get some vectors
    fieldTypes.enableTermVectors("text_vectors");
    try {
      for (int i = 0; i < numDocs; i++) {
        Document doc = iw.newDocument();
        doc.addAtom("id", Integer.toString(i));
        doc.addInt("dv", i);
        doc.addBinary("dv2", new BytesRef(Integer.toString(i)));
        doc.addBinary("dv3", new BytesRef(Integer.toString(i)));
        doc.addBinary("dv4", new BytesRef(Integer.toString(i)));
        doc.addBinary("dv4", new BytesRef(Integer.toString(i-1)));
        doc.addInt("dv5", i);
        doc.addInt("dv5", i-1);
        doc.addLargeText("text1", TestUtil.randomAnalysisString(random(), 20, true));
        // ensure we store something
        doc.addStored("stored1", "foo");
        doc.addStored("stored1", "bar");
        // ensure we get some payloads
        doc.addLargeText("text_payloads", TestUtil.randomAnalysisString(random(), 6, true));
        doc.addLargeText("text_vectors", TestUtil.randomAnalysisString(random(), 6, true));
        
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
          Document doc2 = iw.newDocument();
          doc2.addAtom("id", Integer.toString(-i));
          doc2.addLargeText("text1", TestUtil.randomAnalysisString(random(), 20, true));
          doc2.addStored("stored1", "foo");
          doc2.addStored("stored1", "bar");
          doc2.addLargeText("text_vectors", TestUtil.randomAnalysisString(random(), 6, true));
          
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
