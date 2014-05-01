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
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CrankyTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.cranky.CrankyCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Rethrow;

/** 
 * Causes a bunch of non-aborting and aborting exceptions and checks that
 * no index corruption is ever created
 */
// TODO: not sure which fails are test bugs or real bugs yet...
// reproduce with: ant test  -Dtestcase=TestIndexWriterExceptions2 -Dtests.method=testSimple -Dtests.seed=9D05AC6DFF3CC9A4 -Dtests.multiplier=10 -Dtests.locale=fi_FI -Dtests.timezone=Canada/Pacific -Dtests.file.encoding=ISO-8859-1
// also sometimes when it fails, the exception-stream printing doesnt seem to be working yet
// 
public class TestIndexWriterExceptions2 extends LuceneTestCase {
  
  // just one thread, serial merge policy, hopefully debuggable
  public void testBasics() throws Exception {
    Directory dir = newDirectory();
    
    // log all exceptions we hit, in case we fail (for debugging)
    ByteArrayOutputStream exceptionLog = new ByteArrayOutputStream();
    PrintStream exceptionStream = new PrintStream(exceptionLog, true, "UTF-8");
    //PrintStream exceptionStream = System.out;
    
    // create lots of non-aborting exceptions with a broken analyzer
    final long analyzerSeed = random().nextLong();
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, false);
        tokenizer.setEnableChecks(false); // TODO: can we turn this on? our filter is probably too evil
        TokenStream stream = new CrankyTokenFilter(tokenizer, new Random(analyzerSeed));
        return new TokenStreamComponents(tokenizer, stream);
      }
    };
    
    // create lots of aborting exceptions with a broken codec
    Codec codec = new CrankyCodec(Codec.getDefault(), new Random(random().nextLong()));
    
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    // just for now, try to keep this test reproducible
    conf.setMergeScheduler(new SerialMergeScheduler());
    conf.setCodec(codec);
    
    // TODO: too much?
    int numDocs = RANDOM_MULTIPLIER * 1000;
    
    IndexWriter iw = new IndexWriter(dir, conf);
    try {
      for (int i = 0; i < numDocs; i++) {
        // TODO: add crankyDocValuesFields, etc
        Document doc = new Document();
        doc.add(newStringField("id", Integer.toString(i), Field.Store.NO));
        doc.add(new NumericDocValuesField("dv", i));
        doc.add(newTextField("text1", TestUtil.randomAnalysisString(random(), 20, true), Field.Store.NO));
        // TODO: sometimes update dv
        try {
          iw.addDocument(doc);
          // we made it, sometimes delete our doc
          if (random().nextInt(4) == 0) {
            iw.deleteDocuments(new Term("id", Integer.toString(i)));
          }
        } catch (Exception e) {
          if (e.getMessage() != null && e.getMessage().startsWith("Fake IOException")) {
            exceptionStream.println("\nTEST: got expected fake exc:" + e.getMessage());
            e.printStackTrace(exceptionStream);
          } else {
            Rethrow.rethrow(e);
          }
        }
        if (random().nextInt(10) == 0) {
          // trigger flush:
          try {
            if (random().nextBoolean()) {
              DirectoryReader ir = null;
              try {
                ir = DirectoryReader.open(iw, random().nextBoolean());
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
        iw.shutdown();
      } catch (Exception e) {
        if (e.getMessage() != null && e.getMessage().startsWith("Fake IOException")) {
          exceptionStream.println("\nTEST: got expected fake exc:" + e.getMessage());
          e.printStackTrace(exceptionStream);
          try {
            iw.rollback();
          } catch (Throwable t) {}
        } else {
          Rethrow.rethrow(e);
        }
      }
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
