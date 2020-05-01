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
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CrankyTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.MockVariableLengthPayloadFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.asserting.AssertingCodec;
import org.apache.lucene.codecs.cranky.CrankyCodec;
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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Rethrow;
import org.apache.lucene.util.TestUtil;

/** 
 * Causes a bunch of non-aborting and aborting exceptions and checks that
 * no index corruption is ever created
 */
@SuppressCodecs("SimpleText")
public class TestIndexWriterExceptions2 extends LuceneTestCase {
  
  // just one thread, serial merge policy, hopefully debuggable
  public void testBasics() throws Exception {
    // disable slow things: we don't rely upon sleeps here.
    Directory dir = newDirectory();
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
      ((MockDirectoryWrapper)dir).setUseSlowOpenClosers(false);
    }
    
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
        TokenStream stream = tokenizer;
        // emit some payloads
        if (fieldName.contains("payloads")) {
          stream = new MockVariableLengthPayloadFilter(new Random(analyzerSeed), stream);
        }
        stream = new CrankyTokenFilter(stream, new Random(analyzerSeed));
        return new TokenStreamComponents(tokenizer, stream);
      }
    };
    
    // create lots of aborting exceptions with a broken codec
    // we don't need a random codec, as we aren't trying to find bugs in the codec here.
    Codec inner = RANDOM_MULTIPLIER > 1 ? Codec.getDefault() : new AssertingCodec();
    Codec codec = new CrankyCodec(inner, new Random(random().nextLong()));
    
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    // just for now, try to keep this test reproducible
    conf.setMergeScheduler(new SerialMergeScheduler());
    conf.setCodec(codec);

    int numDocs = atLeast(100);
    
    IndexWriter iw = new IndexWriter(dir, conf);
    try {
      boolean allowAlreadyClosed = false;
      for (int i = 0; i < numDocs; i++) {
        // TODO: add crankyDocValuesFields, etc
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
          } catch (AlreadyClosedException ace) {
            // OK: writer was closed by abort; we just reopen now:
            assertTrue(iw.isDeleterClosed());
            assertTrue(allowAlreadyClosed);
            allowAlreadyClosed = false;
            conf = newIndexWriterConfig(analyzer);
            // just for now, try to keep this test reproducible
            conf.setMergeScheduler(new SerialMergeScheduler());
            conf.setCodec(codec);
            iw = new IndexWriter(dir, conf);            
          } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().startsWith("Fake IOException")) {
              exceptionStream.println("\nTEST: got expected fake exc:" + e.getMessage());
              e.printStackTrace(exceptionStream);
              allowAlreadyClosed = true;
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
          } catch (AlreadyClosedException ace) {
            // OK: writer was closed by abort; we just reopen now:
            assertTrue(iw.isDeleterClosed());
            assertTrue(allowAlreadyClosed);
            allowAlreadyClosed = false;
            conf = newIndexWriterConfig(analyzer);
            // just for now, try to keep this test reproducible
            conf.setMergeScheduler(new SerialMergeScheduler());
            conf.setCodec(codec);
            iw = new IndexWriter(dir, conf);            
          } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().startsWith("Fake IOException")) {
              exceptionStream.println("\nTEST: got expected fake exc:" + e.getMessage());
              e.printStackTrace(exceptionStream);
              allowAlreadyClosed = true;
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
          } catch (AlreadyClosedException ace) {
            // OK: writer was closed by abort; we just reopen now:
            assertTrue(iw.isDeleterClosed());
            assertTrue(allowAlreadyClosed);
            allowAlreadyClosed = false;
            conf = newIndexWriterConfig(analyzer);
            // just for now, try to keep this test reproducible
            conf.setMergeScheduler(new SerialMergeScheduler());
            conf.setCodec(codec);
            iw = new IndexWriter(dir, conf);            
          } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().startsWith("Fake IOException")) {
              exceptionStream.println("\nTEST: got expected fake exc:" + e.getMessage());
              e.printStackTrace(exceptionStream);
              allowAlreadyClosed = true;
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
