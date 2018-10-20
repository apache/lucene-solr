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
package org.apache.lucene.util;

import java.util.Collections;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import com.carrotsearch.randomizedtesting.RandomizedTest;

// LUCENE-4456: Test that we fail if there are unreferenced files
public class TestFailIfUnreferencedFiles extends WithNestedTests {
  public TestFailIfUnreferencedFiles() {
    super(true);
  }
  
  public static class Nested1 extends WithNestedTests.AbstractNestedTest {
    public void testDummy() throws Exception {
      MockDirectoryWrapper dir = newMockDirectory();
      dir.setAssertNoUnrefencedFilesOnClose(true);
      IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(null));
      iw.addDocument(new Document());
      iw.close();
      IndexOutput output = dir.createOutput("_hello.world", IOContext.DEFAULT);
      output.writeString("i am unreferenced!");
      output.close();
      dir.sync(Collections.singleton("_hello.world"));
      dir.close();
    }
  }

  @Test
  public void testFailIfUnreferencedFiles() {
    Result r = JUnitCore.runClasses(Nested1.class);
    RandomizedTest.assumeTrue("Ignoring nested test, very likely zombie threads present.", 
        r.getIgnoreCount() == 0);

    // We are suppressing output anyway so dump the failures.
    for (Failure f : r.getFailures()) {
      System.out.println(f.getTrace());
    }

    Assert.assertEquals("Expected exactly one failure.", 
        1, r.getFailureCount());
    Assert.assertTrue("Expected unreferenced files assertion.", 
        r.getFailures().get(0).getTrace().contains("unreferenced files:"));
  }
}
