package org.apache.lucene.util.junitcompat;

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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.store.Directory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class TestFailOnFieldCacheInsanity extends WithNestedTests {
  public TestFailOnFieldCacheInsanity() {
    super(true);
  }
  
  public static class Nested1 extends WithNestedTests.AbstractNestedTest {
    private Directory d;
    private IndexReader r;
    private AtomicReader subR;

    private void makeIndex() throws Exception {
      d = newDirectory();
      RandomIndexWriter w = new RandomIndexWriter(random(), d);
      Document doc = new Document();
      doc.add(newField("ints", "1", StringField.TYPE_NOT_STORED));
      w.addDocument(doc);
      w.forceMerge(1);
      r = w.getReader();
      w.close();

      subR = r.leaves().get(0).reader();
    }

    public void testDummy() throws Exception {
      makeIndex();
      assertNotNull(FieldCache.DEFAULT.getTermsIndex(subR, "ints"));
      assertNotNull(FieldCache.DEFAULT.getTerms(subR, "ints"));
      // NOTE: do not close reader/directory, else it
      // purges FC entries
    }
  }

  @Test
  public void testFailOnFieldCacheInsanity() {
    Result r = JUnitCore.runClasses(Nested1.class);
    boolean insane = false;
    for(Failure f : r.getFailures()) {
      if (f.getMessage().indexOf("Insane") != -1) {
        insane = true;
      }
    }
    Assert.assertTrue(insane);
  }
}
