package org.apache.lucene.index;

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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class TestNoDeletionPolicy extends LuceneTestCase {

  @Test
  public void testNoDeletionPolicy() throws Exception {
    IndexDeletionPolicy idp = NoDeletionPolicy.INSTANCE;
    idp.onInit(null);
    idp.onCommit(null);
  }

  @Test
  public void testFinalSingleton() throws Exception {
    assertTrue(Modifier.isFinal(NoDeletionPolicy.class.getModifiers()));
    Constructor<?>[] ctors = NoDeletionPolicy.class.getDeclaredConstructors();
    assertEquals("expected 1 private ctor only: " + Arrays.toString(ctors), 1, ctors.length);
    assertTrue("that 1 should be private: " + ctors[0], Modifier.isPrivate(ctors[0].getModifiers()));
  }

  @Test
  public void testMethodsOverridden() throws Exception {
    // Ensures that all methods of IndexDeletionPolicy are
    // overridden/implemented. That's important to ensure that NoDeletionPolicy 
    // overrides everything, so that no unexpected behavior/error occurs.
    // NOTE: even though IndexDeletionPolicy is an interface today, and so all
    // methods must be implemented by NoDeletionPolicy, this test is important
    // in case one day IDP becomes an abstract class.
    for (Method m : NoDeletionPolicy.class.getMethods()) {
      // getDeclaredMethods() returns just those methods that are declared on
      // NoDeletionPolicy. getMethods() returns those that are visible in that
      // context, including ones from Object. So just filter out Object. If in
      // the future IndexDeletionPolicy will become a class that extends a
      // different class than Object, this will need to change.
      if (m.getDeclaringClass() != Object.class) {
        assertTrue(m + " is not overridden !", m.getDeclaringClass() == NoDeletionPolicy.class);
      }
    }
  }

  @Test
  public void testAllCommitsRemain() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE));
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(newField("c", "a" + i, TextField.TYPE_STORED));
      writer.addDocument(doc);
      writer.commit();
      assertEquals("wrong number of commits !", i + 1, DirectoryReader.listCommits(dir).size());
    }
    writer.close();
    dir.close();
  }
  
}
