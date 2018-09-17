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

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestFilterCodecReader extends LuceneTestCase {

  public void testDeclaredMethodsOverridden() throws Exception {
    final Class<?> subClass = FilterCodecReader.class;
    implTestDeclaredMethodsOverridden(subClass.getSuperclass(), subClass);
  }

  public void testGetDelegate() throws IOException {
    try (Directory dir = newDirectory();
         IndexWriter w = new IndexWriter(dir,newIndexWriterConfig())) {
      w.addDocument(new Document());
      try (DirectoryReader reader = w.getReader()) {
        FilterCodecReader r = FilterCodecReader.wrapLiveDocs((CodecReader) reader.getSequentialSubReaders().get(0),
            null, 1);

        assertSame(FilterCodecReader.unwrap(r), reader.getSequentialSubReaders().get(0));
        assertSame(r.getDelegate(), reader.getSequentialSubReaders().get(0));
      }
    }
  }

  private void implTestDeclaredMethodsOverridden(Class<?> superClass, Class<?> subClass) throws Exception {
    for (final Method superClassMethod : superClass.getDeclaredMethods()) {
      final int modifiers = superClassMethod.getModifiers();
      if (Modifier.isPrivate(modifiers)) continue;
      if (Modifier.isFinal(modifiers)) continue;
      if (Modifier.isStatic(modifiers)) continue;
      try {
        final Method subClassMethod = subClass.getDeclaredMethod(
            superClassMethod.getName(),
            superClassMethod.getParameterTypes());
        assertEquals("getReturnType() difference",
            superClassMethod.getReturnType(),
            subClassMethod.getReturnType());
      } catch (NoSuchMethodException e) {
        fail(subClass + " needs to override '" + superClassMethod + "'");
      }
    }
  }

}
