package org.apache.lucene.store;

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

import org.apache.lucene.util.LuceneTestCase;
import java.lang.reflect.Method;

public class TestMMapDirectory extends LuceneTestCase {

  // Simply verify that if there is a method in FSDirectory
  // that returns IndexInput or a subclass, that
  // MMapDirectory overrides it.
  public void testIndexInputMethods() throws ClassNotFoundException  {
    
    Class FSDirectory = Class.forName("org.apache.lucene.store.FSDirectory");
    Class IndexInput = Class.forName("org.apache.lucene.store.IndexInput");
    Class MMapDirectory = Class.forName("org.apache.lucene.store.MMapDirectory");

    Method[] methods = FSDirectory.getDeclaredMethods();
    for(int i=0;i<methods.length;i++) {
      Method method = methods[i];
      if (IndexInput.isAssignableFrom(method.getReturnType())) {
        // There is a method that returns IndexInput or a
        // subclass of IndexInput
        try {
          Method m = MMapDirectory.getMethod(method.getName(), method.getParameterTypes());
          if (m.getDeclaringClass() != MMapDirectory) {
            fail("FSDirectory has method " + method + " but MMapDirectory does not override");
          }
        } catch (NoSuchMethodException e) {
          // Should not happen
          fail("unexpected NoSuchMethodException");
        }
      }
    }
  }
}
