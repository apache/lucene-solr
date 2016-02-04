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
package org.apache.lucene.store;


import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class TestFilterDirectory extends BaseDirectoryTestCase {

  @Override
  protected Directory getDirectory(Path path) throws IOException {
    return new FilterDirectory(new RAMDirectory());
  }
  
  @Test
  public void testOverrides() throws Exception {
    // verify that all methods of Directory are overridden by FilterDirectory,
    // except those under the 'exclude' list
    Set<Method> exclude = new HashSet<>();
    exclude.add(Directory.class.getMethod("copyFrom", Directory.class, String.class, String.class, IOContext.class));
    exclude.add(Directory.class.getMethod("openChecksumInput", String.class, IOContext.class));
    for (Method m : FilterDirectory.class.getMethods()) {
      if (m.getDeclaringClass() == Directory.class) {
        assertTrue("method " + m.getName() + " not overridden!", exclude.contains(m));
      }
    }
  }

  public void testUnwrap() throws IOException {
    Directory dir = FSDirectory.open(createTempDir());
    FilterDirectory dir2 = new FilterDirectory(dir);
    assertEquals(dir, dir2.getDelegate());
    assertEquals(dir, FilterDirectory.unwrap(dir2));
    dir2.close();
  }
}
