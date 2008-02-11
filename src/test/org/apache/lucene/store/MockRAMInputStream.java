package org.apache.lucene.store;

import java.io.IOException;

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

/**
 * Used by MockRAMDirectory to create an input stream that
 * keeps track of when it's been closed.
 */

public class MockRAMInputStream extends RAMInputStream {
  private MockRAMDirectory dir;
  private String name;
  private boolean isClone;

  /** Construct an empty output buffer. 
   * @throws IOException */
  public MockRAMInputStream(MockRAMDirectory dir, String name, RAMFile f) throws IOException {
    super(f);
    this.name = name;
    this.dir = dir;
  }

  public void close() {
    super.close();
    // Pending resolution on LUCENE-686 we may want to
    // remove the conditional check so we also track that
    // all clones get closed:
    if (!isClone) {
      synchronized(dir.openFiles) {
        Integer v = (Integer) dir.openFiles.get(name);
        // Could be null when MockRAMDirectory.crash() was called
        if (v != null) {
          if (v.intValue() == 1) {
            dir.openFiles.remove(name);
          } else {
            v = new Integer(v.intValue()-1);
            dir.openFiles.put(name, v);
          }
        }
      }
    }
  }

  public Object clone() {
    MockRAMInputStream clone = (MockRAMInputStream) super.clone();
    clone.isClone = true;
    // Pending resolution on LUCENE-686 we may want to
    // uncomment this code so that we also track that all
    // clones get closed:
    /*
    synchronized(dir.openFiles) {
      if (dir.openFiles.containsKey(name)) {
        Integer v = (Integer) dir.openFiles.get(name);
        v = new Integer(v.intValue()+1);
        dir.openFiles.put(name, v);
      } else {
        throw new RuntimeException("BUG: cloned file was not open?");
      }
    }
    */
    return clone;
  }
}
