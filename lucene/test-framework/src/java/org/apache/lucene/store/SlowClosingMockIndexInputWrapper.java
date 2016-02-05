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

import org.apache.lucene.util.ThreadInterruptedException;

/**
 * hangs onto files a little bit longer (50ms in close).
 * MockDirectoryWrapper acts like windows: you can't delete files
 * open elsewhere. so the idea is to make race conditions for tiny
 * files (like segments) easier to reproduce.
 */
class SlowClosingMockIndexInputWrapper extends MockIndexInputWrapper {

  public SlowClosingMockIndexInputWrapper(MockDirectoryWrapper dir,
      String name, IndexInput delegate) {
    super(dir, name, delegate);
  }
  
  @Override
  public void close() throws IOException {
    try {
      Thread.sleep(50);
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    } finally {
      super.close();
    }
  }
}
