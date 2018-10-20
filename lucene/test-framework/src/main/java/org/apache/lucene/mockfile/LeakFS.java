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
package org.apache.lucene.mockfile;

import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 
 * FileSystem that tracks open handles.
 * <p>
 * When {@link FileSystem#close()} is called, this class will throw
 * an exception if any file handles are still open.
 */
public class LeakFS extends HandleTrackingFS {
  // we explicitly use reference hashcode/equality in our keys
  private final Map<Object,Exception> openHandles = new ConcurrentHashMap<>();
  
  /**
   * Create a new instance, tracking file handle leaks for the 
   * specified delegate filesystem.
   * @param delegate delegate filesystem to wrap.
   */
  public LeakFS(FileSystem delegate) {
    super("leakfs://", delegate);
  }

  @Override
  protected void onOpen(Path path, Object stream) {
    openHandles.put(stream, new Exception());
  }

  @Override
  protected void onClose(Path path, Object stream) {
    openHandles.remove(stream);
  }

  @Override
  public synchronized void onClose() {
    if (!openHandles.isEmpty()) {
      // print the first one as it's very verbose otherwise
      Exception cause = null;
      Iterator<Exception> stacktraces = openHandles.values().iterator();
      if (stacktraces.hasNext()) {
        cause = stacktraces.next();
      }
      throw new RuntimeException("file handle leaks: " + openHandles.keySet(), cause);
    }
  }
}
