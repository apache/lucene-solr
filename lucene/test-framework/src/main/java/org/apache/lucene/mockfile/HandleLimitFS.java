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

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

/** 
 * FileSystem that throws exception if file handles 
 * in use exceeds a specified limit 
 */
public class HandleLimitFS extends HandleTrackingFS {
  final int limit;
  final AtomicInteger count = new AtomicInteger();
  
  /**
   * Create a new instance, limiting the maximum number
   * of open files to {@code limit}
   * @param delegate delegate filesystem to wrap.
   * @param limit maximum number of open files.
   */
  public HandleLimitFS(FileSystem delegate, int limit) {
    super("handlelimit://", delegate);
    this.limit = limit;
  }

  @Override
  protected void onOpen(Path path, Object stream) throws IOException {
    if (count.incrementAndGet() > limit) {
      count.decrementAndGet();
      throw new FileSystemException(path.toString(), null, "Too many open files");
    }
  }

  @Override
  protected void onClose(Path path, Object stream) throws IOException {
    count.decrementAndGet();
  }
}
