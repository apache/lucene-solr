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
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/** 
 * Disables actual calls to fsync.
 * <p>
 * All other filesystem operations are passed thru as normal.
 */
public class DisableFsyncFS extends FilterFileSystemProvider {
  
  /** 
   * Create a new instance, wrapping {@code delegate}.
   */
  public DisableFsyncFS(FileSystem delegate) {
    super("disablefsync://", delegate);
  }

  @Override
  public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
    return new FilterFileChannel(super.newFileChannel(path, options, attrs)) {
      @Override
      public void force(boolean metaData) throws IOException {}
    };
  }

  @Override
  public AsynchronousFileChannel newAsynchronousFileChannel(Path path, Set<? extends OpenOption> options, ExecutorService executor, FileAttribute<?>... attrs) throws IOException {
    return new FilterAsynchronousFileChannel(super.newAsynchronousFileChannel(path, options, executor, attrs)) {
      @Override
      public void force(boolean metaData) throws IOException {}
    };
  }
}
