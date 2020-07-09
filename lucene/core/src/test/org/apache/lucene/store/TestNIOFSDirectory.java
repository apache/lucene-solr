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
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;

import org.apache.lucene.mockfile.FilterFileChannel;
import org.apache.lucene.mockfile.FilterPath;
import org.apache.lucene.mockfile.LeakFS;

/**
 * Tests NIOFSDirectory
 */
public class TestNIOFSDirectory extends BaseDirectoryTestCase {

  @Override
  protected Directory getDirectory(Path path) throws IOException {
    return new NIOFSDirectory(path);
  }

  public void testHandleExceptionInConstructor() throws Exception {
    Path path = createTempDir().toRealPath();
    final LeakFS leakFS = new LeakFS(path.getFileSystem()) {
      @Override
      public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options,
                                        FileAttribute<?>... attrs) throws IOException {
        return new FilterFileChannel(super.newFileChannel(path, options, attrs)) {
          @Override
          public long size() throws IOException {
            throw new IOException("simulated");
          }
        };
      }
    };
    FileSystem fs = leakFS.getFileSystem(URI.create("file:///"));
    Path wrapped = new FilterPath(path, fs);
    try (Directory dir = new NIOFSDirectory(wrapped)) {
      try (IndexOutput out = dir.createOutput("test.bin", IOContext.DEFAULT)) {
        out.writeString("hello");
      }
      final IOException error = expectThrows(IOException.class, () -> dir.openInput("test.bin", IOContext.DEFAULT));
      assertEquals("simulated", error.getMessage());
    }
  }
}
