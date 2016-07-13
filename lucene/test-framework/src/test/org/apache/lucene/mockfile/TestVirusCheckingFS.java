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
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;

/** Basic tests for VirusCheckingFS */
public class TestVirusCheckingFS extends MockFileSystemTestCase {
  
  @Override
  protected Path wrap(Path path) {
    FileSystem fs = new VirusCheckingFS(path.getFileSystem(), random().nextLong()).getFileSystem(URI.create("file:///"));
    return new FilterPath(path, fs);
  }
  
  /** Test Files.delete fails if a file has an open inputstream against it */
  public void testDeleteSometimesFails() throws IOException {
    Path dir = wrap(createTempDir());

    int counter = 0;
    while (true) {
      Path path = dir.resolve("file" + counter);
      counter++;

      OutputStream file = Files.newOutputStream(path);
      file.write(5);
      file.close();

      // File is now closed, we attempt delete:
      try {
        Files.delete(path);
      } catch (AccessDeniedException ade) {
        // expected (sometimes)
        assertTrue(ade.getMessage().contains("VirusCheckingFS is randomly refusing to delete file "));
        break;
      }

      assertFalse(Files.exists(path));
    }
  }
}
