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

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Basic tests for DisableFsyncFS */
public class TestDisableFsyncFS extends MockFileSystemTestCase {
  
  @Override
  protected Path wrap(Path path) {
    FileSystem fs = new DisableFsyncFS(path.getFileSystem()).getFileSystem(URI.create("file:///"));
    return new FilterPath(path, fs);
  }
  
  /** Test that we don't corrumpt fsync: it just doesnt happen */
  public void testFsyncWorks() throws Exception {
    Path dir = wrap(createTempDir());
    
    FileChannel file = FileChannel.open(dir.resolve("file"), 
                                        StandardOpenOption.CREATE_NEW, 
                                        StandardOpenOption.READ, 
                                        StandardOpenOption.WRITE);
    byte bytes[] = new byte[128];
    random().nextBytes(bytes);
    file.write(ByteBuffer.wrap(bytes));
    file.force(true);
    file.close();
  }
}
