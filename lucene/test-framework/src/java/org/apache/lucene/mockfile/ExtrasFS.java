package org.apache.lucene.mockfile;

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

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import com.carrotsearch.randomizedtesting.RandomizedContext;

/** 
 * Adds extra files/subdirectories when directories are created.
 * <p>
 * Lucene shouldn't care about these, but sometimes operating systems
 * create special files themselves (.DS_Store, thumbs.db, .nfsXXX, ...),
 * so we add them and see what breaks. 
 * <p>
 * When a directory is created, sometimes a file or directory named 
 * "extra0" will be included with it.
 * All other filesystem operations are passed thru as normal.
 */
public class ExtrasFS extends FilterFileSystemProvider {
  final int seed;
  
  /** 
   * Create a new instance, wrapping {@code delegate}.
   */
  public ExtrasFS(FileSystem delegate, Random random) {
    super("extras://", delegate);
    this.seed = random.nextInt();
  }

  @Override
  public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
    super.createDirectory(dir, attrs);   
    // ok, we created the directory successfully.
    
    // a little funky: we only look at hashcode (well-defined) of the target class name.
    // using a generator won't reproduce, because we are a per-class resource.
    // using hashing on filenames won't reproduce, because many of the names rely on other things
    // the test class did.
    // so a test gets terrorized with extras or gets none at all depending on the initial seed.
    int hash = RandomizedContext.current().getTargetClass().toString().hashCode() ^ seed;
    if ((hash & 3) == 0) {
      // lets add a bogus file... if this fails, we don't care, its best effort.
      try {
        Path target = dir.resolve("extra0");
        if (hash < 0) {
          super.createDirectory(target);
        } else {
          Files.createFile(target);
        }
      } catch (Exception ignored) { 
        // best effort
      }
    }
  }
  
  // TODO: would be great if we overrode attributes, so file size was always zero for
  // our fake files. But this is tricky because its hooked into several places. 
  // Currently MDW has a hack so we don't break disk full tests.

}
