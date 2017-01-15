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
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.LuceneTestCase;

/** 
 * Acts like a virus checker on Windows, where random programs may open the files you just wrote in an unfriendly
 * way preventing deletion (e.g. not passing FILE_SHARE_DELETE) or renaming or overwriting etc.  This is more evil
 * than WindowsFS which just prevents deletion of files you still old open.
 */
public class VirusCheckingFS extends FilterFileSystemProvider {

  private volatile boolean enabled = true;

  private final AtomicLong state;

  /** 
   * Create a new instance, wrapping {@code delegate}.
   */
  public VirusCheckingFS(FileSystem delegate, long salt) {
    super("viruschecking://", delegate);
    this.state = new AtomicLong(salt);
  }

  public void enable() {
    enabled = true;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void disable() {
    enabled = false;
  }

  @Override
  public void delete(Path path) throws IOException {

    // Fake but deterministic and hopefully portable like-randomness:
    long hash = state.incrementAndGet() * path.getFileName().hashCode();
    
    if (enabled // test infra disables when it's "really" time to delete after test is done, so it can reclaim temp dirs
        && Files.exists(path) // important that we NOT delay a NoSuchFileException until later
        && path.getFileName().toString().equals(IndexWriter.WRITE_LOCK_NAME) == false // life is particularly difficult if the virus checker hits our lock file
        && (hash % 5) == 1) {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("NOTE: VirusCheckingFS now refusing to delete " + path);
      }
      throw new AccessDeniedException("VirusCheckingFS is randomly refusing to delete file \"" + path + "\"");
    }
    super.delete(path);
  }

  // TODO: we could be more evil here, e.g. rename, createOutput, deleteIfExists
}
