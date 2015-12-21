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
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

/** 
 * Acts like Windows, where random programs may open the files you just wrote in an unfriendly
 * way preventing deletion (e.g. not passing FILE_SHARE_DELETE) or renaming or overwriting etc.
 */
public class VirusCheckingFS extends FilterFileSystemProvider {

  final Random random;

  /** 
   * Create a new instance, wrapping {@code delegate}.
   */
  public VirusCheckingFS(FileSystem delegate, Random random) {
    super("viruschecking://", delegate);
    this.random = random;
  }

  @Override
  public void delete(Path path) throws IOException {
    if (random.nextInt(5) == 1) {
      throw new AccessDeniedException("VirusCheckingFS is randomly refusing to delete file \"" + path + "\"");
    }
    super.delete(path);
  }

  // TODO: rename?  createOutput?  deleteIfExists?
}
