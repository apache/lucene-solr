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
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Gives an unpredictable, but deterministic order to directory listings.
 * <p>
 * This can be useful if for instance, you have build servers on
 * linux but developers are using macs.
 */
public class ShuffleFS extends FilterFileSystemProvider {
  final long seed;
  
  /** 
   * Create a new instance, wrapping {@code delegate}.
   */
  public ShuffleFS(FileSystem delegate, long seed) {
    super("shuffle://", delegate);
    this.seed = seed;
  }

  @Override
  public DirectoryStream<Path> newDirectoryStream(Path dir, Filter<? super Path> filter) throws IOException {
    try (DirectoryStream<Path> stream = super.newDirectoryStream(dir, filter)) {
      // read complete directory listing
      final List<Path> contents = new ArrayList<>();
      for (Path path : stream) {
        contents.add(path);
      }
      // sort first based only on filename
      Collections.sort(contents, new Comparator<Path>() {
        @Override
        public int compare(Path path1, Path path2) {
          return path1.getFileName().toString().compareTo(path2.getFileName().toString());
        }
      });
      // sort based on current class seed
      Collections.shuffle(contents, new Random(seed));
      return new DirectoryStream<Path>() {
        @Override
        public Iterator<Path> iterator() {
          return contents.iterator();
        }
        @Override
        public void close() throws IOException {}        
      };
    }
  }
}
