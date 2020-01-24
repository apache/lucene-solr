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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;

/** 
 * Adds extra files/subdirectories when directories are created.
 * <p>
 * Lucene shouldn't care about these, but sometimes operating systems
 * create special files themselves (.DS_Store, thumbs.db, .nfsXXX, ...),
 * so we add them and see what breaks. 
 * <p>
 * When a directory is created, sometimes an "extra" file or directory
 * will be included with it (use {@link #isExtra(String)} to check if it's one
 * of those files).
 *
 * All other filesystem operations are delegated as normal.
 */
public class ExtrasFS extends FilterFileSystemProvider {
  private final static String EXTRA_FILE_NAME = "extra0";

  final boolean active;
  final boolean createDirectory;
  
  /** 
   * Create a new instance, wrapping {@code delegate}.
   * @param active {@code true} if we should create extra files
   * @param createDirectory {@code true} if we should create directories instead of files.
   *        Ignored if {@code active} is {@code false}.
   */
  public ExtrasFS(FileSystem delegate, boolean active, boolean createDirectory) {
    super("extras://", delegate);
    this.active = active;
    this.createDirectory = createDirectory;
  }

  @Override
  public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
    super.createDirectory(dir, attrs);   
    // ok, we created the directory successfully.
    
    if (active) {
      // lets add a bogus file... if this fails, we don't care, its best effort.
      try {
        Path target = dir.resolve(EXTRA_FILE_NAME);
        if (createDirectory) {
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

  /**
   * @return Return true if {@code fileName} is one of the extra files added by this class.
   */
  public static boolean isExtra(String fileName) {
    return fileName.equals(EXTRA_FILE_NAME);
  }
}
