package org.apache.lucene.benchmark.byTask.utils;

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

import java.io.File;
import java.io.IOException;

/**
 * File utilities.
 */
public class FileUtils {

  /**
   * Delete files and directories, even if non-empty.
   *
   * @param dir file or directory
   * @return true on success, false if no or part of files have been deleted
   * @throws IOException If there is a low-level I/O error.
   */
  public static boolean fullyDelete(File dir) throws IOException {
    if (dir == null || !dir.exists()) return false;
    File contents[] = dir.listFiles();
    if (contents != null) {
      for (int i = 0; i < contents.length; i++) {
        if (contents[i].isFile()) {
          if (!contents[i].delete()) {
            return false;
          }
        } else {
          if (!fullyDelete(contents[i])) {
            return false;
          }
        }
      }
    }
    return dir.delete();
  }

}
