package org.apache.lucene.util;

import java.io.*;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * A {@link Closeable} that attempts to remove a given file/folder.
 */
final class CloseableFile implements Closeable {
  private final File file;
  private final TestRuleMarkFailure failureMarker;

  public CloseableFile(File file, TestRuleMarkFailure failureMarker) {
    this.file = file;
    this.failureMarker = failureMarker;
  }

  @Override
  public void close() throws IOException {
    // only if there were no other test failures.
    if (failureMarker.wasSuccessful()) {
      if (file.exists()) {
        try {
          _TestUtil.rmDir(file);
        } catch (IOException e) {
          // Ignore the exception from rmDir.
        }

        // Re-check.
        if (file.exists()) {
          throw new IOException(
            "Could not remove: " + file.getAbsolutePath());
        }
    }
    }
  }
}