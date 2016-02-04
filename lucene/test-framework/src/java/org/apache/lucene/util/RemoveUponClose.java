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
package org.apache.lucene.util;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A {@link Closeable} that attempts to remove a given file/folder.
 */
final class RemoveUponClose implements Closeable {
  private final Path path;
  private final TestRuleMarkFailure failureMarker;
  private final String creationStack;

  public RemoveUponClose(Path path, TestRuleMarkFailure failureMarker) {
    this.path = path;
    this.failureMarker = failureMarker;

    StringBuilder b = new StringBuilder();
    for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
      b.append('\t').append(e.toString()).append('\n');
    }
    creationStack = b.toString();
  }

  @Override
  public void close() throws IOException {
    // only if there were no other test failures.
    if (failureMarker.wasSuccessful()) {
      if (Files.exists(path)) {
        try {
          IOUtils.rm(path);
        } catch (IOException e) {
          throw new IOException(
              "Could not remove temporary location '" 
                  + path.toAbsolutePath() + "', created at stack trace:\n" + creationStack, e);
        }
      }
    }
  }
}