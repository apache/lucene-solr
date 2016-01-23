package org.apache.lucene.util;

import java.io.Closeable;

import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.junit.Assert;

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

/**
 * Attempts to close a {@link BaseDirectoryWrapper}.
 * 
 * @see LuceneTestCase#newDirectory(java.util.Random)
 */
final class CloseableDirectory implements Closeable {
  private final BaseDirectoryWrapper dir;
  private final TestRuleMarkFailure failureMarker;
  
  public CloseableDirectory(BaseDirectoryWrapper dir,
      TestRuleMarkFailure failureMarker) {
    this.dir = dir;
    this.failureMarker = failureMarker;
  }
  
  @Override
  public void close() {
    // We only attempt to check open/closed state if there were no other test
    // failures.
    try {
      if (failureMarker.wasSuccessful() && dir.isOpen()) {
        Assert.fail("Directory not closed: " + dir);
      }
    } finally {
      // TODO: perform real close of the delegate: LUCENE-4058
      // dir.close();
    }
  }
}
