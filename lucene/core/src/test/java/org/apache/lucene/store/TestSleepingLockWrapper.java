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
package org.apache.lucene.store;


import java.io.IOException;
import java.nio.file.Path;

import org.apache.lucene.store.BaseLockFactoryTestCase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.store.SleepingLockWrapper;
import org.apache.lucene.util.TestUtil;

/** Simple tests for SleepingLockWrapper */
public class TestSleepingLockWrapper extends BaseLockFactoryTestCase {

  @Override
  protected Directory getDirectory(Path path) throws IOException {
    long lockWaitTimeout = TestUtil.nextLong(random(), 20, 100);
    long pollInterval = TestUtil.nextLong(random(), 2, 10);
    
    int which = random().nextInt(3);
    switch (which) {
      case 0:
        return new SleepingLockWrapper(newDirectory(random(), new SingleInstanceLockFactory()), lockWaitTimeout, pollInterval);
      case 1:
        return new SleepingLockWrapper(newFSDirectory(path), lockWaitTimeout, pollInterval);
      default:
        return new SleepingLockWrapper(newFSDirectory(path), lockWaitTimeout, pollInterval);
    }
  }
  
  // TODO: specific tests to this impl
}
