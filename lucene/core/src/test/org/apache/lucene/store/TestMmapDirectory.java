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
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.Ignore;

/**
 * Tests MMapDirectory
 */
// See: https://issues.apache.org/jira/browse/SOLR-12028 Tests cannot remove files on Windows machines occasionally
public class TestMmapDirectory extends BaseDirectoryTestCase {

  @Override
  protected Directory getDirectory(Path path) throws IOException {
    MMapDirectory m = new MMapDirectory(path);
    m.setPreload(random().nextBoolean());
    return m;
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    assumeTrue(MMapDirectory.UNMAP_NOT_SUPPORTED_REASON, MMapDirectory.UNMAP_SUPPORTED);
  }
  
  @Ignore("This test is for JVM testing purposes. There are no guarantees that it may not fail with SIGSEGV!")
  public void testAceWithThreads() throws Exception {
    for (int iter = 0; iter < 10; iter++) {
      Directory dir = getDirectory(createTempDir("testAceWithThreads"));
      IndexOutput out = dir.createOutput("test", IOContext.DEFAULT);
      Random random = random();
      for (int i = 0; i < 8 * 1024 * 1024; i++) {
        out.writeInt(random.nextInt());
      }
      out.close();
      IndexInput in = dir.openInput("test", IOContext.DEFAULT);
      IndexInput clone = in.clone();
      final byte accum[] = new byte[32 * 1024 * 1024];
      final CountDownLatch shotgun = new CountDownLatch(1);
      Thread t1 = new Thread(() -> {
        try {
          shotgun.await();
          for (int i = 0; i < 10; i++) {
            clone.seek(0);
            clone.readBytes(accum, 0, accum.length);
          }
        } catch (IOException | AlreadyClosedException ok) {
          // OK
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });
      t1.start();
      shotgun.countDown();
      in.close();
      t1.join();
      dir.close();
    }
  }
}
