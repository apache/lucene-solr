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

package org.apache.solr.update.processor;

import static org.hamcrest.CoreMatchers.is;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.VersionBucket;
import org.junit.BeforeClass;
import org.junit.Test;

public class DistributedUpdateProcessorTest extends SolrTestCaseJ4 {

  private static ExecutorService executor;

  @BeforeClass
  public static void beforeClass() throws Exception {
    executor = Executors.newCachedThreadPool();
    initCore("solr/collection1/conf/solrconfig.xml","solr/collection1/conf/schema-minimal-with-another-uniqkey.xml");
  }
  
  public static void AfterClass() {
    executor.shutdown();
  }

  @Test
  public void testShouldBufferUpdate() {
    SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), new ModifiableSolrParams());
    DistributedUpdateProcessor processor = new DistributedUpdateProcessor(
        req, null, null, null);

    AddUpdateCommand cmd = new AddUpdateCommand(req);
    cmd.solrDoc = new SolrInputDocument();
    cmd.solrDoc.setField("notid", "10");

    // applying buffer updates, isReplayOrPeerSync flag doesn't matter
    assertFalse(processor.shouldBufferUpdate(cmd, false, UpdateLog.State.APPLYING_BUFFERED));
    assertFalse(processor.shouldBufferUpdate(cmd, true, UpdateLog.State.APPLYING_BUFFERED));

    assertTrue(processor.shouldBufferUpdate(cmd, false, UpdateLog.State.BUFFERING));
    // this is not an buffer updates and it depend on other updates
    cmd.prevVersion = 10;
    assertTrue(processor.shouldBufferUpdate(cmd, false, UpdateLog.State.APPLYING_BUFFERED));
  }

  @Test
  public void testVersionAdd() throws IOException {
    SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), new ModifiableSolrParams());
    int threads = 5;
    Function<DistributedUpdateProcessor,Boolean> versionAddFunc = (DistributedUpdateProcessor process) -> {
      try {
        AddUpdateCommand cmd = new AddUpdateCommand(req);
        cmd.solrDoc = new SolrInputDocument();
        cmd.solrDoc.setField("notid", "10");
        return process.versionAdd(cmd);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
    int failed = runCommands(threads, 1000, req, versionAddFunc);
    // only one should succeed
    assertThat(failed, is(threads - 1));

    failed = runCommands(threads, -1, req, versionAddFunc);
    // all should succeed
    assertThat(failed, is(0));
  }

  @Test
  public void testVersionDelete() throws IOException {
    SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), new ModifiableSolrParams());

    int threads = 5;
    Function<DistributedUpdateProcessor,Boolean> versionDeleteFunc = (DistributedUpdateProcessor process) -> {
      try {
        DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);
        cmd.id = "1";
        return process.versionDelete(cmd);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };

    int failed = runCommands(threads, 1000, req, versionDeleteFunc);
    // only one should succeed
    assertThat(failed, is(threads - 1));

    failed = runCommands(threads, -1, req, versionDeleteFunc);
    // all should succeed
    assertThat(failed, is(0));
  }

  public int runCommands(int threads, int versionLockInMill, SolrQueryRequest req,
      Function<DistributedUpdateProcessor,Boolean> function)
      throws IOException {
    try (DistributedUpdateProcessor processor = new DistributedUpdateProcessor(
        req, null, null, null) {
      @Override
      protected boolean tryLockElseThrow(VersionBucket bucket) {
        bucket.setVersionLockInMill(versionLockInMill);

        boolean locked = super.tryLockElseThrow(bucket);
        if (locked) {
          try {
            // simulate the case: it takes 5 seconds to add the doc
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        } else {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Unable to get version bucket lock in " + bucket.getVersionLockInMill() + " mill seconds");
        }
        return locked;
      }
    }) {
      CountDownLatch latch = new CountDownLatch(1);
      Collection<Future<Boolean>> futures = new ArrayList<>();
      for (int t = 0; t < threads; ++t) {
        futures.add(executor.submit(() -> {
          latch.await();
          return function.apply(processor);
        }));
      }
      latch.countDown();

      int failed = 0;
      for (Future<Boolean> f : futures) {
        try {
          f.get();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          failed++;
        }
      }
      return failed;
    }
  }
}
