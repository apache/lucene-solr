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

package org.apache.solr.schema;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestManagedSchemaThreadSafety extends SolrTestCaseJ4 {

  private static final class SuspendingZkClient extends SolrZkClient {
    AtomicReference<Thread> slowpoke = new AtomicReference<>();

    private SuspendingZkClient(String zkServerAddress, int zkClientTimeout) {
      super(zkServerAddress, zkClientTimeout);
    }

    boolean isSlowpoke(){
      Thread youKnow;
      if ((youKnow = slowpoke.get())!=null) {
        return youKnow == Thread.currentThread();
      } else {
        return slowpoke.compareAndSet(null, Thread.currentThread());
      }
    }

    @Override
    public byte[] getData(String path, Watcher watcher, Stat stat, boolean retryOnConnLoss)
        throws KeeperException, InterruptedException {
      byte[] data;
      try {
        data = super.getData(path, watcher, stat, retryOnConnLoss);
      } catch (NoNodeException e) {
        if (isSlowpoke()) {
          //System.out.println("suspending "+Thread.currentThread()+" on " + path);
          Thread.sleep(500);
        }
        throw e;
      }
      return data;
    }
  }

  private static ZkTestServer zkServer;
  private static Path loaderPath;

  @BeforeClass
  public static void startZkServer() throws Exception {
    zkServer = new ZkTestServer(createTempDir());
    zkServer.run();
    loaderPath = createTempDir();
  }

  @AfterClass
  public static void stopZkServer() throws Exception {
    if (null != zkServer) {
      zkServer.shutdown();
      zkServer = null;
    }
    loaderPath = null;
  }

  @Test
  @LogLevel("org.apache.solr.common.cloud.SolrZkClient=debug")
  public void testThreadSafety() throws Exception {

    final String configsetName = "managed-config";//

    try (SolrZkClient client = new SuspendingZkClient(zkServer.getZkHost(), 30000)) {
      // we can pick any to load configs, I suppose, but here we check
      client.upConfig(configset("cloud-managed-upgrade"), configsetName);
    }

    ExecutorService executor = ExecutorUtil.newMDCAwareCachedThreadPool("threadpool");
    
    try (SolrZkClient raceJudge = new SuspendingZkClient(zkServer.getZkHost(), 30000)) {

      ZkController zkController = createZkController(raceJudge);

      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        futures.add(executor.submit(indexSchemaLoader(configsetName, zkController)));
      }

      for (Future<?> future : futures) {
        future.get();
      }
    }
    finally {
      ExecutorUtil.shutdownAndAwaitTermination(executor);
    }
  }

  private ZkController createZkController(SolrZkClient client) throws KeeperException, InterruptedException {
    assumeWorkingMockito();
    
    CoreContainer mockAlwaysUpCoreContainer = mock(CoreContainer.class, 
        Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS));
    when(mockAlwaysUpCoreContainer.isShutDown()).thenReturn(Boolean.FALSE);  // Allow retry on session expiry
    
    
    ZkController zkController = mock(ZkController.class,
        Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS));

    when(zkController.getCoreContainer()).thenReturn(mockAlwaysUpCoreContainer);

    when(zkController.getZkClient()).thenReturn(client);
    Mockito.doAnswer(new Answer<Boolean>() {
      volatile boolean sessionExpired=false;
      
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        String path = (String) invocation.getArguments()[0];
        perhapsExpired();
        Boolean exists = client.exists(path, true);
        perhapsExpired();
        return exists;
      }

      private void perhapsExpired() throws SessionExpiredException {
        if (!sessionExpired && rarely()) {
          sessionExpired = true;
          throw new KeeperException.SessionExpiredException();
        }
      }
    }).when(zkController).pathExists(Mockito.anyString());
    return zkController;
  }

  private Runnable indexSchemaLoader(String configsetName, final ZkController zkController) {
    return () -> {
      try {
        SolrResourceLoader loader = new ZkSolrResourceLoader(loaderPath, configsetName, zkController);
        SolrConfig solrConfig = SolrConfig.readFromResourceLoader(loader, "solrconfig.xml");

        ManagedIndexSchemaFactory factory = new ManagedIndexSchemaFactory();
        factory.init(new NamedList());
        factory.create("schema.xml", solrConfig);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

}
