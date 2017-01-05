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

package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.common.collect.ImmutableSet;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.OverseerMessageHandler.Lock;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICAPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MODIFYCOLLECTION;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.SPLITSHARD;

public class TestLockTree extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public void testLocks() throws Exception {
    LockTree lockTree = new LockTree();
    Lock coll1Lock = lockTree.getSession().lock(CollectionAction.CREATE,
        Arrays.asList("coll1"));
    assertNotNull(coll1Lock);
    assertNull("Should not be able to lock coll1/shard1", lockTree.getSession().lock(CollectionAction.BALANCESHARDUNIQUE,
        Arrays.asList("coll1", "shard1")));

    assertNull(lockTree.getSession().lock(ADDREPLICAPROP,
        Arrays.asList("coll1", "shard1", "core_node2")));
    coll1Lock.unlock();
    Lock shard1Lock = lockTree.getSession().lock(CollectionAction.BALANCESHARDUNIQUE,
        Arrays.asList("coll1", "shard1"));
    assertNotNull(shard1Lock);
    shard1Lock.unlock();
    Lock replica1Lock = lockTree.getSession().lock(ADDREPLICAPROP,
        Arrays.asList("coll1", "shard1", "core_node2"));
    assertNotNull(replica1Lock);


    List<Pair<CollectionAction, List<String>>> operations = new ArrayList<>();
    operations.add(new Pair<>(ADDREPLICAPROP, Arrays.asList("coll1", "shard1", "core_node2")));
    operations.add(new Pair<>(MODIFYCOLLECTION, Arrays.asList("coll1")));
    operations.add(new Pair<>(SPLITSHARD, Arrays.asList("coll1", "shard1")));
    operations.add(new Pair<>(SPLITSHARD, Arrays.asList("coll2", "shard2")));
    operations.add(new Pair<>(MODIFYCOLLECTION, Arrays.asList("coll2")));
    operations.add(new Pair<>(DELETEREPLICA, Arrays.asList("coll2", "shard1")));

    List<Set<String>> orderOfExecution = Arrays.asList(
        ImmutableSet.of("coll1/shard1/core_node2", "coll2/shard2"),
        ImmutableSet.of("coll1", "coll2"),
        ImmutableSet.of("coll1/shard1", "coll2/shard1"));
    lockTree = new LockTree();
    for (int counter = 0; counter < orderOfExecution.size(); counter++) {
      LockTree.Session session = lockTree.getSession();
      List<Pair<CollectionAction, List<String>>> completedOps = new CopyOnWriteArrayList<>();
      List<Lock> locks = new CopyOnWriteArrayList<>();
      List<Thread> threads = new ArrayList<>();
      for (int i = 0; i < operations.size(); i++) {
        Pair<CollectionAction, List<String>> operation = operations.get(i);
        final Lock lock = session.lock(operation.first(), operation.second());
        if (lock != null) {
          Thread thread = new Thread(getRunnable(completedOps, operation, locks, lock));
          threads.add(thread);
          thread.start();
        }
      }


      for (Thread thread : threads) thread.join();
      if (locks.isEmpty())
        throw new RuntimeException("Could not attain lock for anything " + operations);

      Set<String> expectedOps = orderOfExecution.get(counter);
      log.info("counter : {} , expected : {}, actual : {}", counter, expectedOps, locks);
      assertEquals(expectedOps.size(), locks.size());
      for (Lock lock : locks)
        assertTrue("locks : " + locks + " expectedOps : " + expectedOps, expectedOps.contains(lock.toString()));
      locks.clear();
      for (Pair<CollectionAction, List<String>> completedOp : completedOps) {
        operations.remove(completedOp);
      }
    }
  }

  private Runnable getRunnable(List<Pair<CollectionAction, List<String>>> completedOps, Pair<CollectionAction,
      List<String>> operation, List<Lock> locks, Lock lock) {
    return () -> {
      try {
        Thread.sleep(1);
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        lock.unlock();
        completedOps.add(operation);
        locks.add(lock);
      }
    };
  }
}
