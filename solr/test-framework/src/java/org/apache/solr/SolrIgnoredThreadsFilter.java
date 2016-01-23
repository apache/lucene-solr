package org.apache.solr;

import org.apache.lucene.search.TimeLimitingCollector.TimerThread;

import com.carrotsearch.randomizedtesting.ThreadFilter;


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
 * This ignores those threads in Solr for which there is no way to
 * clean up after a suite.
 */
public class SolrIgnoredThreadsFilter implements ThreadFilter {
  @Override
  public boolean reject(Thread t) {
    /*
     * IMPORTANT! IMPORTANT!
     * 
     * Any threads added here should have ABSOLUTELY NO SIDE EFFECTS
     * (should be stateless). This includes no references to cores or other
     * test-dependent information.
     */

    String threadName = t.getName();
    if (threadName.equals(TimerThread.THREAD_NAME)) {
      return true;
    }

    if (threadName.startsWith("facetExecutor-") || 
        threadName.startsWith("cmdDistribExecutor-") ||
        threadName.startsWith("httpShardExecutor-")) {
      return true;
    }
    
    // This is a bug in ZooKeeper where they call System.exit(11) when
    // this thread receives an interrupt signal.
    if (threadName.startsWith("SyncThread")) {
      return true;
    }

    // THESE ARE LIKELY BUGS - these threads should be closed!
    if (threadName.startsWith("Overseer-") ||
        threadName.startsWith("aliveCheckExecutor-") ||
        threadName.startsWith("concurrentUpdateScheduler-")) {
      return true;
    }

    return false;
  }
}
