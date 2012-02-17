package org.apache.lucene.search;

/**
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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Utility class that runs a reopen thread to periodically
 * reopen the NRT searchers in the provided {@link
 * NRTManager}.
 *
 * <p> Typical usage looks like this:
 *
 * <pre>
 *   ... open your own writer ...
 * 
 *   NRTManager manager = new NRTManager(writer);
 *
 *   // Refreshes searcher every 5 seconds when nobody is waiting, and up to 100 msec delay
 *   // when somebody is waiting:
 *   NRTManagerReopenThread reopenThread = new NRTManagerReopenThread(manager, 5.0, 0.1);
 *   reopenThread.setName("NRT Reopen Thread");
 *   reopenThread.setPriority(Math.min(Thread.currentThread().getPriority()+2, Thread.MAX_PRIORITY));
 *   reopenThread.setDaemon(true);
 *   reopenThread.start();
 * </pre>
 *
 * Then, for each incoming query, do this:
 *
 * <pre>
 *   // For each incoming query:
 *   IndexSearcher searcher = manager.get();
 *   try {
 *     // Use searcher to search...
 *   } finally {
 *     manager.release(searcher);
 *   }
 * </pre>
 *
 * You should make changes using the <code>NRTManager</code>; if you later need to obtain
 * a searcher reflecting those changes:
 *
 * <pre>
 *   // ... or updateDocument, deleteDocuments, etc:
 *   long gen = manager.addDocument(...);
 *   
 *   // Returned searcher is guaranteed to reflect the just added document
 *   IndexSearcher searcher = manager.get(gen);
 *   try {
 *     // Use searcher to search...
 *   } finally {
 *     manager.release(searcher);
 *   }
 * </pre>
 *
 *
 * When you are done be sure to close both the manager and the reopen thrad:
 * <pre> 
 *   reopenThread.close();       
 *   manager.close();
 * </pre>
 * 
 * @lucene.experimental
 */

public class NRTManagerReopenThread extends Thread implements NRTManager.WaitingListener, Closeable {
  
  private final NRTManager manager;
  private final long targetMaxStaleNS;
  private final long targetMinStaleNS;
  private boolean finish;
  private long waitingGen;

  /**
   * Create NRTManagerReopenThread, to periodically reopen the NRT searcher.
   *
   * @param targetMaxStaleSec Maximum time until a new
   *        reader must be opened; this sets the upper bound
   *        on how slowly reopens may occur
   *
   * @param targetMinStaleSec Mininum time until a new
   *        reader can be opened; this sets the lower bound
   *        on how quickly reopens may occur, when a caller
   *        is waiting for a specific indexing change to
   *        become visible.
   */

  public NRTManagerReopenThread(NRTManager manager, double targetMaxStaleSec, double targetMinStaleSec) {
    if (targetMaxStaleSec < targetMinStaleSec) {
      throw new IllegalArgumentException("targetMaxScaleSec (= " + targetMaxStaleSec + ") < targetMinStaleSec (=" + targetMinStaleSec + ")");
    }
    this.manager = manager;
    this.targetMaxStaleNS = (long) (1000000000*targetMaxStaleSec);
    this.targetMinStaleNS = (long) (1000000000*targetMinStaleSec);
    manager.addWaitingListener(this);
  }

  public synchronized void close() {
    //System.out.println("NRT: set finish");
    manager.removeWaitingListener(this);
    this.finish = true;
    notify();
    try {
      join();
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    }
  }

  public synchronized void waiting(long targetGen) {
    waitingGen = Math.max(waitingGen, targetGen);
    notify();
    //System.out.println(Thread.currentThread().getName() + ": force wakeup waitingGen=" + waitingGen + " applyDeletes=" + applyDeletes);
  }

  @Override
  public void run() {
    // TODO: maybe use private thread ticktock timer, in
    // case clock shift messes up nanoTime?
    long lastReopenStartNS = System.nanoTime();

    //System.out.println("reopen: start");
    try {
      while (true) {

        boolean hasWaiting = false;

        synchronized(this) {
          // TODO: try to guestimate how long reopen might
          // take based on past data?

          while (!finish) {
            //System.out.println("reopen: cycle");

            // True if we have someone waiting for reopen'd searcher:
            hasWaiting = waitingGen > manager.getCurrentSearchingGen();
            final long nextReopenStartNS = lastReopenStartNS + (hasWaiting ? targetMinStaleNS : targetMaxStaleNS);

            final long sleepNS = nextReopenStartNS - System.nanoTime();

            if (sleepNS > 0) {
              //System.out.println("reopen: sleep " + (sleepNS/1000000.0) + " ms (hasWaiting=" + hasWaiting + ")");
              try {
                wait(sleepNS/1000000, (int) (sleepNS%1000000));
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                //System.out.println("NRT: set finish on interrupt");
                finish = true;
                break;
              }
            } else {
              break;
            }
          }

          if (finish) {
            //System.out.println("reopen: finish");
            return;
          }
          //System.out.println("reopen: start hasWaiting=" + hasWaiting);
        }

        lastReopenStartNS = System.nanoTime();
        try {
          //final long t0 = System.nanoTime();
          manager.maybeRefresh();
          //System.out.println("reopen took " + ((System.nanoTime()-t0)/1000000.0) + " msec");
        } catch (IOException ioe) {
          //System.out.println(Thread.currentThread().getName() + ": IOE");
          //ioe.printStackTrace();
          throw new RuntimeException(ioe);
        }
      }
    } catch (Throwable t) {
      //System.out.println("REOPEN EXC");
      //t.printStackTrace(System.out);
      throw new RuntimeException(t);
    }
  }
}
