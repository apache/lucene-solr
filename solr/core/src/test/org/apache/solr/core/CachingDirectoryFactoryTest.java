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
package org.apache.solr.core;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachingDirectoryFactoryTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private Map<String,Tracker> dirs = new HashMap<>();
  private volatile boolean stop = false;
  
  private static class Tracker {
    String path;
    AtomicInteger refCnt = new AtomicInteger(0);
    Directory dir;
  }
  
  @Test
  public void stressTest() throws Exception {
    doStressTest(new RAMDirectoryFactory());
    doStressTest(new ByteBuffersDirectoryFactory());
  }
  
  private void doStressTest(final CachingDirectoryFactory df) throws Exception {
    List<Thread> threads = new ArrayList<>();
    int threadCount = 11;
    for (int i = 0; i < threadCount; i++) {
      Thread getDirThread = new GetDirThread(df);
      threads.add(getDirThread);
      getDirThread.start();
    }
    
    for (int i = 0; i < 4; i++) {
      Thread releaseDirThread = new ReleaseDirThread(df);
      threads.add(releaseDirThread);
      releaseDirThread.start();
    }
    
    for (int i = 0; i < 2; i++) {
      Thread incRefThread = new IncRefThread(df);
      threads.add(incRefThread);
      incRefThread.start();
    }

    Thread.sleep(TEST_NIGHTLY ? 30000 : 8000);
    
    Thread closeThread = new Thread() {
      public void run() {
        try {
          df.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
    closeThread.start();
    
    
    stop = true;
    
    for (Thread thread : threads) {
      thread.join();
    }
    
    
    // do any remaining releases
    synchronized (dirs) {
      int sz = dirs.size();
      if (sz > 0) {
        for (Tracker tracker : dirs.values()) {
          int cnt = tracker.refCnt.get();
          for (int i = 0; i < cnt; i++) {
            tracker.refCnt.decrementAndGet();
            df.release(tracker.dir);
          }
        }
      }
      
    }
    
    closeThread.join();

  }
  
  private class ReleaseDirThread extends Thread {
    Random random;
    private CachingDirectoryFactory df;
    
    public ReleaseDirThread(CachingDirectoryFactory df) {
      this.df = df;
    }
    
    @Override
    public void run() {
      random = random();
      while (!stop) {
        try {
          Thread.sleep(random.nextInt(50) + 1);
        } catch (InterruptedException e1) {
          throw new RuntimeException(e1);
        }
        
        synchronized (dirs) {
          int sz = dirs.size();
          List<Tracker> dirsList = new ArrayList<>();
          dirsList.addAll(dirs.values());
          if (sz > 0) {
            Tracker tracker = dirsList.get(Math.min(dirsList.size() - 1,
                random.nextInt(sz + 1)));
            try {
              if (tracker.refCnt.get() > 0) {
                if (random.nextInt(10) > 7) {
                  df.doneWithDirectory(tracker.dir);
                }
                if (random.nextBoolean()) {
                  df.remove(tracker.dir);
                } else {
                  df.remove(tracker.path);
                }
                tracker.refCnt.decrementAndGet();
                df.release(tracker.dir);
              }
            } catch (Exception e) {
              throw new RuntimeException("path:" + tracker.path + "ref cnt:" + tracker.refCnt, e);
            }
          }
        }
        
      }
    }
  }
  
  private class GetDirThread extends Thread {
    Random random;
    private CachingDirectoryFactory df;
    
    public GetDirThread(CachingDirectoryFactory df) {
      this.df = df;
    }
    
    @Override
    public void run() {
      random = random();
      while (!stop) {
        try {
          Thread.sleep(random.nextInt(350) + 1);
        } catch (InterruptedException e1) {
          throw new RuntimeException(e1);
        }
        try {
          String path;
          if (random.nextBoolean()) {
            path = "path" + random.nextInt(20);
          } else {
            if (random.nextBoolean()) {
              path = "path" + random.nextInt(20) + "/" + random.nextInt(20);
            } else {
              path = "path" + random.nextInt(20) + "/" + random.nextInt(20) + "/" + random.nextInt(20);
            }
          }
          synchronized (dirs) {
            Tracker tracker = dirs.get(path);
            if (tracker == null) {
              tracker = new Tracker();
              tracker.path = path;
              tracker.dir = df.get(path, DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_SINGLE);
              dirs.put(path, tracker);
            } else {
              tracker.dir = df.get(path, DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_SINGLE);
            }
            tracker.refCnt.incrementAndGet();
          }
          
        } catch (AlreadyClosedException e) {
          log.warn("Cannot get dir, factory is already closed");
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
  
  private class IncRefThread extends Thread {
    Random random;
    private CachingDirectoryFactory df;
    
    public IncRefThread(CachingDirectoryFactory df) {
      this.df = df;
    }
    
    @Override
    public void run() {
      random = random();
      while (!stop) {
        try {
          Thread.sleep(random.nextInt(300) + 1);
        } catch (InterruptedException e1) {
          throw new RuntimeException(e1);
        }
        
        String path = "path" + random.nextInt(20);
        synchronized (dirs) {
          Tracker tracker = dirs.get(path);
          
          if (tracker != null && tracker.refCnt.get() > 0) {
            try {
              df.incRef(tracker.dir);
            } catch (SolrException e) {
              log.warn("", e);
              continue;
            }
            
            tracker.refCnt.incrementAndGet();
          }
        }
        
      }
    }
  }
  
}
