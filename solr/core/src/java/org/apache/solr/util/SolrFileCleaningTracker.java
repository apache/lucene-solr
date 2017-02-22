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
package org.apache.solr.util;

import java.io.File;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.FileCleaningTracker;
import org.apache.commons.io.FileDeleteStrategy;

public class SolrFileCleaningTracker extends FileCleaningTracker {

  ReferenceQueue<Object> q = new ReferenceQueue<>();

  final Collection<Tracker> trackers = Collections.synchronizedSet(new HashSet<Tracker>());

  final List<String> deleteFailures = Collections.synchronizedList(new ArrayList<String>());

  volatile boolean exitWhenFinished = false;

  Thread reaper;

  public void track(final File file, final Object marker) {
    track(file, marker, null);
  }

  public void track(final File file, final Object marker, final FileDeleteStrategy deleteStrategy) {
    if (file == null) {
      throw new NullPointerException("The file must not be null");
    }
    addTracker(file.getPath(), marker, deleteStrategy);
  }

  public void track(final String path, final Object marker) {
    track(path, marker, null);
  }

  public void track(final String path, final Object marker, final FileDeleteStrategy deleteStrategy) {
    if (path == null) {
      throw new NullPointerException("The path must not be null");
    }
    addTracker(path, marker, deleteStrategy);
  }

  private synchronized void addTracker(final String path, final Object marker,
      final FileDeleteStrategy deleteStrategy) {
    if (exitWhenFinished) {
      throw new IllegalStateException("No new trackers can be added once exitWhenFinished() is called");
    }
    if (reaper == null) {
      reaper = new Reaper();
      reaper.start();
    }
    trackers.add(new Tracker(path, deleteStrategy, marker, q));
  }

  public int getTrackCount() {
    return trackers.size();
  }

  public List<String> getDeleteFailures() {
    return deleteFailures;
  }

  public synchronized void exitWhenFinished() {
    // synchronized block protects reaper
    exitWhenFinished = true;
    if (reaper != null) {
      synchronized (reaper) {
        reaper.interrupt();
        try {
          reaper.join();
        } catch (InterruptedException e) { 
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private final class Reaper extends Thread {
    Reaper() {
      super("MultiPart Upload Tmp File Reaper");
      setDaemon(true);
    }

    @Override
    public void run() {
      while (exitWhenFinished == false || trackers.size() > 0) {
        try {
          // Wait for a tracker to remove.
          final Tracker tracker = (Tracker) q.remove(); // cannot return null
          trackers.remove(tracker);
          if (!tracker.delete()) {
            deleteFailures.add(tracker.getPath());
          }
          tracker.clear();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  private static final class Tracker extends PhantomReference<Object> {

    private final String path;

    private final FileDeleteStrategy deleteStrategy;

    Tracker(final String path, final FileDeleteStrategy deleteStrategy, final Object marker,
        final ReferenceQueue<? super Object> queue) {
      super(marker, queue);
      this.path = path;
      this.deleteStrategy = deleteStrategy == null ? FileDeleteStrategy.NORMAL : deleteStrategy;
    }

    public String getPath() {
      return path;
    }

    public boolean delete() {
      return deleteStrategy.deleteQuietly(new File(path));
    }
  }

}