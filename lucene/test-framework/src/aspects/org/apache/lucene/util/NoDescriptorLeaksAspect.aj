package org.apache.lucene.util;

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

import com.carrotsearch.aspects.*;
import com.carrotsearch.aspects.Tracker.TrackingInfo;
import com.carrotsearch.aspects.Tracker.TrackingStats;

import java.io.*;
import java.util.Locale;

public aspect NoDescriptorLeaksAspect {
  void around(): execution(void org.apache.lucene.util.TestRuleAspectJAspects.pointcutBeforeSuite(..)) {
    Tracker.startTracking();
    proceed();
  }
  
  void around(): execution(void org.apache.lucene.util.TestRuleAspectJAspects.pointcutAfterSuite(..)) {
    TrackingStats stats = Tracker.snapshot();

    // Close any unclosed resources.
    for (Object o : stats.open.keySet()) {
      try {
        ((Closeable) o).close();
      } catch (IOException e) {
        // Ignore if we cannot close it.
      }
    }

    // Disable tracking and reset tracking buffers.
    Tracker.endTracking();
    Tracker.reset();

    // Verify assertions.
    System.out.println(String.format(Locale.ENGLISH, 
        "Tracked %d objects, %d closed, %d open.",
        stats.closed.size() + stats.open.size(),
        stats.closed.size(),
        stats.open.size()));
    
    for (TrackingInfo i : stats.closed.values()) {
      System.out.println(i);
    }
    for (TrackingInfo i : stats.open.values()) {
      System.out.println(i);
    }
    
    if (!stats.open.isEmpty()) {
      throw new AssertionError("Unclosed file handles: " + stats.open.values());
    }

    proceed();
  }  
}
