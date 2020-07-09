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
package org.apache.solr.common;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeTracker {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static final Map<String,TimeTracker> CLOSE_TIMES = new ConcurrentHashMap<>();
  
  private final long startTime;

  private volatile long doneTime;

  private volatile Object trackedObject;
  
  private final List<TimeTracker> children = Collections.synchronizedList(new ArrayList<>());

  private final Class<? extends Object> clazz;

  private final StringBuilder label = new StringBuilder();
  
  private final int depth;
  
  public TimeTracker(Object object, String label) {
    this(object, label, 1);
  }

  private TimeTracker(Object object, String label, int i) {
    this.trackedObject = object;
    this.clazz = object == null ? null : object.getClass();
    this.startTime = System.nanoTime();
    this.label.append(label);
    this.depth = i;
    
    if (depth <= 1) {
      CLOSE_TIMES.put((object != null ? object.hashCode() : 0) + "_" + label.hashCode(), this);
    }
  }

  public void doneClose() {
    if (log.isDebugEnabled()) {
      log.debug("doneClose() - start");
    }

    doneTime = System.nanoTime();
    //System.out.println("done close: " + trackedObject + " "  + label + " " + getElapsedNS());

    if (log.isDebugEnabled()) {
      log.debug("doneClose() - end");
    }
  }
  
  public void doneClose(String label) {
    if (log.isDebugEnabled()) {
      log.debug("doneClose(String label={}) - start", label);
    }

   // if (theObject == null) return;
   // log.info(theObject instanceof String ? theObject.getClass().getName() : theObject.toString() +  " was closed");
    doneTime = System.nanoTime();
    
    //this.label.append(label);
    StringBuilder spacer = new StringBuilder();
    for (int i =0; i < depth; i++) {
      spacer.append(' ');
    }
    
    String extra = "";
    if (label.trim().length() != 0) {
      extra = label + "->";
    }
    
    this.label.insert(0, spacer.toString() + extra);

    if (log.isDebugEnabled()) {
      log.debug("doneClose(String) - end");
    }
  }

  public long getElapsedNS() {
    if (log.isDebugEnabled()) {
      log.debug("getElapsedNS() - start");
    }

    long returnlong = getElapsedNS(startTime, doneTime);
    if (log.isDebugEnabled()) {
      log.debug("getElapsedNS() - end");
    }
    return returnlong;
  }

  public TimeTracker startSubClose(String label) {
    if (log.isDebugEnabled()) {
      log.debug("startSubClose(String label={}) - start", label);
    }

    TimeTracker subTracker = new TimeTracker(null, label, depth+1);
    children.add(subTracker);

    if (log.isDebugEnabled()) {
      log.debug("startSubClose(String) - end");
    }
    return subTracker;
  }
  
  public TimeTracker startSubClose(Object object) {
    if (log.isDebugEnabled()) {
      log.debug("startSubClose(Object object={}) - start", object);
    }

    TimeTracker subTracker = new TimeTracker(object, object.getClass().getName(), depth+1);
    children.add(subTracker);

    if (log.isDebugEnabled()) {
      log.debug("startSubClose(Object) - end");
    }
    return subTracker;
  }
  
  public void printCloseTimes() {
    if (log.isDebugEnabled()) {
      log.debug("printCloseTimes() - start");
    }

    String times = getCloseTimes();
    if (times.trim().length()>0) {
      System.out.println("\n------\n" +  times + "\n------\n");
    }

    if (log.isDebugEnabled()) {
      log.debug("printCloseTimes() - end");
    }
  }

  public String getCloseTimes() {
    if (log.isDebugEnabled()) {
      log.debug("getCloseTimes() - start");
    }
    
    if ( getElapsedMS() <=0) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
//    if (trackedObject != null) {
//      if (trackedObject instanceof String) {
//        sb.append(label + trackedObject.toString() + " " + getElapsedMS() + "ms");
//      } else {
//        sb.append(label + trackedObject.getClass().getName() + " " + getElapsedMS() + "ms");
//      }
//    } else {
      sb.append(label + " " + getElapsedMS() + "ms");
//    }
     // sb.append("[\n");
    synchronized (children) {
     // sb.append(" children(" + children.size() + ")");
      for (TimeTracker entry : children) {
        if (entry.getElapsedMS() >= 0) {
          sb.append("\n");
          for (int i = 0; i < depth; i++) {
            sb.append(' ');
          }

          sb.append(entry.getCloseTimes());
        }
      }
    }
    //sb.append("]\n");
    String returnString = sb.toString();
    if (log.isDebugEnabled()) {
      log.debug("getCloseTimes() - end");
    }
    return returnString;

    // synchronized (startTimes) {
    // synchronized (endTimes) {
    // for (String label : startTimes.keySet()) {
    // long startTime = startTimes.get(label);
    // long endTime = endTimes.get(label);
    // System.out.println(" -" + label + ": " + getElapsedMS(startTime, endTime) + "ms");
    // }
    // }
    // }
  }
  
  public String toString() {
    if (label != null) {
      return (children.size() > 0 ? ":" : "") + label + " " + getElapsedMS() + "ms";
    } else if (trackedObject != null) {
      if (trackedObject instanceof String) {
        return trackedObject.toString() + "-> " + getElapsedMS() + "ms";
      } else {
        return trackedObject.getClass().getSimpleName() + "--> " + getElapsedMS() + "ms";
      }
    }
    return "*InternalError*";
  }

  private long getElapsedNS(long startTime, long doneTime) {
    if (log.isDebugEnabled()) {
      log.debug("getElapsedNS(long startTime={}, long doneTime={}) - start", startTime, doneTime);
    }

    long returnlong = doneTime - startTime;
    if (log.isDebugEnabled()) {
      log.debug("getElapsedNS(long, long) - end");
    }
    return returnlong;
  }

  public Class<? extends Object> getClazz() {
    return clazz;
  }
  
  public long getElapsedMS() {
    if (log.isDebugEnabled()) {
      log.debug("getElapsedMS() - start");
    }

    long ms = getElapsedMS(startTime, doneTime);
    long returnlong = ms < 0 ? 0 : ms;
    if (log.isDebugEnabled()) {
      log.debug("getElapsedMS() - end");
    }
    return returnlong;
  }

  public long getElapsedMS(long startTime, long doneTime) {
    if (log.isDebugEnabled()) {
      log.debug("getElapsedMS(long startTime={}, long doneTime={}) - start", startTime, doneTime);
    }

    long returnlong = TimeUnit.MILLISECONDS.convert(doneTime - startTime, TimeUnit.NANOSECONDS);
    if (log.isDebugEnabled()) {
      log.debug("getElapsedMS(long, long) - end");
    }
    return returnlong;
  }


}