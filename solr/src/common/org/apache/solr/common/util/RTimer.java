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

package org.apache.solr.common.util;

import java.lang.System;
import java.lang.Thread;
import java.util.*;

/** A recursive timer.
 * 
 * RTimers are started automatically when instantiated; subtimers are also
 * started automatically when created.
 *
 * @since solr 1.3
 *
 */
public class RTimer {

  public static final int STARTED = 0;
  public static final int STOPPED = 1;
  public static final int PAUSED = 2;

  protected int state;
  protected double startTime;
  protected double time;
  protected double culmTime;
  protected SimpleOrderedMap<RTimer> children;

  public RTimer() {
    time = 0;
    culmTime = 0;
    children = new SimpleOrderedMap<RTimer>();
    startTime = now();
    state = STARTED;
  }

  /** Get current time
   *
   * May override to implement a different timer (CPU time, etc).
   */
  protected double now() { return System.currentTimeMillis(); }

  /** Recursively stop timer and sub timers */
  public double stop() {
    assert state == STARTED || state == PAUSED;
    time = culmTime;
    if(state == STARTED) 
      time += now() - startTime;
    state = STOPPED;
    
    for( Map.Entry<String,RTimer> entry : children ) {
      RTimer child = entry.getValue();
      if(child.state == STARTED || child.state == PAUSED) 
        child.stop();
    }
    return time;
  }

  public void pause() {
    assert state == STARTED;
    culmTime += now() - startTime;
    state = PAUSED;
  }
  
  public void resume() {
    if(state == STARTED)
      return;
    assert state == PAUSED;
    state = STARTED;
    startTime = now();
  }

  /** Get total elapsed time for this timer.
   *
   * Timer must be STOPped.
   */
  public double getTime() {
    assert state == STOPPED;
    return time;
  }

  /** Create new subtimer with given name
   *
   * Subtimer will be started.
   */
  public RTimer sub(String desc) {
    RTimer child = children.get( desc );
    if( child == null ) {
      child = new RTimer();
      children.add(desc, child);
    }
    return child;
  }

  @Override
  public String toString() {
    return asNamedList().toString();
  }

  public NamedList asNamedList() {
    NamedList<Object> m = new SimpleOrderedMap<Object>();
    m.add( "time", time );
    if( children.size() > 0 ) {
      for( Map.Entry<String, RTimer> entry : children ) {
        m.add( entry.getKey(), entry.getValue().asNamedList() );
      }
    }
    return m;
  }
  
  /**
   * Manipulating this map may have undefined results.
   */
  public SimpleOrderedMap<RTimer> getChildren()
  {
    return children;
  }

  /*************** Testing *******/
  public static void main(String []argv) throws InterruptedException {
    RTimer rt = new RTimer(), subt, st;
    Thread.sleep(100);

    subt = rt.sub("sub1");
    Thread.sleep(50);
    st = subt.sub("sub1.1");
    st.resume();
    Thread.sleep(10);
    st.pause();
    Thread.sleep(50);
    st.resume();
    Thread.sleep(10);
    st.pause();
    subt.stop();
    rt.stop();

    System.out.println( rt.toString());
  }
}
