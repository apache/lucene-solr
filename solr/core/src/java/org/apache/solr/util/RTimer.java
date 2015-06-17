package org.apache.solr.util;

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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

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
  protected TimerImpl timerImpl;
  protected double time;
  protected double culmTime;
  protected SimpleOrderedMap<RTimer> children;

  protected interface TimerImpl {
    void start();
    double elapsed();
  }

  private class NanoTimeTimerImpl implements TimerImpl {
    private long start;
    public void start() {
      start = System.nanoTime();
    }
    public double elapsed() {
      return TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    }
  }

  protected TimerImpl newTimerImpl() {
    return new NanoTimeTimerImpl();
  }

  protected RTimer newTimer() {
    return new RTimer();
  }

  public RTimer() {
    time = 0;
    culmTime = 0;
    children = new SimpleOrderedMap<>();
    timerImpl = newTimerImpl();
    timerImpl.start();
    state = STARTED;
  }

  /** Recursively stop timer and sub timers */
  public double stop() {
    assert state == STARTED || state == PAUSED;
    time = culmTime;
    if(state == STARTED) 
      time += timerImpl.elapsed();
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
    culmTime += timerImpl.elapsed();
    state = PAUSED;
  }
  
  public void resume() {
    if(state == STARTED)
      return;
    assert state == PAUSED;
    state = STARTED;
    timerImpl.start();
  }

  /** Get total elapsed time for this timer. */
  public double getTime() {
    if (state == STOPPED) return time;
    else if (state == PAUSED) return culmTime;
    else {
      assert state == STARTED;
      return culmTime + timerImpl.elapsed();
    }
 }

  /** Create new subtimer with given name
   *
   * Subtimer will be started.
   */
  public RTimer sub(String desc) {
    RTimer child = children.get( desc );
    if( child == null ) {
      child = newTimer();
      children.add(desc, child);
    }
    return child;
  }

  @Override
  public String toString() {
    return asNamedList().toString();
  }

  public NamedList asNamedList() {
    NamedList<Object> m = new SimpleOrderedMap<>();
    m.add( "time", getTime() );
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
}
