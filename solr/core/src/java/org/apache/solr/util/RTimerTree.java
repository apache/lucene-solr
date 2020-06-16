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

import java.util.Map;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

/** A recursive timer.
 *
 * RTimerTree's are started automatically when instantiated; sub-timers are also
 * started automatically when created.
 */
public class RTimerTree extends RTimer {

  protected SimpleOrderedMap<RTimerTree> children;

  public RTimerTree() {
    children = new SimpleOrderedMap<>();
  }

  /** Recursively stop timer and sub timers */
  @Override
  public double stop() {
    double time = super.stop();

    for( Map.Entry<String,RTimerTree> entry : children ) {
      RTimer child = entry.getValue();
      if(child.state == STARTED || child.state == PAUSED)
        child.stop();
    }
    return time;
  }

  protected RTimerTree newTimer() {
    return new RTimerTree();
  }

  /**
   * Returns a subtimer given its name.
   * If the subtimer did not exist a new subtimer will be started and returned,
   * otherwise an existing subtimer will be returned as-is.
   */
  public RTimerTree sub(String desc) {
    RTimerTree child = children.get( desc );
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

  @SuppressWarnings({"rawtypes"})
  public NamedList asNamedList() {
    NamedList<Object> m = new SimpleOrderedMap<>();
    m.add( "time", getTime() );
    if( children.size() > 0 ) {
      for( Map.Entry<String, RTimerTree> entry : children ) {
        m.add( entry.getKey(), entry.getValue().asNamedList() );
      }
    }
    return m;
  }

  /**
   * Manipulating this map may have undefined results.
   */
  public SimpleOrderedMap<RTimerTree> getChildren()
  {
    return children;
  }
}
