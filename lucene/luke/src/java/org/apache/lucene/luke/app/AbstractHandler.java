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

package org.apache.lucene.luke.app;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.luke.util.LoggerFactory;

/** Abstract handler class */
public abstract class AbstractHandler<T extends Observer> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private List<T> observers = new ArrayList<>();

  public void addObserver(T observer) {
    observers.add(observer);
    if (log.isDebugEnabled()) {
      log.debug("{} registered.", observer.getClass().getName());
    }
  }

  void notifyObservers() {
    for (T observer : observers) {
      notifyOne(observer);
    }
  }

  protected abstract void notifyOne(T observer);

}
