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

package org.apache.solr.cloud.autoscaling;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.solr.core.CoreContainer;

public class AutoScaling {

  public enum EventType {
    NODEADDED,
    NODELOST,
    REPLICALOST,
    MANUAL,
    SCHEDULED,
    SEARCHRATE,
    INDEXRATE
  }

  public enum TriggerStage {
    WAITING,
    STARTED,
    ABORTED,
    SUCCEEDED,
    FAILED,
    BEFORE_ACTION,
    AFTER_ACTION
  }

  public static interface TriggerEvent<T extends Trigger> {
    public T getSource();

    public long getEventNanoTime();

    public void setContext(Map<String, Object> context);

    public Map<String, Object> getContext();
  }

  public static interface TriggerListener<E extends TriggerEvent<? extends Trigger>> {
    public void triggerFired(E event);
  }

  public static class HttpCallbackListener implements TriggerListener {
    @Override
    public void triggerFired(TriggerEvent event) {

    }
  }

  /**
   * Interface for a Solr trigger. Each trigger implements Runnable and Closeable interface. A trigger
   * is scheduled using a {@link java.util.concurrent.ScheduledExecutorService} so it is executed as
   * per a configured schedule to check whether the trigger is ready to fire. The {@link #setListener(TriggerListener)}
   * method should be used to set a callback listener which is fired by implementation of this class whenever
   * ready.
   * <p>
   * As per the guarantees made by the {@link java.util.concurrent.ScheduledExecutorService} a trigger
   * implementation is only ever called sequentially and therefore need not be thread safe. However, it
   * is encouraged that implementations be immutable with the exception of the associated listener
   * which can be get/set by a different thread than the one executing the trigger. Therefore, implementations
   * should use appropriate synchronization around the listener.
   *
   * @param <E> the {@link TriggerEvent} which is handled by this Trigger
   */
  public static interface Trigger<E extends TriggerEvent<? extends Trigger>> extends Closeable, Runnable {
    public String getName();

    public EventType getEventType();

    public boolean isEnabled();

    public Map<String, Object> getProperties();

    public int getWaitForSecond();

    public List<TriggerAction> getActions();

    public void setListener(TriggerListener<E> listener);

    public TriggerListener<E> getListener();

    public boolean isClosed();
  }

  public static class TriggerFactory implements Closeable {

    private final CoreContainer coreContainer;

    private boolean isClosed = false;

    public TriggerFactory(CoreContainer coreContainer) {
      this.coreContainer = coreContainer;
    }

    public synchronized Trigger create(EventType type, String name, Map<String, Object> props) {
      if (isClosed) {
        throw new AlreadyClosedException("TriggerFactory has already been closed, cannot create new triggers");
      }
      switch (type) {
        case NODEADDED:
          return new NodeAddedTrigger(name, props, coreContainer);
        case NODELOST:
          return new NodeLostTrigger(name, props, coreContainer);
        default:
          throw new IllegalArgumentException("Unknown event type: " + type + " in trigger: " + name);
      }
    }

    @Override
    public void close() throws IOException {
      synchronized (this) {
        isClosed = true;
      }
    }
  }
}
