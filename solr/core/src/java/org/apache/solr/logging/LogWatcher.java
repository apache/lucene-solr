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
package org.apache.solr.logging;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.logging.jul.JulWatcher;
import org.apache.solr.logging.log4j2.Log4j2Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Class to monitor Logging events and hold N events in memory
 * 
 * This is abstract so we can support both JUL and Log4j2 (and other logging platforms)
 */
public abstract class LogWatcher<E> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  protected CircularList<E> history;
  protected long last = -1;
  
  /**
   * @return The implementation name
   */
  public abstract String getName();
  
  /**
   * @return The valid level names for this framework
   */
  public abstract List<String> getAllLevels();
  
  /**
   * Sets the log level within this framework
   */
  public abstract void setLogLevel(String category, String level);
  
  /**
   * @return all registered loggers
   */
  public abstract Collection<LoggerInfo> getAllLoggers();
  
  public abstract void setThreshold(String level);
  public abstract String getThreshold();

  public void add(E event, long timstamp) {
    history.add(event);
    last = timstamp;
  }
  
  public long getLastEvent() {
    return last;
  }
  
  public int getHistorySize() {
    return (history==null) ? -1 : history.getBufferSize();
  }
  
  public SolrDocumentList getHistory(long since, AtomicBoolean found) {
    if(history==null) {
      return null;
    }
    
    SolrDocumentList docs = new SolrDocumentList();
    Iterator<E> iter = history.iterator();
    while(iter.hasNext()) {
      E e = iter.next();
      long ts = getTimestamp(e);
      if(ts == since) {
        if(found!=null) {
          found.set(true);
        }
      }
      if(ts>since) {
        docs.add(toSolrDocument(e));
      }
    }
    docs.setNumFound(docs.size()); // make it not look too funny
    return docs;
  }
  
  public abstract long getTimestamp(E event);
  public abstract SolrDocument toSolrDocument(E event);
  
  public abstract void registerListener(ListenerConfig cfg);

  public void reset() {
    history.clear();
    last = -1;
  }

  /**
   * Create and register a LogWatcher.
   *
   * JUL and Log4j watchers are supported out-of-the-box.  You can register your own
   * LogWatcher implementation via the plugins architecture
   *
   * @param config a LogWatcherConfig object, containing the configuration for this LogWatcher.
   * @param loader a SolrResourceLoader, to be used to load plugin LogWatcher implementations.
   *               Can be null if
   *
   * @return a LogWatcher configured for the container's logging framework
   */
  @SuppressWarnings({"rawtypes"})
  public static LogWatcher newRegisteredLogWatcher(LogWatcherConfig config, SolrResourceLoader loader) {

    if (!config.isEnabled()) {
      log.debug("A LogWatcher is not enabled");
      return null;
    }

    LogWatcher logWatcher = createWatcher(config, loader);

    if (logWatcher != null) {
      if (config.getWatcherSize() > 0) {
        if (log.isDebugEnabled()) {
          log.debug("Registering Log Listener [{}]", logWatcher.getName());
        }
        logWatcher.registerListener(config.asListenerConfig());
      }
    }

    return logWatcher;
  }

  @SuppressWarnings({"rawtypes"})
  private static LogWatcher createWatcher(LogWatcherConfig config, SolrResourceLoader loader) {

    String fname = config.getLoggingClass();
    String slf4jImpl;

    try {
      slf4jImpl = StaticLoggerBinder.getSingleton().getLoggerFactoryClassStr();
      log.debug("SLF4J impl is {}", slf4jImpl);
      if (fname == null) {
        if ("org.apache.logging.slf4j.Log4jLoggerFactory".equals(slf4jImpl)) {
          fname = "Log4j2";
        } else if (slf4jImpl.indexOf("JDK") > 0) {
          fname = "JUL";
        }
      }
    }
    catch (Throwable e) {
      log.warn("Unable to read SLF4J version.  LogWatcher will be disabled: ", e);
      if (e instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) e;
      }
      return null;
    }

    if (fname == null) {
      log.debug("No LogWatcher configured");
      return null;
    }

    if ("JUL".equalsIgnoreCase(fname)) {
      return new JulWatcher(slf4jImpl);
    }
    if ("Log4j2".equals(fname)) {
      return new Log4j2Watcher();
    }

    try {
      return loader != null ? loader.newInstance(fname, LogWatcher.class) : null;
    }
    catch (Throwable e) {
      log.warn("Unable to load LogWatcher {}: {}", fname, e);
      if (e instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) e;
      }
    }

    return null;
  }
}