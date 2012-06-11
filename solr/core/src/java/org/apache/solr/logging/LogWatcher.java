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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.core.CoreContainer;

/**
 * A Class to monitor Logging events and hold N events in memory
 * 
 * This is abstract so we can support both JUL and Log4j (and other logging platforms)
 */
public abstract class LogWatcher<E> {
  
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
  
  public abstract void registerListener(ListenerConfig cfg, CoreContainer container);

  public void reset() {
    history.clear();
    last = -1;
  }
}