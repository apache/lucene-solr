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
package org.apache.solr.logging.log4j;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.solr.logging.LogWatcher;


public final class EventAppender extends AppenderSkeleton {

  final LogWatcher<LoggingEvent> watcher;

  public EventAppender(LogWatcher<LoggingEvent> framework) {
    this.watcher = framework;
  }

  @Override
  public void append( LoggingEvent event )
  {
    watcher.add(event,event.timeStamp);
  }

  @Override
  public void close() {
    watcher.reset();
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }
}