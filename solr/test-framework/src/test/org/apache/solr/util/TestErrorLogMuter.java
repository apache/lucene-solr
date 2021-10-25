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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.SolrTestCaseJ4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.impl.MutableLogEvent;

import static org.hamcrest.core.StringContains.containsString;

@SuppressForbidden(reason="We need to use log4J2 classes directly to check that the ErrorLogMuter is working")
public class TestErrorLogMuter extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @LogLevel("=WARN")
  public void testErrorMutingRegex() throws Exception {

    final ListAppender rootSanityCheck = new ListAppender("sanity-checker");
    try {
      LoggerContext.getContext(false).getConfiguration().getRootLogger().addAppender(rootSanityCheck, Level.WARN, null);
      LoggerContext.getContext(false).updateLoggers();

      try (ErrorLogMuter x = ErrorLogMuter.regex("eRrOr\\s+Log")) {
        assertEquals(0, x.getCount());

        log.error("This is an {} Log msg that x should be muted", "eRrOr");
        assertEquals(1, x.getCount());
        
        log.error("This is an {} Log msg that x should not mute", "err");
        log.warn("This is an warn message, mentioning 'eRrOr Log', that should also not be muted");
        assertEquals(1, x.getCount());

        log.error("This {} because of the {} msg", "error", "thowable",
                  new Exception("outer", new Exception("inner eRrOr Log throwable")));
        assertEquals(2, x.getCount());

      }
    } finally {
      LoggerContext.getContext(false).getConfiguration().getRootLogger().removeAppender(rootSanityCheck.getName());
      LoggerContext.getContext(false).updateLoggers();
    }
    
    // the root loger should not have seen anything that was muted...
    assertEquals(2, rootSanityCheck.getEvents().size());
    assertThat(rootSanityCheck.getEvents().get(0).getMessage().getFormattedMessage(), containsString("should not mute"));
    assertThat(rootSanityCheck.getEvents().get(1).getMessage().getFormattedMessage(), containsString("also not be muted"));
  }

  @LogLevel("=WARN") 
  public void testMultipleMuters() throws Exception {

    // Add a ListAppender to our ROOT logger so we can sanity check what log messages it gets
    final ListAppender rootSanityCheck = new ListAppender("sanity-checker");
    try {
      LoggerContext.getContext(false).getConfiguration().getRootLogger().addAppender(rootSanityCheck, Level.WARN, null);
      LoggerContext.getContext(false).updateLoggers();
      
      // sanity check that muters "mute" in the order used...
      // (If this fails, then it means log4j has changed the precedence order it uses when addFilter is called,
      // if that happens, we'll need to change our impl to check if an impl of some special "container" Filter subclass we create.
      // is in the list of ROOT filters -- if not add one, and then "add" the specific muting filter to our "container" Filter)
      try (ErrorLogMuter x = ErrorLogMuter.substring("xxx");
           ErrorLogMuter y = ErrorLogMuter.regex(Pattern.compile("YYY", Pattern.CASE_INSENSITIVE));
           ErrorLogMuter z = ErrorLogMuter.regex("(xxx|yyy)")) {
           
        
        log.error("xx{}  ", "x");
        log.error("    yyy");
        log.error("xxx yyy");
        log.warn("xxx  yyy");
        log.error("abc", new Exception("yyy"));
        
        assertEquals(2, x.getCount()); // x is first, so it swallows up the "x + y" message
        assertEquals(2, y.getCount()); // doesn't get anything x already got
        assertEquals(0, z.getCount()); // doesn't get anything x already got
      }
    } finally {
      LoggerContext.getContext(false).getConfiguration().getRootLogger().removeAppender(rootSanityCheck.getName());
      LoggerContext.getContext(false).updateLoggers();
    }

    assertEquals(1, rootSanityCheck.getEvents().size()); // our warning
  }

  @LogLevel("=WARN") 
  public void testDeprecatedBaseClassMethods() throws Exception {
    
    // Add a ListAppender to our ROOT logger so we can sanity check what log messages it gets
    final ListAppender rootSanityCheck = new ListAppender("sanity-checker");
    try {
      LoggerContext.getContext(false).getConfiguration().getRootLogger().addAppender(rootSanityCheck, Level.WARN, null);
      LoggerContext.getContext(false).updateLoggers();
      
      log.error("this matches the default ignore_exception pattern");
      log.error("something matching foo that should make it"); // 1
      assertEquals(1, rootSanityCheck.getEvents().size());
      ignoreException("foo");
      log.error("something matching foo that should NOT make it");
      ignoreException("foo");
      ignoreException("ba+r");
      log.error("something matching foo that should still NOT make it");
      log.error("something matching baaaar that should NOT make it");
      log.warn("A warning should be fine even if it matches ignore_exception and foo and bar"); // 2
      assertEquals(2, rootSanityCheck.getEvents().size());
      unIgnoreException("foo");
      log.error("another thing matching foo that should make it"); // 3
      assertEquals(3, rootSanityCheck.getEvents().size());
      log.error("something matching baaaar that should still NOT make it");
      resetExceptionIgnores();
      log.error("this still matches the default ignore_exception pattern");
      log.error("but something matching baaaar should make it now"); // 4
      assertEquals(4, rootSanityCheck.getEvents().size());

    } finally {
      LoggerContext.getContext(false).getConfiguration().getRootLogger().removeAppender(rootSanityCheck.getName());
      LoggerContext.getContext(false).updateLoggers();
    }
    assertEquals(4, rootSanityCheck.getEvents().size());
  }
  
  /**
   * Maintains an in memory List of log events.
   * <p>
   * Inspired by <code>org.apache.logging.log4j.core.test.appender.ListAppender</code>
   * but we have much simpler needs.
   */
  @SuppressForbidden(reason="We need to use log4J2 classes directly to check that the ErrorLogMuter is working")
  public static final class ListAppender extends AbstractAppender {
    // Use Collections.synchronizedList rather than CopyOnWriteArrayList because we expect
    // more frequent writes than reads.
    private final List<LogEvent> events = Collections.synchronizedList(new ArrayList<>());
    private final List<LogEvent> publicEvents = Collections.unmodifiableList(events);
    
    public ListAppender(final String name) {
      super(name, null, null, true, Property.EMPTY_ARRAY);
      assert null != name;
    }
    
    @Override
    public void append(final LogEvent event) {
      if (event instanceof MutableLogEvent) {
        // must take snapshot or subsequent calls to logger.log() will modify this event
        events.add(((MutableLogEvent) event).createMemento());
      } else {
        events.add(event);
      }
      if (log.isDebugEnabled()) {
        log.debug("{} intercepted a log event (#{})", this.getName(), events.size());
      }
    }

    /** Returns an immutable view of captured log events, contents can change as events are logged */
    public List<LogEvent> getEvents() {
      return publicEvents;
    }
  }
}
