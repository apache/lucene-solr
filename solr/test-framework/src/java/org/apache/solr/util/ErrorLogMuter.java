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
import java.io.Closeable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.function.Predicate;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.solr.common.util.SuppressForbidden;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.message.Message;

/**
 * <p>
 * Helper code for "Muting" ERROR log messages that you expect as a result of the things you are testing, 
 * so they aren't a distraction in test logs.  Example usage...
 * </p>
 * <code>
 *  try (ErrorLogMuter errors = ErrorLogMuter.substring("nullfirst")) {
 *    assertQEx( "invalid query format",
 *               req( "q","id_i:1000", "sort", "nullfirst" ), 400 );
 *    assertEquals(1, errors.getCount());
 *  }
 * </code>
 * <p>
 * ERROR messages are considered a match if their input matches either the message String, or the <code>toString</code> of an included 
 * {@link Throwable}, or any of the recursive {@link Throwable#getCause}es of an included <code>Throwable</code>.
 * </p>
 * <p>
 * Matching ERROR messages are "muted" by filtering them out of the ROOT logger.  Any Appenders attached to more specific 
 * Loggers may still include these "muted" ERRROR messages.
 * </p>
 */
@SuppressForbidden(reason="We need to use log4J2 classes directly to check that the ErrorLogMuter is working")
public final class ErrorLogMuter implements Closeable, AutoCloseable {

  // far easier to use FQN for our (one) slf4j Logger then to use a FQN every time we refe to log4j2 Logger
  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass()); // nowarn_valid_logger

  private final static LoggerContext CTX = LoggerContext.getContext(false);
  
  /** @see #createName */
  private final static AtomicInteger ID_GEN = new AtomicInteger(0);
  /** generate a unique name for each muter to use in it's own lifecycle logging */
  private static String createName(final String type) {
    return MethodHandles.lookup().lookupClass().getSimpleName() + "-" + type + "-" + ID_GEN.incrementAndGet();
  }

  /** Mutes ERROR log messages that contain the input as a substring */
  public static ErrorLogMuter substring(final String substr) { 
    final String name = createName("substring");
    log.info("Creating {} for ERROR logs containing the substring: {}", name, substr);
    return new ErrorLogMuter(name, (str) -> { return str.contains(substr); });
  }
  
  /** 
   * Mutes ERROR log messages that <em>partially</em> match the specified regex.  
   * @see Matcher#find
   */
  public static ErrorLogMuter regex(final String regex) { 
    return regex(Pattern.compile(regex));
  }
  /** 
   * Mutes ERROR log messages that <em>partially</em> match the specified regex.  
   * @see Matcher#find
   */
  public static ErrorLogMuter regex(final Pattern pat) { 
    final String name = createName("regex");
    log.info("Creating {} for ERROR logs matching regex: {}", name, pat);
    return new ErrorLogMuter(name, (str) -> { return pat.matcher(str).find(); });
  }

  private final String name;
  private final CountingFilter rootFilter;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /** @see StringPredicateErrorOrThrowableFilter */
  private ErrorLogMuter(final String name, final Predicate<String> predicate) {
    assert null != name;
    assert null != predicate;

    final LoggerConfig rootLoggerConfig = CTX.getConfiguration().getRootLogger();

    this.name = name;
    this.rootFilter = new StringPredicateErrorOrThrowableFilter(predicate);
    rootLoggerConfig.addFilter(this.rootFilter);
    
    CTX.updateLoggers();
  }

  /** 
   * The number of ERROR messages muted (by this instance) so far in it's lifecycle.
   * This number may be less then the number of ERROR messages expected if multiple ErrorLogMuter 
   * objects are in use which match the same ERROR log messages
   */
  public int getCount() {
    return rootFilter.getCount();
  }
  
  public void close() {
    if (! closed.getAndSet(true)) { // Don't muck with log4j if we accidently get a double close
      CTX.getConfiguration().getRootLogger().removeFilter(rootFilter);
      CTX.updateLoggers();
      if (log.isInfoEnabled()) {
        log.info("Closing {} after mutting {} log messages", this.name, getCount());
      }
    }
  }

  @SuppressForbidden(reason="We need to use log4J2 classes directly to check that the ErrorLogMuter is working")
  private static interface CountingFilter extends Filter {
    /** The number of messages that have been filtered */
    public int getCount();
  }
  
  /**
   * <p>
   * Given a String predicate, this filter DENY's log events that are at least as specific as <code>ERROR</code>, and have a message or 
   * {@link Throwable#toString} that matches the predicate.  When a Throwable is evaluated, the predicate is tested against the 
   * <code>toString()</code> of all nested causes.
   * </p>
   * <p>
   * Any message that is not at least as specific as <code>ERROR</code>, or that the predicate does not match, are left NEUTRAL.
   * </p>
   */
  @SuppressForbidden(reason="We need to use log4J2 classes directly to check that the ErrorLogMuter is working")
  private static final class StringPredicateErrorOrThrowableFilter extends AbstractFilter implements CountingFilter {
    // This could probably be implemented with a combination of "LevelRangeFilter" and "ConjunctionFilter" if "ConjunctionFilter" existed
    // Since it doesn't, we write our own more specialized impl instead of writing & combining multiple generalized versions

    final Predicate<String> predicate;
    final AtomicInteger count = new AtomicInteger(0);
    public StringPredicateErrorOrThrowableFilter(final Predicate<String> predicate) {
      super(Filter.Result.DENY, Filter.Result.NEUTRAL);
      assert null != predicate;
      this.predicate = predicate;
    }

    public int getCount() {
      return count.get();
    }

    // NOTE: This is inspired by log4j's RegexFilter, but with an eye to being more "garbage-free" friendly
    // Oddly, StringMatchFilter does things differnetly and acts like it needs to (re?) format msgs when params are provided
    // Since RegexFilter has tests, and StringMatchFilter doesn't, we assume RegexFilter knows what it's doing...

    /** The main logic of our filter: Evaluate predicate against log msg &amp; throwable msg(s) if and only if ERROR; else mismatch */
    private Filter.Result doFilter(final Level level, final String msg, final Throwable throwable) {
      if (level.isMoreSpecificThan(Level.ERROR)) {
        if (null != msg && predicate.test(msg)) {
          return matchAndCount();
        }
        for (Throwable t = throwable; null != t; t = t.getCause()) {
          if (predicate.test(t.toString())) {
            return matchAndCount();
          }
        }
      }
      return getOnMismatch();
    }
    
    /** helper to be called by doFilter anytime it wants to return a "match" */
    private Filter.Result matchAndCount() {
      count.incrementAndGet();
      return getOnMatch();
    }
    
    public Result filter(Logger logger, Level level, Marker marker, String msg,
                         Object... params) {
      return doFilter(level, msg, null);
    }
    public Result filter(Logger logger, Level level, Marker marker, String msg,
                         Object p0) {
      return doFilter(level, msg, null);
    }
    public Result filter(Logger logger, Level level, Marker marker, String msg,
                         Object p0, Object p1) {
      return doFilter(level, msg, null);
    }
    public Result filter(Logger logger, Level level, Marker marker, String msg,
                         Object p0, Object p1, Object p2) {
      return doFilter(level, msg, null);
    }
    public Result filter(Logger logger, Level level, Marker marker, String msg,
                         Object p0, Object p1, Object p2, Object p3) {
      return doFilter(level, msg, null);
    }
    public Result filter(Logger logger, Level level, Marker marker, String msg,
                         Object p0, Object p1, Object p2, Object p3, Object p4) {
      return doFilter(level, msg, null);
    }
    public Result filter(Logger logger, Level level, Marker marker, String msg,
                         Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
      return doFilter(level,msg, null);
    }
    public Result filter(Logger logger, Level level, Marker marker, String msg,
                         Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
      return doFilter(level, msg, null);
    }
    public Result filter(Logger logger, Level level, Marker marker, String msg,
                         Object p0, Object p1, Object p2, Object p3, Object p4,
                         Object p5, Object p6, Object p7) {
      return doFilter(level, msg, null);
    }
    public Result filter(Logger logger, Level level, Marker marker, String msg,
                         Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
      return doFilter(level, msg, null);
    }
    public Result filter(Logger logger, Level level, Marker marker, String msg,
                         Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
      return doFilter(level, msg, null);
    }
    public Result filter(Logger logger, Level level, Marker marker, Object msg,
                         Throwable t) {
      return doFilter(level, null == msg ? null : msg.toString(), t);
    }
    public Result filter(Logger logger, Level level, Marker marker, Message msg,
                         Throwable t) {
      return doFilter(level, msg.getFormattedMessage(), t);
    }
    public Result filter(LogEvent event) {
      // NOTE: For our usage, we're not worried about needing to filter LogEvents rom remote JVMs with ThrowableProxy
      // stand ins for Throwabls that don't exist in our classloader...
      return doFilter(event.getLevel(),
                      event.getMessage().getFormattedMessage(),
                      event.getThrown());
    }
  }
}
