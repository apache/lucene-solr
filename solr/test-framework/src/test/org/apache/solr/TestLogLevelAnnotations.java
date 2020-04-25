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

package org.apache.solr;

import java.util.Map;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.util.LogLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;

@SuppressForbidden(reason="We need to use log4J2 classes to access the log levels")
@LogLevel("org.apache.solr.bogus_logger.ClassLogLevel=error;org.apache.solr.bogus_logger.MethodLogLevel=warn")
public class TestLogLevelAnnotations extends SolrTestCaseJ4 {

  private static final String bogus_logger_prefix = "org.apache.solr.bogus_logger";
  
  /** 
   * <p>
   * The default log level of the root logger when this class is loaded by the JVM, Used to validate 
   * some logger configurations when the tests are run.
   * </p>
   * <p>
   * We don't want a hardcoded assumption here because it may change based on how the user runs the test.
   * </p>
   * <p>
   * We also don't want to initialize this in a <code>@BeforeClass</code> method because that will run 
   * <em>after</em> the <code>@BeforeClass</code> logic of our super class {@link SolrTestCaseJ4} 
   * where the <code>@LogLevel</code> annotation on this class will be parsed and evaluated -- modifying the
   * log4j run time configuration.  
   * There is no reason why the <code>@LogLevel</code> configuration of this class <em>should</em> affect 
   * the "root" Logger, but setting this in static class initialization protect us (as best we can) 
   * against the possibility that it <em>might</em> due to an unforseen (future) bug.
   * </p>
   *
   * @see #checkLogLevelsBeforeClass
   */
  public static final Level DEFAULT_LOG_LEVEL = LogManager.getRootLogger().getLevel();
  
  /** 
   * Sanity check that our <code>AfterClass</code> logic is valid, and isn't broken right from the start
   *
   * @see #checkLogLevelsAfterClass
   */
  @BeforeClass
  public static void checkLogLevelsBeforeClass() {
    final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    final Configuration config = ctx.getConfiguration();

    // NOTE: we're checking the CONFIGURATION of the loggers, not the "effective" value of the Logger
    assertEquals("Somehow, the configured value of the root logger changed since this class was loaded",
                 DEFAULT_LOG_LEVEL, config.getRootLogger().getLevel());
    assertEquals("Your Logger conf sets a level on a bogus package that breaks this test: "
                 + bogus_logger_prefix,
                 config.getRootLogger(),
                 config.getLoggerConfig(bogus_logger_prefix));
    assertEquals(Level.ERROR, config.getLoggerConfig(bogus_logger_prefix + ".ClassLogLevel").getLevel());
    assertEquals(Level.WARN, config.getLoggerConfig(bogus_logger_prefix + ".MethodLogLevel").getLevel());

    // Now sanity check the EFFECTIVE Level of these loggers before the methods run...
    assertEquals(DEFAULT_LOG_LEVEL, LogManager.getRootLogger().getLevel());
    assertEquals(DEFAULT_LOG_LEVEL, LogManager.getLogger(bogus_logger_prefix).getLevel());
    assertEquals(Level.ERROR, LogManager.getLogger(bogus_logger_prefix + ".ClassLogLevel").getLevel());
    assertEquals(Level.WARN, LogManager.getLogger(bogus_logger_prefix + ".MethodLogLevel").getLevel());
  }

  /** 
   * Check that the expected log level <em>configurations</em> have been reset after the test
   * <p>
   * <b>NOTE:</b> We only validate <code>@LogLevel</code> modifications made at the 
   * {@link #testMethodLogLevels} level,  not at the 'class' level, because of the lifecycle of junit 
   * methods: This <code>@AfterClass</code> will run before the <code>SolrTestCaseJ4</code> 
   * <code>@AfterClass</code> method where the 'class' <code>@LogLevel</code> modifications will be reset.
   * </p>
   *
   * @see #checkLogLevelsBeforeClass
   * @see #testWhiteBoxMethods
   */
  @AfterClass
  public static void checkLogLevelsAfterClass() {
    final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    final Configuration config = ctx.getConfiguration();
    
    // NOTE: we're checking the CONFIGURATION of the loggers, not the "effective" value of the Logger
    assertEquals(DEFAULT_LOG_LEVEL, config.getRootLogger().getLevel());
    assertEquals(bogus_logger_prefix
                 + " should have had it's config unset; should now return the 'root' LoggerConfig",
                 config.getRootLogger(),
                 config.getLoggerConfig(bogus_logger_prefix));
    assertEquals(Level.ERROR, config.getLoggerConfig(bogus_logger_prefix + ".ClassLogLevel").getLevel());
    assertEquals(Level.WARN, config.getLoggerConfig(bogus_logger_prefix + ".MethodLogLevel").getLevel());

    // Now sanity check the EFFECTIVE Level of these loggers...
    assertEquals(DEFAULT_LOG_LEVEL, LogManager.getRootLogger().getLevel());
    assertEquals(DEFAULT_LOG_LEVEL, LogManager.getLogger(bogus_logger_prefix).getLevel());
    assertEquals(Level.ERROR, LogManager.getLogger(bogus_logger_prefix + ".ClassLogLevel").getLevel());
    assertEquals(Level.WARN, LogManager.getLogger(bogus_logger_prefix + ".MethodLogLevel").getLevel());
  }
  
  public void testClassLogLevels() {
    assertEquals(DEFAULT_LOG_LEVEL, LogManager.getLogger("org.apache.solr.bogus_logger").getLevel());
    assertEquals(Level.ERROR, LogManager.getLogger(bogus_logger_prefix + ".ClassLogLevel").getLevel());
    assertEquals(Level.WARN, LogManager.getLogger(bogus_logger_prefix + ".MethodLogLevel").getLevel());
  }

  @LogLevel("org.apache.solr.bogus_logger.MethodLogLevel=debug;org.apache.solr.bogus_logger=INFO")
  public void testMethodLogLevels() {
    assertEquals(Level.INFO, LogManager.getLogger(bogus_logger_prefix + ".BogusClass").getLevel());
    assertEquals(Level.ERROR, LogManager.getLogger(bogus_logger_prefix + ".ClassLogLevel").getLevel());
    assertEquals(Level.DEBUG, LogManager.getLogger(bogus_logger_prefix + ".MethodLogLevel").getLevel());
  }

  /**
   * Directly test the methods in the {@link LogLevel} annotation class
   */
  @LogLevel("org.apache.solr.bogus_logger.MethodLogLevel=TRACE")
  public void testWhiteBoxMethods() {
    final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    final Configuration config = ctx.getConfiguration();

    final Map<String,Level> oldLevels = LogLevel.Configurer.setLevels(bogus_logger_prefix + "=TRACE");
    //
    assertEquals(oldLevels.toString(), 1, oldLevels.size());
    assertNull(oldLevels.get(bogus_logger_prefix));
    //
    assertEquals(Level.TRACE, config.getLoggerConfig(bogus_logger_prefix).getLevel());
    assertEquals(Level.TRACE, LogManager.getLogger(bogus_logger_prefix).getLevel());
    
    // restore (to 'unset' values)...
    LogLevel.Configurer.restoreLogLevels(oldLevels);
    assertEquals(bogus_logger_prefix
                 + " should have had it's config unset; should now return the 'root' LoggerConfig",
                 config.getRootLogger(),
                 config.getLoggerConfig(bogus_logger_prefix));
    assertEquals(DEFAULT_LOG_LEVEL, LogManager.getLogger(bogus_logger_prefix).getLevel());
    
  }
}
