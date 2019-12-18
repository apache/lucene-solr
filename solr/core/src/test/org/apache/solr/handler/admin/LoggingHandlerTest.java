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
package org.apache.solr.handler.admin;


import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.util.LogLevel;
import org.junit.BeforeClass;
import org.junit.Test;


@SuppressForbidden(reason = "test uses log4j2 because it tests output at a specific level")
@LogLevel("org.apache.solr.bogus_logger_package.BogusLoggerClass=DEBUG")
public class LoggingHandlerTest extends SolrTestCaseJ4 {
  private final String PARENT_LOGGER_NAME = "org.apache.solr.bogus_logger_package";
  private final String CLASS_LOGGER_NAME = PARENT_LOGGER_NAME + ".BogusLoggerClass";
  
  // TODO: This only tests Log4j at the moment, as that's what's defined
  // through the CoreContainer.

  // TODO: Would be nice to throw an exception on trying to set a
  // log level that doesn't exist
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testLogLevelHandlerOutput() throws Exception {
    
    // sanity check our setup...
    assertNotNull(this.getClass().getAnnotation(LogLevel.class));
    final String annotationConfig = this.getClass().getAnnotation(LogLevel.class).value();
    assertTrue("WTF: " + annotationConfig, annotationConfig.startsWith( PARENT_LOGGER_NAME ));
    assertTrue("WTF: " + annotationConfig, annotationConfig.startsWith( CLASS_LOGGER_NAME ));
    assertTrue("WTF: " + annotationConfig, annotationConfig.endsWith( Level.DEBUG.toString() ));
    
    assertEquals(Level.DEBUG, LogManager.getLogger( CLASS_LOGGER_NAME ).getLevel());
    
    final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    final Configuration config = ctx.getConfiguration();

    assertEquals("Unexpected config for " + PARENT_LOGGER_NAME + " ... expected 'root' config",
                 config.getRootLogger(),
                 config.getLoggerConfig(PARENT_LOGGER_NAME));
    assertEquals(Level.DEBUG, config.getLoggerConfig(CLASS_LOGGER_NAME).getLevel());

    assertQ("Show Log Levels OK",
            req(CommonParams.QT,"/admin/logging")
            ,"//arr[@name='loggers']/lst/str[.='"+CLASS_LOGGER_NAME+"']/../str[@name='level'][.='DEBUG']"
            ,"//arr[@name='loggers']/lst/str[.='"+PARENT_LOGGER_NAME+"']/../null[@name='level']"
            );

    assertQ("Set a (new) level",
            req(CommonParams.QT,"/admin/logging",  
                "set", PARENT_LOGGER_NAME+":TRACE")
            ,"//arr[@name='loggers']/lst/str[.='"+PARENT_LOGGER_NAME+"']/../str[@name='level'][.='TRACE']"
            );
    assertEquals(Level.TRACE, config.getLoggerConfig(PARENT_LOGGER_NAME).getLevel());
    assertEquals(Level.DEBUG, config.getLoggerConfig(CLASS_LOGGER_NAME).getLevel());
    
    // NOTE: LoggeringHandler doesn't actually "remove" the LoggerConfig, ...
    // evidently so people using they UI can see that it was explicitly turned "OFF" ?
    assertQ("Remove a level",
        req(CommonParams.QT,"/admin/logging",  
            "set", PARENT_LOGGER_NAME+":null")
        ,"//arr[@name='loggers']/lst/str[.='"+PARENT_LOGGER_NAME+"']/../str[@name='level'][.='OFF']"
        );
    assertEquals(Level.OFF, config.getLoggerConfig(PARENT_LOGGER_NAME).getLevel());
    assertEquals(Level.DEBUG, config.getLoggerConfig(CLASS_LOGGER_NAME).getLevel());

    
  }
}
