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
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.SuppressForbidden;
import org.junit.BeforeClass;
import org.junit.Test;


@SuppressForbidden(reason = "test uses log4j2 because it tests output at a specific level")
public class LoggingHandlerTest extends SolrTestCaseJ4 {

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
    Logger tst = LogManager.getLogger("org.apache.solr.SolrTestCaseJ4");
 
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    LoggerConfig loggerConfig = ctx.getConfiguration().getLoggerConfig(tst.getName());
    loggerConfig.setLevel(Level.INFO);
    ctx.updateLoggers();
    
    assertQ("Show Log Levels OK",
            req(CommonParams.QT,"/admin/logging")
            ,"//arr[@name='loggers']/lst/str[.='"+tst.getName()+"']/../str[@name='level'][.='"+tst.getLevel()+"']"
            ,"//arr[@name='loggers']/lst/str[.='org.apache']/../null[@name='level']"
            );

    assertQ("Set a level",
            req(CommonParams.QT,"/admin/logging",  
                "set", tst.getName()+":TRACE")
            ,"//arr[@name='loggers']/lst/str[.='"+tst.getName()+"']/../str[@name='level'][.='TRACE']"
            );
    
    assertQ("Remove a level",
        req(CommonParams.QT,"/admin/logging",  
            "set", tst.getName()+":null")
        ,"//arr[@name='loggers']/lst/str[.='"+tst.getName()+"']/../str[@name='level'][.='OFF']"
        );
  }
}
