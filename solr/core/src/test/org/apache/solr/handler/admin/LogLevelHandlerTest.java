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

package org.apache.solr.handler.admin;


import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.junit.BeforeClass;
import org.junit.Test;

public class LogLevelHandlerTest extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testLogLevelHandlerOutput() throws Exception {
    assertQ("Show Log Levels OK",
            req(CommonParams.QT,"/admin/loglevel")
            ,"//arr[@name='loggers']/lst/str[.='global']/../str[@name='level'][.='INFO']"
            ,"//arr[@name='loggers']/lst/str[.='org.apache']/../null[@name='level']"
            );

    assertQ("Set and remove a level",
            req(CommonParams.QT,"/admin/loglevel",  
                "set", "org.xxx.yyy.abc:null",
                "set", "org.xxx.yyy.zzz:FINEST")
            ,"//arr[@name='loggers']/lst/str[.='org.xxx.yyy.zzz']/../str[@name='level'][.='FINEST']"
            );
  }
}
