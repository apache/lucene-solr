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

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Arrays;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.util.SimpleOrderedMap;


public class SystemInfoHandlerTest extends LuceneTestCase {

  public void testMagickGetter() throws Exception {

    OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

    // make one directly
    SimpleOrderedMap<Object> info = new SimpleOrderedMap<>();
    info.add( "name", os.getName() );
    info.add( "version", os.getVersion() );
    info.add( "arch", os.getArch() );

    // make another using addMXBeanProperties() 
    SimpleOrderedMap<Object> info2 = new SimpleOrderedMap<>();
    SystemInfoHandler.addMXBeanProperties( os, OperatingSystemMXBean.class, info2 );

    // make sure they got the same thing
    for (String p : Arrays.asList("name", "version", "arch")) {
      assertEquals(info.get(p), info2.get(p));
    }
  }

}
