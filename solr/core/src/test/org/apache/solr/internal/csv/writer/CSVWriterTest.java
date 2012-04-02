/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.solr.internal.csv.writer;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

/**
 * The testcase for the csv writer.
 */
public class CSVWriterTest extends TestCase {

    public void testCSVConfig() {
        CSVWriter writer = new CSVWriter();
        assertEquals(null, writer.getConfig());
        CSVConfig config = new CSVConfig();
        writer.setConfig(config);
        assertEquals(config, writer.getConfig());
        writer = new CSVWriter(config);
        assertEquals(config, writer.getConfig());
    }
    
    public void testWriter() {
        CSVWriter writer = new CSVWriter();
        CSVConfig config = new CSVConfig();
        config.addField(new CSVField("field1", 5));
        config.addField(new CSVField("field2", 4));
        writer.setConfig(config);
        StringWriter sw = new StringWriter();
        writer.setWriter(sw);
        Map map = new HashMap();
        map.put("field1", "12345");
        map.put("field2", "1234");
        writer.writeRecord(map);
        assertEquals("12345,1234\n",sw.toString());
    }
}
