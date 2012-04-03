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

import java.util.Collection;

import junit.framework.TestCase;

/**
 * Testcase for the CSVConfig
 */
public class CSVConfigTest extends TestCase {
    

    public void testFixedWith() {
        CSVConfig config = new CSVConfig();
        assertEquals(false, config.isFixedWidth());
        config.setFixedWidth(true);
        assertEquals(true, config.isFixedWidth());
    }
    
    public void testFields() {
        CSVConfig config = new CSVConfig();
        assertEquals(0, config.getFields().length);
        config.setFields((CSVField[])null);
        assertEquals(0, config.getFields().length);
        config.setFields((Collection)null);
        assertEquals(0, config.getFields().length);
        CSVField field = new CSVField();
        field.setName("field1");
        config.addField(field);
        assertEquals(field, config.getFields()[0]);
        assertEquals(null, config.getField(null));
        assertEquals(null, config.getField("field11"));
        assertEquals(field, config.getField("field1"));
    }
    
    public void testFill() {
        CSVConfig config = new CSVConfig();
        assertEquals(CSVConfig.FILLNONE, config.getFill());
        config.setFill(CSVConfig.FILLLEFT);
        assertEquals(CSVConfig.FILLLEFT, config.getFill());
        config.setFill(CSVConfig.FILLRIGHT);
        assertEquals(CSVConfig.FILLRIGHT, config.getFill());
        assertEquals(' ', config.getFillChar());
        config.setFillChar('m');
        assertEquals('m', config.getFillChar());
    }
    
    public void testDelimiter() {
        CSVConfig config = new CSVConfig();
        assertEquals(',', config.getDelimiter());
        config.setDelimiter(';');
        assertEquals(';', config.getDelimiter());
        assertEquals(false, config.isDelimiterIgnored());
        config.setIgnoreDelimiter(true);
        assertEquals(true, config.isDelimiterIgnored());
    }
    
    public void testValueDelimiter() {
        CSVConfig config = new CSVConfig();
        assertEquals('"', config.getValueDelimiter());
        config.setValueDelimiter('m');
        assertEquals('m', config.getValueDelimiter());
        assertEquals(true, config.isValueDelimiterIgnored());
        config.setIgnoreValueDelimiter(false);
        assertEquals(false, config.isValueDelimiterIgnored());
    }
    
    public void testFieldHeader() {
        CSVConfig config = new CSVConfig();
        assertEquals(false, config.isFieldHeader());
        config.setFieldHeader(true);
        assertEquals(true, config.isFieldHeader());
    }
    
    public void testTrimEnd() {
        CSVConfig config = new CSVConfig();
        assertEquals(false, config.isEndTrimmed());
        config.setEndTrimmed(true);
        assertEquals(true, config.isEndTrimmed());
    }

}
