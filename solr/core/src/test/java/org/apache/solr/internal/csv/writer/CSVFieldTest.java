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
package org.apache.solr.internal.csv.writer;

import junit.framework.TestCase;

public class CSVFieldTest extends TestCase {

    public void testCSVField() {
        CSVField field = new CSVField();
        assertEquals(null, field.getName());
        field.setName("id");
        assertEquals("id", field.getName());
        assertEquals(0, field.getSize());
        field.setSize(10);
        assertEquals(10, field.getSize());
        field = new CSVField("name");
        assertEquals("name", field.getName());
        field = new CSVField("name", 10);
        assertEquals("name", field.getName());
        assertEquals(10, field.getSize());
    }
    
    public void testFill() {
        CSVField field = new CSVField();
        assertEquals(CSVConfig.FILLNONE, field.getFill());
        assertEquals(false, field.overrideFill());
        field.setFill(CSVConfig.FILLLEFT);
        assertEquals(true, field.overrideFill());
        assertEquals(CSVConfig.FILLLEFT, field.getFill());
    }
}
