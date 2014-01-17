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

import java.io.ByteArrayInputStream;

import junit.framework.TestCase;

/**
 * Tests for the config guesser.
 */
public class CSVConfigGuesserTest extends TestCase {

    public void testSetters() throws Exception {
        CSVConfigGuesser guesser = new CSVConfigGuesser();
        ByteArrayInputStream in = new ByteArrayInputStream(new byte[0]);
        guesser.setInputStream(in);
        assertEquals(in, guesser.getInputStream());
        guesser = new CSVConfigGuesser(in);
        assertEquals(in, guesser.getInputStream());
        assertEquals(false, guesser.hasFieldHeader());
        guesser.setHasFieldHeader(true);
        assertEquals(true, guesser.hasFieldHeader());
    }
    /**
     * Test a format like
     *  1234 ; abcd ; 1234 ;
     *
     */
    public void testConfigGuess1() throws Exception {
        CSVConfig expected = new CSVConfig();
        expected.setDelimiter(';');
        expected.setValueDelimiter(' ');
        expected.setFill(CSVConfig.FILLRIGHT);
        expected.setIgnoreValueDelimiter(false);
        expected.setFixedWidth(true);
        CSVField field = new CSVField();
        field.setSize(4);
        expected.addField(field);
        expected.addField(field);
        StringBuilder sb = new StringBuilder();
        sb.append("1234;abcd;1234\n");
        sb.append("abcd;1234;abcd");
        ByteArrayInputStream in = new ByteArrayInputStream(sb.toString().getBytes("UTF-8"));
        CSVConfigGuesser guesser = new CSVConfigGuesser(in);
        CSVConfig guessed = guesser.guess();
        assertEquals(expected.isFixedWidth(), guessed.isFixedWidth());
        assertEquals(expected.getFields().length, guessed.getFields().length);
        assertEquals(expected.getFields()[0].getSize(), guessed.getFields()[0].getSize());
    }
    /**
     * Test a format like
     *  1234,123123,12312312,213123
     *  1,2,3,4
     *
     */
    public void testConfigGuess2() throws Exception {
        CSVConfig expected = new CSVConfig();
        expected.setDelimiter(';');
        expected.setValueDelimiter(' ');
        expected.setFill(CSVConfig.FILLRIGHT);
        expected.setIgnoreValueDelimiter(false);
//        expected.setFixedWidth(false);
        StringBuilder sb = new StringBuilder();
        sb.append("1,2,3,4\n");
        sb.append("abcd,1234,abcd,1234");
        ByteArrayInputStream in = new ByteArrayInputStream(sb.toString().getBytes("UTF-8"));
        CSVConfigGuesser guesser = new CSVConfigGuesser(in);
        CSVConfig guessed = guesser.guess();
        assertEquals(expected.isFixedWidth(), guessed.isFixedWidth());
    }
}
