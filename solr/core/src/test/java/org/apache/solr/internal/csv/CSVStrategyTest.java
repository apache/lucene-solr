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
package org.apache.solr.internal.csv;

import junit.framework.TestCase;

/**
 * CSVStrategyTest
 *
 * The test are organized in three different sections:
 * The 'setter/getter' section, the lexer section and finally the strategy 
 * section. In case a test fails, you should follow a top-down approach for 
 * fixing a potential bug (it's likely that the strategy itself fails if the lexer
 * has problems...).
 */
public class CSVStrategyTest extends TestCase {

  // ======================================================
  //   getters / setters
  // ======================================================
  public void testGetSetCommentStart() {
    CSVStrategy strategy = (CSVStrategy)CSVStrategy.DEFAULT_STRATEGY.clone();
    strategy.setCommentStart('#');
    assertEquals(strategy.getCommentStart(), '#');
    strategy.setCommentStart('!');
    assertEquals(strategy.getCommentStart(), '!');
  }

  public void testGetSetEncapsulator() {
    CSVStrategy strategy = (CSVStrategy)CSVStrategy.DEFAULT_STRATEGY.clone();
    strategy.setEncapsulator('"');
    assertEquals(strategy.getEncapsulator(), '"');
    strategy.setEncapsulator('\'');
    assertEquals(strategy.getEncapsulator(), '\'');
  }

  public void testGetSetDelimiter() {
    CSVStrategy strategy = (CSVStrategy)CSVStrategy.DEFAULT_STRATEGY.clone();
    strategy.setDelimiter(';');
    assertEquals(strategy.getDelimiter(), ';');
    strategy.setDelimiter(',');
    assertEquals(strategy.getDelimiter(), ',');
    strategy.setDelimiter('\t');
    assertEquals(strategy.getDelimiter(), '\t');
  }

  public void testSetCSVStrategy() {
    CSVStrategy strategy = CSVStrategy.DEFAULT_STRATEGY;
    // default settings
    assertEquals(strategy.getDelimiter(), ',');
    assertEquals(strategy.getEncapsulator(), '"');
    assertEquals(strategy.getCommentStart(), CSVStrategy.COMMENTS_DISABLED);
    assertEquals(true,  strategy.getIgnoreLeadingWhitespaces());
    assertEquals(false, strategy.getUnicodeEscapeInterpretation());
    assertEquals(true,  strategy.getIgnoreEmptyLines());
    // explicit csv settings
    assertEquals(strategy.getDelimiter(), ',');
    assertEquals(strategy.getEncapsulator(), '"');
    assertEquals(strategy.getCommentStart(), CSVStrategy.COMMENTS_DISABLED);
    assertEquals(true,  strategy.getIgnoreLeadingWhitespaces());
    assertEquals(false, strategy.getUnicodeEscapeInterpretation());
    assertEquals(true,  strategy.getIgnoreEmptyLines());
  }
  
  public void testSetExcelStrategy() {
    CSVStrategy strategy = CSVStrategy.EXCEL_STRATEGY;
    assertEquals(strategy.getDelimiter(), ',');
    assertEquals(strategy.getEncapsulator(), '"');
    assertEquals(strategy.getCommentStart(), CSVStrategy.COMMENTS_DISABLED);
    assertEquals(false,  strategy.getIgnoreLeadingWhitespaces());
    assertEquals(false, strategy.getUnicodeEscapeInterpretation());
    assertEquals(false, strategy.getIgnoreEmptyLines());
  }
  
} 
