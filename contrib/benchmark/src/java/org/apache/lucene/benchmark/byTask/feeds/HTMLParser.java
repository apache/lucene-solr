package org.apache.lucene.benchmark.byTask.feeds;

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

import java.io.IOException;
import java.io.Reader;
import java.text.DateFormat;
import java.util.Date;

/**
 * HTML Parsing Interfacew for test purposes
 */
public interface HTMLParser {

  /**
   * Parse the input Reader and return DocData. 
   * A provided name or date is used for the result, otherwise an attempt is 
   * made to set them from the parsed data.
   * @param dateFormat date formatter to use for extracting the date.   
   * @param name name of the result doc data. If null, attempt to set by parsed data.
   * @param date date of the result doc data. If null, attempt to set by parsed data.
   * @param reader of html text to parse.
   * @return Parsed doc data.
   * @throws IOException
   * @throws InterruptedException
   */
  public DocData parse(String name, Date date, Reader reader, DateFormat dateFormat) throws IOException, InterruptedException;
  
  /**
   * Parse the inputText and return DocData. 
   * @param inputText the html text to parse.
   * @see #parse(String, Date, Reader, DateFormat)
   */
  public DocData parse(String name, Date date, StringBuffer inputText, DateFormat dateFormat) throws IOException, InterruptedException;

}
