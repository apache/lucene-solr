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
package org.apache.lucene.benchmark.byTask.feeds;

import java.io.IOException;
import java.io.Reader;
import java.util.Date;

/** HTML Parsing Interface for test purposes */
public interface HTMLParser {

  /**
   * Parse the input Reader and return DocData. The provided name,title,date are used for the
   * result, unless when they're null, in which case an attempt is made to set them from the parsed
   * data.
   *
   * @param docData result reused
   * @param name name of the result doc data.
   * @param date date of the result doc data. If null, attempt to set by parsed data.
   * @param reader reader of html text to parse.
   * @param trecSrc the {@link TrecContentSource} used to parse dates.
   * @return Parsed doc data.
   * @throws IOException If there is a low-level I/O error.
   */
  public DocData parse(
      DocData docData, String name, Date date, Reader reader, TrecContentSource trecSrc)
      throws IOException;
}
