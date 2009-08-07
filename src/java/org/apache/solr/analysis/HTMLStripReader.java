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

package org.apache.solr.analysis;


import java.io.Reader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Set;

import org.apache.lucene.analysis.CharReader;

/**
 * A Reader that wraps another reader and attempts to strip out HTML constructs.
 *
 *
 * @version $Id$
 * @deprecated Use {@link HTMLStripCharFilter}
 */
@Deprecated
public class HTMLStripReader extends HTMLStripCharFilter {

  public static void main(String[] args) throws IOException {
    Reader in = new HTMLStripReader(
            new InputStreamReader(System.in));
    int ch;
    while ( (ch=in.read()) != -1 ) System.out.print((char)ch);
  }

  public HTMLStripReader(Reader source){
    super(CharReader.get(source.markSupported() ? source : new BufferedReader(source)));
  }

  public HTMLStripReader(Reader source, Set<String> escapedTags){
    super(CharReader.get(source),escapedTags);
  }

  public HTMLStripReader(Reader source, Set<String> escapedTags,int readAheadLimit){
    super(CharReader.get(source),escapedTags,readAheadLimit);
  }
}
