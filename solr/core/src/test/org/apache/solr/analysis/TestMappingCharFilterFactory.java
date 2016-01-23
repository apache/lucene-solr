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

import org.apache.lucene.util.LuceneTestCase;

public class TestMappingCharFilterFactory extends LuceneTestCase {
  public void testParseString() throws Exception {

    MappingCharFilterFactory f = new MappingCharFilterFactory();

    try {
      f.parseString( "\\" );
      fail( "escape character cannot be alone." );
    }
    catch( RuntimeException expected ){}
    
    assertEquals( "unexpected escaped characters",
        "\\\"\n\t\r\b\f", f.parseString( "\\\\\\\"\\n\\t\\r\\b\\f" ) );
    assertEquals( "unexpected escaped characters",
        "A", f.parseString( "\\u0041" ) );
    assertEquals( "unexpected escaped characters",
        "AB", f.parseString( "\\u0041\\u0042" ) );

    try {
      f.parseString( "\\u000" );
      fail( "invalid length check." );
    }
    catch( RuntimeException expected ){}

    try {
      f.parseString( "\\u123x" );
      fail( "invalid hex number check." );
    }
    catch( NumberFormatException expected ){}
  }
}
