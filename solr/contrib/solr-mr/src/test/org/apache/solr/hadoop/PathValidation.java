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
package org.apache.solr.hadoop;

import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class PathValidation extends MRUnitBase {
  
  @Test
  public void testPath() {
    Path path = new Path("hdfs://c2202.mycompany.com:8020/user/foo/bar.txt");
    assertEquals("/user/foo/bar.txt", path.toUri().getPath());
    assertEquals("bar.txt", path.getName());
    assertEquals("hdfs", path.toUri().getScheme());
    assertEquals("c2202.mycompany.com:8020", path.toUri().getAuthority());
    
    path = new Path("/user/foo/bar.txt");
    assertEquals("/user/foo/bar.txt", path.toUri().getPath());
    assertEquals("bar.txt", path.getName());
    assertEquals(null, path.toUri().getScheme());
    assertEquals(null, path.toUri().getAuthority());
    
    assertEquals("-", new Path("-").toString());
  }
  
  @Test
  public void testRegex() {
    Pattern regex = Pattern.compile("text/plain|text/html");
    assertTrue(regex.matcher("text/plain").matches());    
    assertTrue(regex.matcher("text/html").matches());    
    assertFalse(regex.matcher("xxtext/html").matches());    
  }
  
}
