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

package org.apache.solr.common.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.core.SolrResourceLoader;

/**
 */
public class ContentStreamTest extends LuceneTestCase 
{  
  public void testStringStream() throws IOException 
  {
    String input = "aads ghaskdgasgldj asl sadg ajdsg &jag # @ hjsakg hsakdg hjkas s";
    ContentStreamBase stream = new ContentStreamBase.StringStream( input );
    assertEquals( input.length(), stream.getSize().intValue() );
    assertEquals( input, IOUtils.toString( stream.getStream(), "UTF-8" ) );
    assertEquals( input, IOUtils.toString( stream.getReader() ) );
  }

  public void testFileStream() throws IOException 
  {
    InputStream is = new SolrResourceLoader(null, null).openResource( "solrj/README" );
    assertNotNull( is );
    File file = new File(TEMP_DIR, "README");
    FileOutputStream os = new FileOutputStream(file);
    IOUtils.copy(is, os);
    os.close();
    
    ContentStreamBase stream = new ContentStreamBase.FileStream( file );
    assertEquals( file.length(), stream.getSize().intValue() );
    assertTrue( IOUtils.contentEquals( new FileInputStream( file ), stream.getStream() ) );
    assertTrue( IOUtils.contentEquals( new InputStreamReader(new FileInputStream(file), "UTF-8"), stream.getReader() ) );
  }
  

  public void testURLStream() throws IOException 
  {
    InputStream is = new SolrResourceLoader(null, null).openResource( "solrj/README" );
    assertNotNull( is );
    File file = new File(TEMP_DIR, "README");
    FileOutputStream os = new FileOutputStream(file);
    IOUtils.copy(is, os);
    os.close();
    
    ContentStreamBase stream = new ContentStreamBase.URLStream( new URL(file.toURI().toASCIIString()) );
    assertTrue( IOUtils.contentEquals( new FileInputStream( file ), stream.getStream() ) );
    assertEquals( file.length(), stream.getSize().intValue() );
    assertTrue( IOUtils.contentEquals( new InputStreamReader(new FileInputStream(file), "UTF-8"), stream.getReader() ) );
    assertEquals( file.length(), stream.getSize().intValue() );
  }
}
