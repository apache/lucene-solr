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
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.core.SolrResourceLoader;

/**
 */
public class ContentStreamTest extends SolrTestCaseJ4 
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
    File file = new File(createTempDir().toFile(), "README");
    FileOutputStream os = new FileOutputStream(file);
    IOUtils.copy(is, os);
    os.close();
    is.close();
    
    ContentStreamBase stream = new ContentStreamBase.FileStream(file);
    InputStream s = stream.getStream();
    FileInputStream fis = new FileInputStream(file);
    InputStreamReader isr = new InputStreamReader(
        new FileInputStream(file), StandardCharsets.UTF_8);
    Reader r = stream.getReader();
    try {
      assertEquals(file.length(), stream.getSize().intValue());
      assertTrue(IOUtils.contentEquals(fis, s));
      assertTrue(IOUtils.contentEquals(isr, r));
    } finally {
      s.close();
      r.close();
      isr.close();
      fis.close();
    }
  }
  

  public void testURLStream() throws IOException 
  {
    InputStream is = new SolrResourceLoader(null, null).openResource( "solrj/README" );
    assertNotNull( is );
    File file = new File(createTempDir().toFile(), "README");
    FileOutputStream os = new FileOutputStream(file);
    IOUtils.copy(is, os);
    os.close();
    is.close();
    
    ContentStreamBase stream = new ContentStreamBase.URLStream(new URL(file
        .toURI().toASCIIString()));
    InputStream s = stream.getStream();
    FileInputStream fis = new FileInputStream(file);
    FileInputStream fis2 = new FileInputStream(file);
    InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
    Reader r = stream.getReader();
    try {
      assertTrue(IOUtils.contentEquals(fis2, s));
      assertEquals(file.length(), stream.getSize().intValue());
      assertTrue(IOUtils.contentEquals(isr, r));
      assertEquals(file.length(), stream.getSize().intValue());
    } finally {
      r.close();
      s.close();
      isr.close();
      fis.close();
      fis2.close();
    }
  }
}
