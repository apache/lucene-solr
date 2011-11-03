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

package org.apache.solr.common.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.ConnectException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.util.ContentStreamBase;
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
    assertTrue( IOUtils.contentEquals( new FileReader(      file ), stream.getReader() ) );
  }
  

  public void testURLStream() throws IOException 
  {
    byte[] content = null;
    String contentType = null;
    URL url = new URL( "http://svn.apache.org/repos/asf/lucene/dev/trunk/" );
    InputStream in = null;
    try {
      URLConnection conn = url.openConnection();
      in = conn.getInputStream();
      contentType = conn.getContentType();
      content = IOUtils.toByteArray(in);

      assumeTrue("not enough content for test to be useful",
                 content.length > 10 ); 

    } catch (IOException ex) {
      assumeNoException("Unable to connect to " + url + " to run the test.", ex);
    }finally {
      if (in != null) {
        IOUtils.closeQuietly(in);
      }
    }
    
    
    ContentStreamBase stream = new ContentStreamBase.URLStream( url );
    in = stream.getStream();  // getStream is needed before getSize is valid
    assertEquals( content.length, stream.getSize().intValue() );
    
    try {
      assertTrue( IOUtils.contentEquals( 
          new ByteArrayInputStream(content), in ) );
    } 
    finally {
      IOUtils.closeQuietly(in);
    }

    String charset = ContentStreamBase.getCharsetFromContentType(contentType);
    if (charset == null)
      charset = ContentStreamBase.DEFAULT_CHARSET;
    // Re-open the stream and this time use a reader
    stream = new ContentStreamBase.URLStream( url );
    assertTrue( IOUtils.contentEquals( new StringReader(new String(content, charset)), stream.getReader() ) );
  }
}
