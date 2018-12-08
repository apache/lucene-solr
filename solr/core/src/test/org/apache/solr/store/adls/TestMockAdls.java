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

package org.apache.solr.store.adls;

import java.io.File;
import java.nio.charset.Charset;
import java.util.UUID;

import com.microsoft.azure.datalake.store.ADLFileInputStream;
import com.microsoft.azure.datalake.store.ADLFileOutputStream;
import com.microsoft.azure.datalake.store.IfExists;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestRuleLimitSysouts;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@TestRuleLimitSysouts.Limit(bytes=100000)
public class TestMockAdls extends LuceneTestCase{

  MockAdlsProvider mocker = new MockAdlsProvider();

  String tempDir=new File("target/"+ UUID.randomUUID().toString()).getAbsolutePath();

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    mocker.cleanUp();
  }

  @Test
  public void testMock() {
    try {
      WrappedADLFileOutputStream writer = mocker.createFile("/foo.txt", IfExists.FAIL);
      writer.write("LINE1\n".getBytes(Charset.forName("UTF-8")));
      writer.flush();
      writer.write("LINE2".getBytes(Charset.forName("UTF-8")));
      writer.flush();
      writer.close();

      ADLFileInputStream input = mocker.getReadStream("/foo.txt");
      String val = IOUtils.toString(input,Charset.forName("UTF-8"));
      Assert.assertEquals("LINE1\nLINE2",val);
      input.close();

      writer = mocker.getAppendStream("/foo.txt");
      writer.write("\nline3".getBytes(Charset.forName("UTF-8")));
      writer.close();

      input = mocker.getReadStream("/foo.txt");
      val = IOUtils.toString(input,Charset.forName("UTF-8"));
      Assert.assertEquals("LINE1\nLINE2\nline3",val);
      input.close();

      Assert.assertTrue(mocker.createDirectory("/testfoo"));
      writer = mocker.createFile("/testfoo/foo.txt", IfExists.FAIL);
      writer.write("LINE1\n".getBytes(Charset.forName("UTF-8")));
      writer.flush();
      writer.write("LINE2".getBytes(Charset.forName("UTF-8")));
      writer.flush();
      writer.close();

      Assert.assertEquals(mocker.getDirectoryEntry("/testfoo/foo.txt").fullName,"/testfoo/foo.txt");
      Assert.assertEquals(mocker.getDirectoryEntry("/testfoo/foo.txt").length,11);


    }catch (Exception e){
      e.printStackTrace();

    } finally {
      mocker.cleanUp();
    }

  }
}
