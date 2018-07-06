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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.NoSuchFileException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import junit.framework.Assert;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestRuleLimitSysouts;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.AdlsDirectoryFactory;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@TestRuleLimitSysouts.Limit(bytes=100000)
public class TestAdlsDirectory extends LuceneTestCase{
  private static final int MAX_NUMBER_OF_WRITES = 10000;
  private static final int MIN_FILE_SIZE = 100;
  private static final int MAX_FILE_SIZE = 100000;
  private static final int MIN_BUFFER_SIZE = 1;
  private static final int MAX_BUFFER_SIZE = 5000;
  private static final int MAX_NUMBER_OF_READS = 10000;
  private AdlsDirectory directory;
  private AdlsDirectoryFactory factory = new AdlsDirectoryFactory();

  
  @Before
  public void setUp() throws Exception {
    super.setUp();
    NamedList args =new NamedList();
    args.add(AdlsDirectoryFactory.ADLS_PROVIDER,MockAdlsProvider.class.getName());
    args.add(AdlsDirectoryFactory.ADLS_HOME,"adl://www.lovefoobar.com/lovefoobar/solrrocks");
    args.add(AdlsDirectoryFactory.ADLS_OAUTH_CLIENT_ID,"plzgivemeaclient");
    args.add(AdlsDirectoryFactory.ADLS_OAUTH_REFRESH_URL,"www.foo.com");

    args.add(AdlsDirectoryFactory.ADLS_OAUTH_CREDENTIAL,"credz");
    args.add(AdlsDirectoryFactory.NRTCACHINGDIRECTORY_ENABLE,"false");
    args.add(AdlsDirectoryFactory.BLOCKCACHE_ENABLED,"false");
    factory.init(args);
    directory= (AdlsDirectory) factory.get("/somerandomcrap", DirectoryFactory.DirContext.DEFAULT,"ADLS");

    // turn off loggging for mock web server, which uses the java logging API
    Class loggerClass = Class.forName("java.util.logging.Logger");
    Method loggerMethod = loggerClass.getDeclaredMethod("getLogger", String.class);
    Object logger = loggerMethod.invoke(null,MockWebServer.class.getName());

    //java.util.logging.Level
    Class myLevelClass = Class.forName(StringUtils.reverse("leveL.gniggol.litu.avaj"));
    Field f = myLevelClass.getDeclaredField("OFF");
    Object noLoggingValue=f.get(null);
    Object val = f.get(null);
    Method noLogging = loggerClass.getDeclaredMethod("setLevel", myLevelClass);
    noLogging.invoke(logger,noLoggingValue);
  }



  @After
  public void tearDown() throws Exception {
    super.tearDown();
    ((MockAdlsProvider)factory.getProvider()).cleanUp();
  }

  @Test
  public void testWritingAndReadingAFile() throws IOException {
    String[] listAll = directory.listAll();
    for (String file : listAll) {
      directory.deleteFile(file);
    }

    IndexOutput output = directory.createOutput("testing.test", new IOContext());
    output.writeInt(12345);
    output.close();

    IndexInput input = directory.openInput("testing.test", new IOContext());
    Assert.assertEquals(12345, input.readInt());
    input.close();

    listAll = directory.listAll();
    Assert.assertEquals(1, listAll.length);
    Assert.assertEquals("testing.test", listAll[0]);

    Assert.assertEquals(4, directory.fileLength("testing.test"));

    IndexInput input1 = directory.openInput("testing.test", new IOContext());

    IndexInput input2 = (IndexInput) input1.clone();
    Assert.assertEquals(12345, input2.readInt());
    input2.close();

    Assert.assertEquals(12345, input1.readInt());
    input1.close();

    Assert.assertFalse(slowFileExists(directory, "testing.test.other"));
    Assert.assertTrue(slowFileExists(directory, "testing.test"));
    directory.deleteFile("testing.test");
    Assert.assertFalse(slowFileExists(directory, "testing.test"));
  }

  public void testRename() throws IOException {
    String[] listAll = directory.listAll();
    for (String file : listAll) {
      directory.deleteFile(file);
    }

    IndexOutput output = directory.createOutput("testing.test", new IOContext());
    output.writeInt(12345);
    output.close();
    directory.rename("testing.test", "testing.test.renamed");
    Assert.assertFalse(slowFileExists(directory, "testing.test"));
    Assert.assertTrue(slowFileExists(directory, "testing.test.renamed"));
    IndexInput input = directory.openInput("testing.test.renamed", new IOContext());
    Assert.assertEquals(12345, input.readInt());
    Assert.assertEquals(input.getFilePointer(), input.length());
    input.close();
    directory.deleteFile("testing.test.renamed");
    Assert.assertFalse(slowFileExists(directory, "testing.test.renamed"));
  }

  @Test
  @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  public void testEOF() throws IOException {
    Directory fsDir = new RAMDirectory();
    String name = "test.eof";
    createFile(name, fsDir, directory);
    long fsLength = fsDir.fileLength(name);
    long adlsLength = directory.fileLength(name);
    Assert.assertEquals(fsLength, adlsLength);
    testEof(name,fsDir,fsLength);
    testEof(name,directory,adlsLength);
  }

  private void testEof(String name, Directory directory, long length) throws IOException {
    IndexInput input = directory.openInput(name, new IOContext());
    input.seek(length);
    try {
      input.readByte();
      Assert.fail("should throw eof");
    } catch (IOException e) {
    }
  }

  @Test
  public void testRandomAccessWrites() throws IOException {
    int i = 0;
    try {
      Set<String> names = new HashSet<>();
      for (; i< 10; i++) {
        Directory fsDir = new RAMDirectory();
        String name = getName1();
        System.out.println("Working on pass [" + i  +"] contains [" + names.contains(name) + "]");
        names.add(name);
        createFile(name,fsDir,directory);
        assertInputsEquals(name,fsDir,directory);
        fsDir.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Test failed on pass [" + i + "]");
    }
  }

  public String getName1() {
    return UUID.randomUUID().toString();
  }

  private void assertInputsEquals(String name, Directory fsDir, AdlsDirectory adls) throws IOException {
    int reads = random().nextInt(MAX_NUMBER_OF_READS);
    IndexInput fsInput = fsDir.openInput(name,new IOContext());
    IndexInput adlsInput = adls.openInput(name,new IOContext());
    Assert.assertEquals(fsInput.length(), adlsInput.length());
    int fileLength = (int) fsInput.length();
    for (int i = 0; i < reads; i++) {
      int nextInt = Math.min(MAX_BUFFER_SIZE - MIN_BUFFER_SIZE,fileLength);
      byte[] fsBuf = new byte[random().nextInt(nextInt > 0 ? nextInt : 1) + MIN_BUFFER_SIZE];
      byte[] adlsBuf = new byte[fsBuf.length];
      int offset = random().nextInt(fsBuf.length);

      nextInt = fsBuf.length - offset;
      int length = random().nextInt(nextInt > 0 ? nextInt : 1);
      nextInt = fileLength - length;
      int pos = random().nextInt(nextInt > 0 ? nextInt : 1);
      fsInput.seek(pos);
      fsInput.readBytes(fsBuf, offset, length);
      adlsInput.seek(pos);
      adlsInput.readBytes(adlsBuf, offset, length);
      for (int f = offset; f < length; f++) {
        if (fsBuf[f] != adlsBuf[f]) {
          Assert.fail();
        }
      }
    }
    fsInput.close();
    adlsInput.close();
  }

  private void createFile(String name, Directory fsDir, AdlsDirectory adls) throws IOException {
    int writes = random().nextInt(MAX_NUMBER_OF_WRITES);
    int fileLength = random().nextInt(MAX_FILE_SIZE - MIN_FILE_SIZE) + MIN_FILE_SIZE;
    IndexOutput fsOutput = fsDir.createOutput(name, new IOContext());
    IndexOutput adlsOutput = adls.createOutput(name, new IOContext());
    for (int i = 0; i < writes; i++) {
      byte[] buf = new byte[random().nextInt(Math.min(MAX_BUFFER_SIZE - MIN_BUFFER_SIZE,fileLength)) + MIN_BUFFER_SIZE];
      random().nextBytes(buf);
      int offset = random().nextInt(buf.length);
      int length = random().nextInt(buf.length - offset);
      fsOutput.writeBytes(buf, offset, length);
      adlsOutput.writeBytes(buf, offset, length);
    }
    fsOutput.close();
    adlsOutput.close();
  }


  /** Returns true if the file exists (can be opened), false
   *  if it cannot be opened, and (unlike Java's
   *  File.exists) throws IOException if there's some
   *  unexpected error. */
  public static boolean slowFileExists(Directory dir, String fileName) throws IOException {
    try {
      dir.openInput(fileName, IOContext.DEFAULT).close();
      return true;
    } catch (NoSuchFileException | FileNotFoundException e) {
      return false;
    }
  }


}
