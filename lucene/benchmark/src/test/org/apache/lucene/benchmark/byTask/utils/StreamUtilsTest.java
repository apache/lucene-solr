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
package org.apache.lucene.benchmark.byTask.utils;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.lucene.benchmark.BenchmarkTestCase;
import org.junit.Before;
import org.junit.Test;

public class StreamUtilsTest extends BenchmarkTestCase {
  private static final String TEXT = "Some-Text..."; 
  private Path testDir;
  
  @Test
  public void testGetInputStreamPlainText() throws Exception {
    assertReadText(rawTextFile("txt"));
    assertReadText(rawTextFile("TXT"));
  }

  @Test
  public void testGetInputStreamGzip() throws Exception {
    assertReadText(rawGzipFile("gz"));
    assertReadText(rawGzipFile("gzip"));
    assertReadText(rawGzipFile("GZ"));
    assertReadText(rawGzipFile("GZIP"));
  }

  @Test
  public void testGetInputStreamBzip2() throws Exception {
    assertReadText(rawBzip2File("bz2"));
    assertReadText(rawBzip2File("bzip"));
    assertReadText(rawBzip2File("BZ2"));
    assertReadText(rawBzip2File("BZIP"));
  }

  @Test
  public void testGetOutputStreamBzip2() throws Exception {
    assertReadText(autoOutFile("bz2"));
    assertReadText(autoOutFile("bzip"));
    assertReadText(autoOutFile("BZ2"));
    assertReadText(autoOutFile("BZIP"));
  }
  
  @Test
  public void testGetOutputStreamGzip() throws Exception {
    assertReadText(autoOutFile("gz"));
    assertReadText(autoOutFile("gzip"));
    assertReadText(autoOutFile("GZ"));
    assertReadText(autoOutFile("GZIP"));
  }

  @Test
  public void testGetOutputStreamPlain() throws Exception {
    assertReadText(autoOutFile("txt"));
    assertReadText(autoOutFile("text"));
    assertReadText(autoOutFile("TXT"));
    assertReadText(autoOutFile("TEXT"));
  }
  
  private Path rawTextFile(String ext) throws Exception {
    Path f = testDir.resolve("testfile." +  ext);
    BufferedWriter w = Files.newBufferedWriter(f, StandardCharsets.UTF_8);
    w.write(TEXT);
    w.newLine();
    w.close();
    return f;
  }
  
  private Path rawGzipFile(String ext) throws Exception {
    Path f = testDir.resolve("testfile." +  ext);
    OutputStream os = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP, Files.newOutputStream(f));
    writeText(os);
    return f;
  }

  private Path rawBzip2File(String ext) throws Exception {
    Path f = testDir.resolve("testfile." +  ext);
    OutputStream os = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, Files.newOutputStream(f));
    writeText(os);
    return f;
  }

  private Path autoOutFile(String ext) throws Exception {
    Path f = testDir.resolve("testfile." +  ext);
    OutputStream os = StreamUtils.outputStream(f);
    writeText(os);
    return f;
  }

  private void writeText(OutputStream os) throws IOException {
    BufferedWriter w = new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
    w.write(TEXT);
    w.newLine();
    w.close();
  }

  private void assertReadText(Path f) throws Exception {
    InputStream ir = StreamUtils.inputStream(f);
    InputStreamReader in = new InputStreamReader(ir, StandardCharsets.UTF_8);
    BufferedReader r = new BufferedReader(in);
    String line = r.readLine();
    assertEquals("Wrong text found in "+f.getFileName(), TEXT, line);
    r.close();
  }
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    testDir = createTempDir("ContentSourceTest");
  }
}
