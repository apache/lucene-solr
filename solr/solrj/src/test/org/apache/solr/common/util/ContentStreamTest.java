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
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrResourceLoader;

/**
 * Tests {@link ContentStream} such as "stream.file".
 */
public class ContentStreamTest extends SolrTestCaseJ4 {

  public void testStringStream() throws IOException {
    String input = "aads ghaskdgasgldj asl sadg ajdsg &jag # @ hjsakg hsakdg hjkas s";
    ContentStreamBase stream = new ContentStreamBase.StringStream(input);
    assertEquals(input.length(), stream.getSize().intValue());
    assertEquals(input, IOUtils.toString(stream.getStream(), "UTF-8"));
    assertEquals(input, IOUtils.toString(stream.getReader()));
  }

  public void testFileStream() throws IOException {
    File file = new File(createTempDir().toFile(), "README");
    try (SolrResourceLoader srl = new SolrResourceLoader(Paths.get("").toAbsolutePath());
         InputStream is = srl.openResource("solrj/README");
         FileOutputStream os = new FileOutputStream(file)) {
      assertNotNull(is);
      IOUtils.copy(is, os);
    }

    ContentStreamBase stream = new ContentStreamBase.FileStream(file);
    try (InputStream s = stream.getStream();
         FileInputStream fis = new FileInputStream(file);
         InputStreamReader isr = new InputStreamReader(
             new FileInputStream(file), StandardCharsets.UTF_8);
         Reader r = stream.getReader()) {
      assertEquals(file.length(), stream.getSize().intValue());
      // Test the code that sets content based on < being the 1st character
      assertEquals("application/xml", stream.getContentType());
      assertTrue(IOUtils.contentEquals(fis, s));
      assertTrue(IOUtils.contentEquals(isr, r));
    }
  }

  public void testFileStreamGZIP() throws IOException {
    File file = new File(createTempDir().toFile(), "README.gz");

    try (SolrResourceLoader srl = new SolrResourceLoader(Paths.get("").toAbsolutePath());
         InputStream is = srl.openResource("solrj/README");
         FileOutputStream os = new FileOutputStream(file);
         GZIPOutputStream zos = new GZIPOutputStream(os)) {
      IOUtils.copy(is, zos);
    }

    ContentStreamBase stream = new ContentStreamBase.FileStream(file);
    try (InputStream s = stream.getStream();
         FileInputStream fis = new FileInputStream(file);
         GZIPInputStream zis = new GZIPInputStream(fis);
         InputStreamReader isr = new InputStreamReader(zis, StandardCharsets.UTF_8);
         FileInputStream fis2 = new FileInputStream(file);
         GZIPInputStream zis2 = new GZIPInputStream(fis2);
         Reader r = stream.getReader()) {
      assertEquals(file.length(), stream.getSize().intValue());
      // Test the code that sets content based on < being the 1st character
      assertEquals("application/xml", stream.getContentType());
      assertTrue(IOUtils.contentEquals(isr, r));
      assertTrue(IOUtils.contentEquals(zis2, s));
    }
  }

  public void testURLStream() throws IOException {
    File file = new File(createTempDir().toFile(), "README");

    try (SolrResourceLoader srl = new SolrResourceLoader(Paths.get("").toAbsolutePath());
         InputStream is = srl.openResource("solrj/README");
         FileOutputStream os = new FileOutputStream(file)) {
      IOUtils.copy(is, os);
    }

    ContentStreamBase stream = new ContentStreamBase.URLStream(new URL(file.toURI().toASCIIString()));

    try (InputStream s = stream.getStream();
         FileInputStream fis = new FileInputStream(file);
         FileInputStream fis2 = new FileInputStream(file);
         InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
         Reader r = stream.getReader()) {
      // For File URLs, the content type is determined automatically by the mime type
      // associated with the file extension,
      // This is inconsistent from the FileStream as that code tries to guess the content based on the 1st character.
      //
      // HTTP URLS, the content type is determined by the headers.  Those are not tested here.
      //
      assertEquals("text/html", stream.getContentType());
      assertTrue(IOUtils.contentEquals(fis2, s));
      assertEquals(file.length(), stream.getSize().intValue());
      assertTrue(IOUtils.contentEquals(isr, r));
      assertEquals(file.length(), stream.getSize().intValue());
    }
  }

  public void testURLStreamGZIP() throws IOException {
    File file = new File(createTempDir().toFile(), "README.gz");

    try (SolrResourceLoader srl = new SolrResourceLoader(Paths.get("").toAbsolutePath());
         InputStream is = srl.openResource("solrj/README");
         FileOutputStream os = new FileOutputStream(file);
         GZIPOutputStream zos = new GZIPOutputStream(os)) {
      IOUtils.copy(is, zos);
    }

    ContentStreamBase stream = new ContentStreamBase.URLStream(new URL(file.toURI().toASCIIString()));
    try (InputStream s = stream.getStream();
         FileInputStream fis = new FileInputStream(file);
         GZIPInputStream zis = new GZIPInputStream(fis);
         InputStreamReader isr = new InputStreamReader(zis, StandardCharsets.UTF_8);
         FileInputStream fis2 = new FileInputStream(file);
         GZIPInputStream zis2 = new GZIPInputStream(fis2);
         Reader r = stream.getReader()) {
      // See the non-GZIP test case for an explanation of header handling.
      assertEquals("application/xml", stream.getContentType());
      assertTrue(IOUtils.contentEquals(isr, r));
      assertTrue(IOUtils.contentEquals(zis2, s));
      assertEquals(file.length(), stream.getSize().intValue());
    }
  }

  public void testURLStreamCSVGZIPExtention() throws IOException {
    File file = new File(createTempDir().toFile(), "README.CSV.gz");

    try (SolrResourceLoader srl = new SolrResourceLoader(Paths.get("").toAbsolutePath());
         InputStream is = srl.openResource("solrj/README");
         FileOutputStream os = new FileOutputStream(file);
         GZIPOutputStream zos = new GZIPOutputStream(os)) {
      IOUtils.copy(is, zos);
    }

    ContentStreamBase stream = new ContentStreamBase.URLStream(new URL(file.toURI().toASCIIString()));
    try (InputStream s = stream.getStream();
         FileInputStream fis = new FileInputStream(file);
         GZIPInputStream zis = new GZIPInputStream(fis);
         InputStreamReader isr = new InputStreamReader(zis, StandardCharsets.UTF_8);
         FileInputStream fis2 = new FileInputStream(file);
         GZIPInputStream zis2 = new GZIPInputStream(fis2);
         Reader r = stream.getReader()) {
      // See the non-GZIP test case for an explanation of header handling.
      assertEquals("text/csv", stream.getContentType());
      assertTrue(IOUtils.contentEquals(isr, r));
      assertTrue(IOUtils.contentEquals(zis2, s));
      assertEquals(file.length(), stream.getSize().intValue());
    }
  }

  public void testURLStreamJSONGZIPExtention() throws IOException {
    File file = new File(createTempDir().toFile(), "README.json.gzip");

    try (SolrResourceLoader srl = new SolrResourceLoader(Paths.get("").toAbsolutePath());
         InputStream is = srl.openResource("solrj/README");
         FileOutputStream os = new FileOutputStream(file);
         GZIPOutputStream zos = new GZIPOutputStream(os)) {
      IOUtils.copy(is, zos);
    }

    ContentStreamBase stream = new ContentStreamBase.URLStream(new URL(file.toURI().toASCIIString()));
    try (InputStream s = stream.getStream();
         FileInputStream fis = new FileInputStream(file);
         GZIPInputStream zis = new GZIPInputStream(fis);
         InputStreamReader isr = new InputStreamReader(zis, StandardCharsets.UTF_8);
         FileInputStream fis2 = new FileInputStream(file);
         GZIPInputStream zis2 = new GZIPInputStream(fis2);
         Reader r = stream.getReader()) {
      // See the non-GZIP test case for an explanation of header handling.
      assertEquals("application/json", stream.getContentType());
      assertTrue(IOUtils.contentEquals(isr, r));
      assertTrue(IOUtils.contentEquals(zis2, s));
      assertEquals(file.length(), stream.getSize().intValue());
    }
  }
}
