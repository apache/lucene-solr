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

package org.apache.solr.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BlobRepositoryMockingTest {

  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final String[][] PARSED = new String[][]{{"foo", "bar", "baz"}, {"bang", "boom", "bash"}};
  private static final String BLOBSTR = "foo,bar,baz\nbang,boom,bash";
  private CoreContainer mockContainer = mock(CoreContainer.class);
  @SuppressWarnings("unchecked")
  private ConcurrentHashMap<String, BlobRepository.BlobContent> mapMock = mock(ConcurrentHashMap.class);
  
  private Object[] mocks = new Object[] {
      mockContainer,
      mapMock
  };
  
  BlobRepository repository;
  ByteBuffer blobData = ByteBuffer.wrap(BLOBSTR.getBytes(UTF8));
  boolean blobFetched = false;
  String blobKey = "";
  String url = null;
  String sha256 = null;
  ByteBuffer filecontent = null;
  
  @BeforeClass
  public static void beforeClass() {
    SolrTestCaseJ4.assumeWorkingMockito();
  }

  @Before
  public void setUp() throws IllegalAccessException, NoSuchFieldException {
    blobFetched = false;
    blobKey = "";
    reset(mocks);
    repository = new BlobRepository(mockContainer) {
      @Override
      ByteBuffer fetchBlob(String key) {
        blobKey = key;
        blobFetched = true;
        return blobData;
      }

      @Override
      ByteBuffer fetchFromUrl(String key, String url) {
        if(!Objects.equals(url, BlobRepositoryMockingTest.this.url)) return null;
        blobKey = key;
        blobFetched = true;
        return filecontent;
      }

      @Override
      BlobContentRef getBlobIncRef(String key, Decoder decoder, String url, String sha256) {
        if(!Objects.equals(sha256, BlobRepositoryMockingTest.this.sha256)) return null;
        blobKey = key;
        blobFetched = true;
        return new BlobContentRef(new BlobContent(key, filecontent)) ;
      }

      @Override
      ConcurrentHashMap<String, BlobContent> createMap() {
        return mapMock;
      }

    };
  }

  @Test (expected = SolrException.class)
  public void testCloudOnly() {
    when(mockContainer.isZooKeeperAware()).thenReturn(false);
    try {
      BlobRepository.BlobContentRef ref = repository.getBlobIncRef("foo!");
    } catch (SolrException e) {
      verify(mockContainer).isZooKeeperAware();
      throw e;
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetBlobIncrRefString() {
    when(mockContainer.isZooKeeperAware()).thenReturn(true);
    BlobRepository.BlobContentRef ref = repository.getBlobIncRef("foo!");
    assertTrue("foo!".equals(blobKey));
    assertTrue(blobFetched);
    assertNotNull(ref.blob);
    assertEquals(blobData, ref.blob.get());
    verify(mockContainer).isZooKeeperAware();
    verify(mapMock).get("foo!");
    verify(mapMock).put(eq("foo!"), any(BlobRepository.BlobContent.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetBlobIncrRefByUrl() throws Exception{
    when(mockContainer.isZooKeeperAware()).thenReturn(true);
    filecontent = TestDynamicLoading.getFileContent("runtimecode/runtimelibs_v2.jar.bin");
    url = "http://localhost:8080/myjar/location.jar";
    sha256 = "79298d7d5c3e60d91154efe7d72f4536eac46698edfa22ab894b85492d562ed4";
    BlobRepository.BlobContentRef ref = repository.getBlobIncRef( "filefoo",null,url,
        "79298d7d5c3e60d91154efe7d72f4536eac46698edfa22ab894b85492d562ed4");
    assertTrue("filefoo".equals(blobKey));
    assertTrue(blobFetched);
    assertNotNull(ref.blob);
    assertEquals(filecontent, ref.blob.get());
    url = null;
    filecontent = null;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCachedAlready() {
    when(mockContainer.isZooKeeperAware()).thenReturn(true);
    when(mapMock.get("foo!")).thenReturn(new BlobRepository.BlobContent<BlobRepository>("foo!", blobData));
    BlobRepository.BlobContentRef ref = repository.getBlobIncRef("foo!");
    assertEquals("",blobKey);
    assertFalse(blobFetched);
    assertNotNull(ref.blob);
    assertEquals(blobData, ref.blob.get());
    verify(mockContainer).isZooKeeperAware();
    verify(mapMock).get("foo!");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetBlobIncrRefStringDecoder() {
    when(mockContainer.isZooKeeperAware()).thenReturn(true);
    BlobRepository.BlobContentRef ref = repository.getBlobIncRef("foo!", new BlobRepository.Decoder<Object>() {
      @Override
      public Object decode(InputStream inputStream) {
        StringWriter writer = new StringWriter();
        try {
          IOUtils.copy(inputStream, writer, UTF8);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        
        assertEquals(BLOBSTR, writer.toString());
        return PARSED;
      }

      @Override
      public String getName() {
        return "mocked";
      }
    });
    assertEquals("foo!",blobKey);
    assertTrue(blobFetched);
    assertNotNull(ref.blob);
    assertEquals(PARSED, ref.blob.get());
    verify(mockContainer).isZooKeeperAware();
    verify(mapMock).get("foo!mocked");
    verify(mapMock).put(eq("foo!mocked"), any(BlobRepository.BlobContent.class));
  }


}
