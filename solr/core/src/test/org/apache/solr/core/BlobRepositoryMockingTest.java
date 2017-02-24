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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrException;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
