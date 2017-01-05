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
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BlobRepositoryMockingTest {

  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final String[][] PARSED = new String[][]{{"foo", "bar", "baz"}, {"bang", "boom", "bash"}};
  private static final String BLOBSTR = "foo,bar,baz\nbang,boom,bash";
  private CoreContainer mockContainer = EasyMock.createMock(CoreContainer.class);
  @SuppressWarnings("unchecked")
  private ConcurrentHashMap<String, BlobRepository.BlobContent> mapMock = EasyMock.createMock(ConcurrentHashMap.class);
  @SuppressWarnings("unchecked")
  private BlobRepository.Decoder<Object> decoderMock = EasyMock.createMock(BlobRepository.Decoder.class);;
  @SuppressWarnings("unchecked")
  private BlobRepository.BlobContent<Object> blobContentMock = EasyMock.createMock(BlobRepository.BlobContent.class);
  
  private Object[] mocks = new Object[] {
      mockContainer,
      decoderMock,
      blobContentMock,
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
    EasyMock.reset(mocks);
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

  @After
  public void tearDown() {
    EasyMock.verify(mocks);
  }

  @Test (expected = SolrException.class)
  public void testCloudOnly() {
    expect(mockContainer.isZooKeeperAware()).andReturn(false);
    EasyMock.replay(mocks);
    BlobRepository.BlobContentRef ref = repository.getBlobIncRef("foo!");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetBlobIncrRefString() {
    expect(mockContainer.isZooKeeperAware()).andReturn(true);
    expect(mapMock.get("foo!")).andReturn(null);
    expect(mapMock.put(eq("foo!"), anyObject(BlobRepository.BlobContent.class))).andReturn(null);
    EasyMock.replay(mocks);
    BlobRepository.BlobContentRef ref = repository.getBlobIncRef("foo!");
    assertTrue("foo!".equals(blobKey));
    assertTrue(blobFetched);
    assertNotNull(ref.blob);
    assertEquals(blobData, ref.blob.get());
  }  
  
  @SuppressWarnings("unchecked")
  @Test
  public void testCachedAlready() {
    expect(mockContainer.isZooKeeperAware()).andReturn(true);
    expect(mapMock.get("foo!")).andReturn(new BlobRepository.BlobContent<BlobRepository>("foo!", blobData));
    EasyMock.replay(mocks);
    BlobRepository.BlobContentRef ref = repository.getBlobIncRef("foo!");
    assertEquals("",blobKey);
    assertFalse(blobFetched);
    assertNotNull(ref.blob);
    assertEquals(blobData, ref.blob.get());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetBlobIncrRefStringDecoder() {
    expect(mockContainer.isZooKeeperAware()).andReturn(true);
    expect(mapMock.get("foo!mocked")).andReturn(null);
    expect(mapMock.put(eq("foo!mocked"), anyObject(BlobRepository.BlobContent.class))).andReturn(null);
    
    EasyMock.replay(mocks);
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
  }


}
