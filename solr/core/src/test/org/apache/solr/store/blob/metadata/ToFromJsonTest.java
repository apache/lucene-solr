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
package org.apache.solr.store.blob.metadata;

import org.junit.Assert;
import org.junit.Test;

import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadataBuilder;
import org.apache.solr.store.blob.client.ToFromJson;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Unit tests for {@link ToFromJson}. Is not in the same package for access to {@link BlobCoreMetadata}.
 */
public class ToFromJsonTest extends Assert {
    final String CORE_NAME = "myCoreNameJsonTest";

    @Test
    public void jsonCoreMetadataNoFiles() throws Exception {
        BlobCoreMetadata bcm = new BlobCoreMetadataBuilder(CORE_NAME, 666L).build();

        verifyToJsonAndBack(bcm);
    }

    @Test
    public void jsonCoreMetadataFile() throws Exception {
        BlobCoreMetadataBuilder bb = new BlobCoreMetadataBuilder(CORE_NAME, 777L);

        BlobCoreMetadata bcm = bb.addFile(new BlobCoreMetadata.BlobFile("solrFilename", "blobFilename", 123000L, 100L)).build();

        verifyToJsonAndBack(bcm);
    }

    @Test
    public void jsonCoreMetadataMultiFiles() throws Exception {
        BlobCoreMetadataBuilder bb = new BlobCoreMetadataBuilder(CORE_NAME, 123L);
        Set<BlobCoreMetadata.BlobFile> files = new HashSet<>(Arrays.asList(
                new BlobCoreMetadata.BlobFile("solrFilename11", "blobFilename11", 1234L, 100L),
                new BlobCoreMetadata.BlobFile("solrFilename21", "blobFilename21", 2345L, 200L),
                new BlobCoreMetadata.BlobFile("solrFilename31", "blobFilename31", 3456L, 200L),
                new BlobCoreMetadata.BlobFile("solrFilename41", "blobFilename41", 4567L, 400L)
        ));
        for (BlobCoreMetadata.BlobFile f : files) {
            bb.addFile(f);
        }

        // Note the builder does not necessarily keep the added files in order.
        BlobCoreMetadata bcm = bb.build();

        verifyToJsonAndBack(bcm);

        assertEquals("blob core metadata should have core name specified to builder", CORE_NAME, bcm.getSharedBlobName());
        assertEquals("blob core metadata should have generation specified to builder",123L, bcm.getGeneration());

        // Files are not necessarily in the same order
        assertEquals("blob core metadata should have file set specified to builder", files, new HashSet<>(Arrays.asList(bcm.getBlobFiles())));
    }


    private void verifyToJsonAndBack(BlobCoreMetadata b) throws Exception {
        ToFromJson<BlobCoreMetadata> converter = new ToFromJson<>();
        String json = converter.toJson(b);
        BlobCoreMetadata b2 = converter.fromJson(json, BlobCoreMetadata.class);
        assertEquals("Conversion from blob metadata to json and back expected to return an equal object", b, b2);
    }
}
