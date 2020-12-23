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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.function.Function;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.*;

/**
 * Tests {@link DelegatingDirectoryFactory}.
 */
public class TestDelegatingDirectoryFactory extends SolrTestCaseJ4 {

    private DirectoryFactory directoryFactory;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initCore("solrconfig-delegatingdirectory.xml", "schema.xml");
    }

    @Before
    public void before() {
        directoryFactory = DirectoryFactory.loadDirectoryFactory(solrConfig, h.getCoreContainer(), null);
    }

    @After
    public void after() throws Exception {
        if (directoryFactory != null) {
            directoryFactory.close();
        }
    }

    @Test
    public void testDelegation() throws Exception {
        // Verify config.
        assertThat(directoryFactory, is(instanceOf(DelegatingDirectoryFactoryForTest.class)));
        assertThat(((DelegatingDirectoryFactoryForTest) directoryFactory).getDelegate(), is(instanceOf(MMapDirectoryFactory.class)));

        Directory directory = directoryFactory.get(h.getCore().getIndexDir(), DirectoryFactory.DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_NATIVE);
        try {
            // Verify the created Directory is a custom one wrapping a MMapDirectory because we use a wrapping function
            // in DelegatingDirectoryFactoryForTest.
            assertThat(directory, is(instanceOf(DelegatingDirectoryForTest.class)));
            Directory delegateDirectory = ((DelegatingDirectoryForTest) directory).getDelegate();
            assertThat(delegateDirectory, is(instanceOf(MMapDirectory.class)));
            // Verify config params are passed to the delegate factory.
            assertThat(((MMapDirectory) delegateDirectory).getMaxChunkSize(), is(524288));
        } finally {
            directoryFactory.release(directory);
        }

        // Verify the delegation works with CachingDirectoryFactory: the cached Directory is the same instance.
        Directory directory2 = directoryFactory.get(h.getCore().getIndexDir(), DirectoryFactory.DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_NATIVE);
        try {
            assertThat(directory2, sameInstance(directory));
        } finally {
            directoryFactory.release(directory2);
            directoryFactory.doneWithDirectory(directory2);
        }
    }

    public static class DelegatingDirectoryFactoryForTest extends DelegatingDirectoryFactory {

        @Override
        public Directory get(String path, DirContext dirContext, String rawLockType, Function<Directory, Directory> wrappingFunction)
                throws IOException {
            return delegateFactory.get(path, dirContext, rawLockType, (dir) -> wrappingFunction.apply(new DelegatingDirectoryForTest(dir)));
        }
    }

    private static class DelegatingDirectoryForTest extends FilterDirectory {

        DelegatingDirectoryForTest(Directory delegate) {
            super(delegate);
        }
    }
}
