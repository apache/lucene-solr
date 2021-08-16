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
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.Checksum;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.BackupRepositoryFactory;

public class TrackingBackupRepository implements BackupRepository {
    private static final List<URI> COPIED_FILES = Collections.synchronizedList(new ArrayList<>());

    private BackupRepository delegate;

    @Override
    public <T> T getConfigProperty(String name) {
        return delegate.getConfigProperty(name);
    }

    @Override
    public URI createURI(String path) {
        return delegate.createURI(path);
    }

    @Override
    public URI createDirectoryURI(String path) {
        return delegate.createDirectoryURI(path);
    }

    @Override
    public URI resolve(URI baseUri, String... pathComponents) {
        return delegate.resolve(baseUri, pathComponents);
    }

    @Override
    public URI resolveDirectory(URI baseUri, String... pathComponents) {
        return delegate.resolveDirectory(baseUri, pathComponents);
    }

    @Override
    public boolean exists(URI path) throws IOException {
        return delegate.exists(path);
    }

    @Override
    public PathType getPathType(URI path) throws IOException {
        return delegate.getPathType(path);
    }

    @Override
    public String[] listAll(URI path) throws IOException {
        return delegate.listAll(path);
    }

    @Override
    public IndexInput openInput(URI dirPath, String fileName, IOContext ctx) throws IOException {
        return delegate.openInput(dirPath, fileName, ctx);
    }

    @Override
    public OutputStream createOutput(URI path) throws IOException {
        return delegate.createOutput(path);
    }

    @Override
    public void createDirectory(URI path) throws IOException {
        delegate.createDirectory(path);
    }

    @Override
    public void deleteDirectory(URI path) throws IOException {
        delegate.deleteDirectory(path);
    }

    @Override
    public void copyIndexFileFrom(Directory sourceDir, String sourceFileName, URI destDir, String destFileName) throws IOException {
        COPIED_FILES.add(delegate.resolve(destDir, destFileName));
        delegate.copyIndexFileFrom(sourceDir, sourceFileName, destDir, destFileName);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }


    @Override
    public void delete(URI path, Collection<String> files, boolean ignoreNoSuchFileException) throws IOException {
        delegate.delete(path, files, ignoreNoSuchFileException);
    }

    @Override
    public Checksum checksum(Directory dir, String fileName) throws IOException {
        return delegate.checksum(dir, fileName);
    }

    @Override
    public void init(@SuppressWarnings("rawtypes") NamedList args) {
        BackupRepositoryFactory factory = (BackupRepositoryFactory) args.get("factory");
        SolrResourceLoader loader = (SolrResourceLoader) args.get("loader");
        String repoName = (String) args.get("delegateRepoName");

        this.delegate = factory.newInstance(loader, repoName);
    }

    /**
     * @return list of files were copied by using {@link #copyFileFrom(Directory, String, URI)}
     */
    public static List<URI> copiedFiles() {
        return new ArrayList<>(COPIED_FILES);
    }

    /**
     * Clear all tracking data
     */
    public static void clear() {
        COPIED_FILES.clear();
    }

    @Override
    public void copyIndexFileTo(URI sourceRepo, String sourceFileName, Directory dest, String destFileName) throws IOException {
        delegate.copyIndexFileTo(sourceRepo, sourceFileName, dest, destFileName);
    }
}
