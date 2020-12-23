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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.LockFactory;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;
import java.util.function.Function;

/**
 * Delegates to a {@link DirectoryFactory} defined by the {@code delegateFactory} parameter.
 * <p>
 * Configured with the {@code delegateFactory} parameter which defines the full class name of the delegate
 * {@link DirectoryFactory}, plus any additional parameter for the delegate factory.</p>
 */
public class DelegatingDirectoryFactory extends DirectoryFactory {

    protected DirectoryFactory delegateFactory;

    public DirectoryFactory getDelegate() {
        return delegateFactory;
    }

    @Override
    public void initCoreContainer(CoreContainer cc) {
        super.initCoreContainer(cc);
        if (delegateFactory != null) {
            delegateFactory.initCoreContainer(cc);
        }
    }

    @Override
    public void init(@SuppressWarnings("rawtypes") NamedList args) {
        SolrParams params = args.toSolrParams();
        String delegateFactoryClass = params.get("delegateFactory");
        if (delegateFactoryClass == null) {
            throw new IllegalArgumentException("delegateFactory class is required");
        }
        DirectoryFactory delegateFactory = coreContainer.getResourceLoader().newInstance(delegateFactoryClass, DirectoryFactory.class);
        delegateFactory.initCoreContainer(coreContainer);
        delegateFactory.init(args);
        this.delegateFactory = delegateFactory;
    }

    @Override
    public void doneWithDirectory(Directory directory) throws IOException {
        delegateFactory.doneWithDirectory(directory);
    }

    @Override
    public void addCloseListener(Directory dir, CachingDirectoryFactory.CloseListener closeListener) {
        delegateFactory.addCloseListener(dir, closeListener);
    }

    @Override
    public void close() throws IOException {
        delegateFactory.close();
    }

    @Override
    protected Directory create(String path, LockFactory lockFactory, DirContext dirContext) throws IOException {
        return delegateFactory.create(path, lockFactory, dirContext);
    }

    @Override
    protected LockFactory createLockFactory(String rawLockType) throws IOException {
        return delegateFactory.createLockFactory(rawLockType);
    }

    @Override
    public boolean exists(String path) throws IOException {
        return delegateFactory.exists(path);
    }

    @Override
    public void remove(Directory dir) throws IOException {
        delegateFactory.remove(dir);
    }

    @Override
    public void remove(Directory dir, boolean afterCoreClose) throws IOException {
        delegateFactory.remove(dir, afterCoreClose);
    }

    @Override
    public void remove(String path, boolean afterCoreClose) throws IOException {
        delegateFactory.remove(path, afterCoreClose);
    }

    @Override
    public void remove(String path) throws IOException {
        delegateFactory.remove(path);
    }

    @Override
    public long size(Directory directory) throws IOException {
        return delegateFactory.size(directory);
    }

    @Override
    public long size(String path) throws IOException {
        return delegateFactory.size(path);
    }

    @Override
    public void move(Directory fromDir, Directory toDir, String fileName, IOContext ioContext) throws IOException {
        delegateFactory.move(fromDir, toDir, fileName, ioContext);
    }

    @Override
    public void renameWithOverwrite(Directory dir, String fileName, String toName) throws IOException {
        delegateFactory.renameWithOverwrite(dir, fileName, toName);
    }

    @Override
    public Directory get(String path, DirContext dirContext, String rawLockType, Function<Directory, Directory> wrappingFunction)
            throws IOException {
        return delegateFactory.get(path, dirContext, rawLockType, wrappingFunction);
    }

    @Override
    public void incRef(Directory directory) {
        delegateFactory.incRef(directory);
    }

    @Override
    public boolean isPersistent() {
        return delegateFactory.isPersistent();
    }

    @Override
    public boolean isSharedStorage() {
        return delegateFactory.isSharedStorage();
    }

    @Override
    public void release(Directory directory) throws IOException {
        delegateFactory.release(directory);
    }

    @Override
    public String normalize(String path) throws IOException {
        return delegateFactory.normalize(path);
    }

    @Override
    public boolean isAbsolute(String path) {
        return delegateFactory.isAbsolute(path);
    }

    @Override
    public boolean searchersReserveCommitPoints() {
        return delegateFactory.searchersReserveCommitPoints();
    }

    @Override
    public String getDataHome(CoreDescriptor cd) throws IOException {
        return delegateFactory.getDataHome(cd);
    }

    @Override
    public void cleanupOldIndexDirectories(String dataDirPath, String currentIndexDirPath, boolean afterCoreReload) {
        delegateFactory.cleanupOldIndexDirectories(dataDirPath, currentIndexDirPath, afterCoreReload);
    }

    @Override
    protected boolean deleteOldIndexDirectory(String oldDirPath) throws IOException {
        return delegateFactory.deleteOldIndexDirectory(oldDirPath);
    }

    @Override
    protected Directory getBaseDir(Directory dir) {
        return delegateFactory.getBaseDir(dir);
    }
}