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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * Factory to instantiate {@link org.apache.lucene.store.ByteBuffersDirectory}
 */
public class ByteBuffersDirectoryFactory extends EphemeralDirectoryFactory {

  @Override
  protected LockFactory createLockFactory(String rawLockType) throws IOException {
    if (!(rawLockType == null || DirectoryFactory.LOCK_TYPE_SINGLE.equalsIgnoreCase(rawLockType.trim()))) {
      throw new SolrException(ErrorCode.FORBIDDEN,
          "ByteBuffersDirectory can only be used with the '"+DirectoryFactory.LOCK_TYPE_SINGLE+"' lock factory type.");
    }
    return new SingleInstanceLockFactory();
  }

  @Override
  protected Directory create(String path, LockFactory lockFactory, DirContext dirContext) throws IOException {
    return new ByteBuffersDirectory(lockFactory);
  }
}
