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
package org.apache.solr.store.adls;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;

import com.microsoft.azure.datalake.store.ADLException;
import com.microsoft.azure.datalake.store.IfExists;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockObtainFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdlsLockFactory extends LockFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static final AdlsLockFactory INSTANCE = new AdlsLockFactory();

  private AdlsLockFactory() {}
  
  @Override
  public Lock obtainLock(Directory dir, String lockName) throws IOException {
    if (!(dir instanceof AdlsDirectory)) {
      throw new UnsupportedOperationException("AdlsLockFactory can only be used with AdlsDirectory subclasses, got: " + dir);
    }

    final AdlsDirectory adlsDir = (AdlsDirectory) dir;

    String lockDir=adlsDir.getAdlsDirPath();
    String lockPath=lockDir+"/"+lockName;

    // this will throw an exeception if the file exists
    try (OutputStream out = adlsDir.getClient().createFile(lockPath, IfExists.FAIL);){
      return new AdlsLock(adlsDir.getClient(),lockPath);
    } catch (ADLException e){
      throw new LockObtainFailedException("Cannot obtain lock file: " + lockPath, e);
    }
  }
  
  private static final class AdlsLock extends Lock {

    private final String lockFile;
    private final AdlsProvider client;
    private volatile boolean closed;

    AdlsLock(AdlsProvider client, String lockFile) {
      this.lockFile = lockFile;
      this.client=client;
    }
    
    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }

      this.closed=true;

      if (client.checkExists(lockFile)){
        client.delete(lockFile);
      }
    }

    @Override
    public void ensureValid() throws IOException {
      // no idea how to implement this on ADLS
    }

    @Override
    public String toString() {
      return "AdlsLock(lockFile=" + lockFile + ")";
    }
  }
}
