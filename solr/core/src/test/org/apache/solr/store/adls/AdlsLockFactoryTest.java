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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestRuleLimitSysouts;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.AdlsDirectoryFactory;
import org.apache.solr.core.DirectoryFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@TestRuleLimitSysouts.Limit(bytes=100000)
public class AdlsLockFactoryTest extends LuceneTestCase {



  @Test
  public void testBasic() throws IOException {
    //MockAdlsProvider mocker = new MockAdlsProvider();


    AdlsDirectoryFactory factory = new AdlsDirectoryFactory();
    NamedList args =new NamedList();
    args.add(AdlsDirectoryFactory.ADLS_PROVIDER,MockAdlsProvider.class.getName());
    args.add(AdlsDirectoryFactory.ADLS_HOME,"adl://www.lovefoobar.com/lovefoobar/solrrocks");
    args.add(AdlsDirectoryFactory.ADLS_OAUTH_CLIENT_ID,"plzgivemeaclient");
    args.add(AdlsDirectoryFactory.ADLS_OAUTH_REFRESH_URL,"www.foo.com");
    args.add(AdlsDirectoryFactory.ADLS_OAUTH_CREDENTIAL,"credz");
    args.add(AdlsDirectoryFactory.NRTCACHINGDIRECTORY_ENABLE,"false");
    args.add(AdlsDirectoryFactory.BLOCKCACHE_ENABLED,"false");
    factory.init(args);


    //DirContext dirContext, String rawLockType

    Directory dir =
        factory.get("/lovefoobar/solrrocks/medir", DirectoryFactory.DirContext.DEFAULT,"ADLS");

    try (Lock lock = dir.obtainLock("testlock")) {
      assert lock != null;
      try (Lock lock2 = dir.obtainLock("testlock")) {
        assert lock2 != null;
        Assert.fail("Locking should fail");
      } catch (LockObtainFailedException lofe) {
        // pass
      }
    }

    // now repeat after close()
    try (Lock lock = dir.obtainLock("testlock")) {
      assert lock != null;
      try (Lock lock2 = dir.obtainLock("testlock")) {
        assert lock2 != null;
        Assert.fail("Locking should fail");
      } catch (LockObtainFailedException lofe) {
        // pass
      }
    }
    ((MockAdlsProvider)factory.getProvider()).cleanUp();
  }
}
