package org.apache.solr.cloud;

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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.util.CryptoKeys;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCryptoKeys extends AbstractFullDistribZkTestBase {
  private static final Logger logger = LoggerFactory.getLogger(TestCryptoKeys.class);


  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
  }

  public TestCryptoKeys() {
    super();
    sliceCount = 1;
  }

  @Test
  public void test() throws Exception {
    String pk1sig = "G8LEW7uJ1is81Aqqfl3Sld3qDtOxPuVFeTLJHFJWecgDvUkmJNFXmf7nkHOVlXnDWahp1vqZf0W02VHXg37lBw==";
    String pk2sig = "pCyBQycB/0YvLVZfKLDIIqG1tFwM/awqzkp2QNpO7R3ThTqmmrj11wEJFDRLkY79efuFuQPHt40EE7jrOKoj9jLNELsfEqvU3jw9sZKiDONY+rV9Bj9QPeW8Pgt+F9Y1";
    String wrongKeySig = "xTk2hTipfpb+J5s4x3YZGOXkmHWtnJz05Vvd8RTm/Q1fbQVszR7vMk6dQ1URxX08fcg4HvxOo8g9bG2TSMOGjg==";
    String result = null;
    CryptoKeys cryptoKeys = null;
    SolrZkClient zk = getCommonCloudSolrClient().getZkStateReader().getZkClient();
    cryptoKeys = new CryptoKeys(CloudUtil.getTrustedKeys(zk));
    byte[] samplefile = readFile("samplefile.bin");
    //there are no keys

    result = cryptoKeys.verify( pk1sig,samplefile);
    assertNull(result);

    zk.makePath("/keys", true);

    createNode(zk, "pubk1.der");
    createNode(zk, "pubk2.der");

    Map<String, byte[]> trustedKeys = CloudUtil.getTrustedKeys(zk);


    cryptoKeys = new CryptoKeys(trustedKeys);


    result = cryptoKeys.verify(pk2sig, samplefile);
    assertEquals("pubk2.der", result);


    result = cryptoKeys.verify(pk1sig, samplefile);
    assertEquals("pubk1.der", result);

    try {
      result = cryptoKeys.verify(wrongKeySig,samplefile);
      assertNull(result);
    } catch (Exception e) {
      //pass
    }


    try {
      result = cryptoKeys.verify( "SGVsbG8gV29ybGQhCg==", samplefile);
      assertNull(result);
    } catch (Exception e) {
      //pass

    }


  }



  private void createNode(SolrZkClient zk, String fname) throws IOException, KeeperException, InterruptedException {
    byte[] buf = readFile(fname);
    zk.create("/keys/" + fname, buf, CreateMode.PERSISTENT, true);

  }

  private byte[] readFile(String fname) throws IOException {
    byte[] buf = null;
    try (FileInputStream fis = new FileInputStream(getFile("cryptokeys/" + fname))) {
      buf = new byte[fis.available()];
      fis.read(buf);
    }
    return buf;
  }


}
