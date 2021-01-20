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
package org.apache.solr.cloud;

import org.apache.solr.SolrTestCase;
import org.apache.solr.util.CryptoKeys;
import org.junit.Test;

import java.net.URL;
import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

public class TestRSAKeyPair extends SolrTestCase {
    @Test
    public void testGenKeyPair() throws Exception {
        testRoundTrip(new CryptoKeys.RSAKeyPair());
    }

    @Test
    public void testReadKeysFromDisk() throws Exception {
        URL privateKey = getClass().getClassLoader().getResource("cryptokeys/priv_key512_pkcs8.pem");
        URL publicKey = getClass().getClassLoader().getResource("cryptokeys/pub_key512.der");

        testRoundTrip(new CryptoKeys.RSAKeyPair(privateKey, publicKey));
    }

    private void testRoundTrip(CryptoKeys.RSAKeyPair kp) throws Exception {
        final byte[] plaintext = new byte[random().nextInt(64)];
        random().nextBytes(plaintext);

        byte[] encrypted = kp.encrypt(ByteBuffer.wrap(plaintext));
        assertThat(plaintext, not(equalTo(encrypted)));

        byte[] decrypted = CryptoKeys.decryptRSA(encrypted, kp.getPublicKey());

        assertTrue("Decrypted text is shorter than original text.", decrypted.length >= plaintext.length);

        // Pad with null bytes because RSAKeyPair uses RSA/ECB/NoPadding
        int pad = decrypted.length - plaintext.length;
        final byte[] padded = new byte[decrypted.length];
        System.arraycopy(plaintext, 0, padded, pad, plaintext.length);
        assertArrayEquals(padded, decrypted);
    }
}
