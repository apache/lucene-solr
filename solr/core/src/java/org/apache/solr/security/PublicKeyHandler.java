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

package org.apache.solr.security;

import com.google.common.annotations.VisibleForTesting;
import org.apache.solr.common.StringUtils;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.CryptoKeys;

import java.io.IOException;
import java.net.URL;
import java.security.spec.InvalidKeySpecException;

public class PublicKeyHandler extends RequestHandlerBase {
  public static final String PATH = "/admin/info/key";

  final CryptoKeys.RSAKeyPair keyPair;

  @VisibleForTesting
  public PublicKeyHandler() {
    keyPair = new CryptoKeys.RSAKeyPair();
  }

  public PublicKeyHandler(CloudConfig config) throws IOException, InvalidKeySpecException {
    keyPair = createKeyPair(config);
  }

  private CryptoKeys.RSAKeyPair createKeyPair(CloudConfig config) throws IOException, InvalidKeySpecException {
    if (config == null) {
      return new CryptoKeys.RSAKeyPair();
    }

    String publicKey = config.getPkiHandlerPublicKeyPath();
    String privateKey = config.getPkiHandlerPrivateKeyPath();

    // If both properties unset, then we fall back to generating a new key pair
    if (StringUtils.isEmpty(publicKey) && StringUtils.isEmpty(privateKey)) {
      return new CryptoKeys.RSAKeyPair();
    }

    return new CryptoKeys.RSAKeyPair(new URL(privateKey), new URL(publicKey));
  }

  public String getPublicKey() {
    return keyPair.getPublicKeyStr();
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    rsp.add("key", keyPair.getPublicKeyStr());
  }

  @Override
  public String getDescription() {
    return "Return the public key of this server";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }
}
