package org.apache.solr.update.processor;
/**
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

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MD5Signature extends Signature {
  protected final static Logger log = LoggerFactory.getLogger(MD5Signature.class);
  private static ThreadLocal<MessageDigest> DIGESTER_FACTORY = new ThreadLocal<MessageDigest>() {
    @Override
    protected MessageDigest initialValue() {
      try {
        return MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    }
  };
  private MessageDigest digester;

  public MD5Signature() {
    digester = DIGESTER_FACTORY.get();
    digester.reset();
  }

  @Override
  public void add(String content) {
    try {
      digester.update(content.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      // won't happen
      log.error("UTF-8 not supported", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte[] getSignature() {
    return digester.digest();
  }
}
