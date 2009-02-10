package org.apache.solr.update.processor;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MD5Signature extends Signature {
  protected final static Logger log = LoggerFactory.getLogger(MD5Signature.class);
  private static ThreadLocal<MessageDigest> DIGESTER_FACTORY = new ThreadLocal<MessageDigest>() {
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

  public void add(String content) {
    try {
      digester.update(content.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      // won't happen
      log.error("UTF-8 not supported", e);
      throw new RuntimeException(e);
    }
  }

  public byte[] getSignature() {
    return digester.digest();
  }
}
