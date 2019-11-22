// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.solr.handler.dataimport;

import org.slf4j.LoggerFactory;
import org.apache.commons.codec.DecoderException;
import javax.crypto.spec.SecretKeySpec;
import org.apache.solr.common.StringUtils;
import org.apache.commons.codec.binary.Hex;
import java.security.spec.AlgorithmParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.Cipher;
import org.slf4j.Logger;
import java.security.Key;
import java.security.SecureRandom;

public class AESCipher
{
    private SecureRandom rand;
    private static final Integer IV_LENGTH;
    private static final String IV_SEPARATOR = "#";
    private Key key;
    private static final Logger LOGGER;
    private static final byte[] DEFAULT_IV;
    private static final String ALGORITHM = "AES/CBC/PKCS5Padding";
    
    public AESCipher(final String keyStr) {
        this.rand = new SecureRandom();
        this.key = this.stringToKey(keyStr);
    }
    
    public String encrypt(final String message) {
        if (message == null) {
            throw new IllegalArgumentException("message is null");
        }
        if (this.key == null) {
            throw new IllegalStateException("key is null");
        }
        try {
            final Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            final byte[] iv = this.generateIV(AESCipher.IV_LENGTH);
            final IvParameterSpec ivspec = new IvParameterSpec(iv);
            cipher.init(1, this.key, ivspec);
            return new String(Hex.encodeHex(iv)) + "#" + new String(Hex.encodeHex(cipher.doFinal(message.getBytes("UTF-8"))));
        }
        catch (Exception e) {
            throw new RuntimeException("Encrypt exception: ", e);
        }
    }
    
    public String decrypt(final String encryptedMessage) {
        if (encryptedMessage == null) {
            throw new IllegalArgumentException("encryptedMessage is null");
        }
        if (this.key == null) {
            throw new IllegalStateException("key is null");
        }
        try {
            final Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            String[] info = encryptedMessage.split("#");
            if (info.length != 2) {
                info = new String[] { new String(Hex.encodeHex(AESCipher.DEFAULT_IV)), encryptedMessage };
            }
            final IvParameterSpec ivspec = new IvParameterSpec(Hex.decodeHex(info[0].toCharArray()));
            cipher.init(2, this.key, ivspec);
            return new String(cipher.doFinal(Hex.decodeHex(info[1].toCharArray())), "UTF-8");
        }
        catch (Exception e) {
            throw new RuntimeException("Decrypt exception : ", e);
        }
    }
    
    public Key stringToKey(final String keyStr) {
        if (StringUtils.isEmpty(keyStr)) {
            AESCipher.LOGGER.error("keyStr is empty.");
            throw new RuntimeException("keyStr is empty.");
        }
        try {
            final byte[] bytes = Hex.decodeHex(keyStr.toCharArray());
            return new SecretKeySpec(bytes, 0, bytes.length, "AES");
        }
        catch (DecoderException decodeException) {
            throw new RuntimeException("Key String to AES key: ", (Throwable)decodeException);
        }
    }
    
    private byte[] generateIV(final int length) {
        final byte[] bytes = new byte[length];
        this.rand.nextBytes(bytes);
        return bytes;
    }
    
    static {
        IV_LENGTH = 16;
        LOGGER = LoggerFactory.getLogger((Class)AESCipher.class);
        DEFAULT_IV = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    }
}
