/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.gdata.server.authentication;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.Provider;
import java.security.Security;
import java.util.StringTokenizer;
import java.util.concurrent.locks.ReentrantLock;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.GDataAccount.AccountRole;
import org.apache.lucene.gdata.server.registry.Component;
import org.apache.lucene.gdata.server.registry.ComponentType;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

/**
 * A
 * {@link org.apache.lucene.gdata.server.authentication.AuthenticationController}
 * implmentation using a <i>Blowfish</i> algorithmn to en/decrpyt the
 * authentification token. The <i>Blowfish</i> algorithmn enables a stateless
 * authetication of the client. The token contains all information to
 * authenticate the client on possible other hosts.
 * <p>
 * The token contains the first 32 bit of the client ip (e.g. 192.168.0),
 * account name, {@link org.apache.lucene.gdata.data.GDataAccount.AccountRole}
 * and the cration time as a timestamp. The timestamp will be checked on every
 * subsequent request. If the timestamp plus the configured timeout is less
 * than the current time the client has to reauthenticate again.
 * </p>
 * <p>
 * The auth token returned by the
 * {@link BlowfishAuthenticationController#authenticatAccount(GDataAccount, String)}
 * method is a BASE64 encoded string.
 * </p>
 * 
 * @see javax.crypto.Cipher
 * @see sun.misc.BASE64Encoder
 * @see sun.misc.BASE64Decoder
 * @author Simon Willnauer
 * 
 */
@Component(componentType = ComponentType.AUTHENTICATIONCONTROLLER)
public class BlowfishAuthenticationController implements
        AuthenticationController {
    private static final Log LOG = LogFactory
            .getLog(BlowfishAuthenticationController.class);

    private static final String ALG = "Blowfish";

    private static final String TOKEN_LIMITER = "#";

    private static final String ENCODING = "UTF-8";

    private Cipher deCrypt;

    private Cipher enCrypt;

    // TODO make this configurable
    private int minuteOffset = 30;

    private long milisecondOffset;

    private BASE64Encoder encoder = new BASE64Encoder();

    private BASE64Decoder decoder = new BASE64Decoder();

    private ReentrantLock lock = new ReentrantLock();

    // TODO make this configurable
    private String key = "myTestKey";

    /**
     * @see org.apache.lucene.gdata.server.authentication.AuthenticationController#initialize()
     */
    public void initialize() {
        if (this.key == null)
            throw new IllegalArgumentException("Auth key must not be null");
        if (this.key.length() < 5 || this.key.length() > 16)
            throw new IllegalArgumentException(
                    "Auth key length must be greater than 4 and less than 17");

        try {
            Provider sunJce = new com.sun.crypto.provider.SunJCE();
            Security.addProvider(sunJce);
            KeyGenerator kgen = KeyGenerator.getInstance(ALG);
            kgen.init(448); // 448 Bit^M
            byte[] raw = this.key.getBytes();
            SecretKeySpec skeySpec = new SecretKeySpec(raw, ALG);
            this.deCrypt = Cipher.getInstance(ALG);
            this.enCrypt = Cipher.getInstance(ALG);
            this.deCrypt.init(Cipher.DECRYPT_MODE, skeySpec);
            this.enCrypt.init(Cipher.ENCRYPT_MODE, skeySpec);
        } catch (Exception e) {
            throw new AuthenticatorException(
                    "Can't initialize BlowfishAuthenticationController -- "
                            + e.getMessage(), e);

        }
        calculateTimeOffset();
    }

    /**
     * @see org.apache.lucene.gdata.server.authentication.AuthenticationController#authenticatAccount(org.apache.lucene.gdata.data.GDataAccount,
     *      java.lang.String)
     */
    public String authenticatAccount(GDataAccount account, String requestIp) {
        try {
            String passIp = requestIp.substring(0, requestIp.lastIndexOf('.'));
            String role = Integer.toString(account.getRolesAsInt());

            return calculateAuthToken(passIp, role, account.getName());
        } catch (Exception e) {
            throw new AuthenticatorException("Can not authenticat account -- "
                    + e.getMessage(), e);

        }
    }

    /**
     * @see org.apache.lucene.gdata.server.authentication.AuthenticationController#authenticateToken(java.lang.String,
     *      java.lang.String,
     *      org.apache.lucene.gdata.data.GDataAccount.AccountRole,
     *      java.lang.String)
     */
    public boolean authenticateToken(final String token,
            final String requestIp, AccountRole role, String accountName) {
        if (LOG.isInfoEnabled())
            LOG.info("authenticate Token " + token + " for requestIp: "
                    + requestIp);
        if (token == null || requestIp == null)
            return false;
        String passIp = requestIp.substring(0, requestIp.lastIndexOf('.'));
        String authString = null;
        try {
            authString = deCryptAuthToken(token);
        } catch (Exception e) {
            throw new AuthenticatorException("Can not decrypt token -- "
                    + e.getMessage(), e);
        }
        if (authString == null)
            return false;
        try {
            StringTokenizer tokenizer = new StringTokenizer(authString,
                    TOKEN_LIMITER);
            if (!tokenizer.nextToken().equals(passIp))
                return false;
            String tempAccountName = tokenizer.nextToken();
            int intRole = Integer.parseInt(tokenizer.nextToken());
            /*
             * Authentication goes either for a account role or a account. For
             * entry manipulation the account name will be retrieved by the
             * feedId otherwise it will be null If it is null the authentication
             * goes against the account role
             */
            if (tempAccountName == null
                    || (!tempAccountName.equals(accountName) && !GDataAccount
                            .isInRole(intRole, role)))
                return false;
            long timeout = Long.parseLong(tokenizer.nextToken());

            return (timeout + this.milisecondOffset) > System
                    .currentTimeMillis();
        } catch (Exception e) {
            LOG.error("Error occured while encrypting token " + e.getMessage(),
                    e);
            return false;
        }

    }

    private void calculateTimeOffset() {
        this.milisecondOffset = this.minuteOffset * 60 * 1000;
    }

    protected String calculateAuthToken(final String ipAddress,
            final String role, String accountName)
            throws IllegalBlockSizeException, BadPaddingException,
            UnsupportedEncodingException {
        StringBuilder builder = new StringBuilder();
        builder.append(ipAddress).append(TOKEN_LIMITER);
        builder.append(accountName).append(TOKEN_LIMITER);
        builder.append(role).append(TOKEN_LIMITER);
        builder.append(System.currentTimeMillis());

        this.lock.lock();
        try {
            byte[] toencode = builder.toString().getBytes(ENCODING);
            byte[] result = this.enCrypt.doFinal(toencode);
            return this.encoder.encode(result);
        } finally {
            this.lock.unlock();

        }

    }

    protected String deCryptAuthToken(final String authToken)
            throws IOException, IllegalBlockSizeException, BadPaddingException {
        this.lock.lock();
        try {
            byte[] input = this.decoder.decodeBuffer(authToken);
            byte[] result = this.deCrypt.doFinal(input);
            return new String(result, ENCODING);
        } finally {
            this.lock.unlock();
        }

    }

    /**
     * @return Returns the minuteOffset.
     */
    public int getMinuteOffset() {
        return this.minuteOffset;
    }

    /**
     * @param minuteOffset
     *            The minuteOffset to set.
     */
    public void setMinuteOffset(int minuteOffset) {
        this.minuteOffset = minuteOffset;
        calculateTimeOffset();
    }

    /**
     * @return Returns the key.
     */
    public String getKey() {
        return this.key;
    }

    /**
     * @param key
     *            The key to set.
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ServerComponent#destroy()
     */
    public void destroy() {
        //
    }

}
