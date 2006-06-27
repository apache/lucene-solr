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

import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.GDataAccount.AccountRole;
import org.apache.lucene.gdata.server.registry.ServerComponent;

/**
 * Implementations of the AuthenticationController interface contain all the
 * logic for processing token based authentification. A token is an encoded
 * unique <tt>String</tt> value passed back to the client if successfully
 * authenticated. Clients provide account name, password, the requested service
 * and the name of the application used for accessing the the gdata service.
 * <p>
 * The algorithmn to create and reauthenticate the token can be choosen by the
 * implementor. <br/> This interface extends
 * {@link org.apache.lucene.gdata.server.registry.ServerComponent} e.g.
 * implementing classes can be registered as a
 * {@link org.apache.lucene.gdata.server.registry.Component} in the
 * {@link org.apache.lucene.gdata.server.registry.GDataServerRegistry} to be
 * accessed via the provided lookup service
 * </p>
 * 
 * @see org.apache.lucene.gdata.server.authentication.BlowfishAuthenticationController
 * @author Simon Willnauer
 * 
 */
public interface AuthenticationController extends ServerComponent {

    /**
     * The header name containing the authentication token provided by the
     * client
     */
    public static final String AUTHORIZATION_HEADER = "Authorization";

    /**
     * Authentication parameter for the account name. Provided by the client to
     * recieve the auth token.
     */
    public static final String ACCOUNT_PARAMETER = "Email";

    /**
     * Authentication parameter for the account password. Provided by the client
     * to recieve the auth token.
     */
    public static final String PASSWORD_PARAMETER = "Passwd";

    /**
     * Authentication parameter for the requested service. Provided by the
     * client to recieve the auth token.
     */
    public static final String SERVICE_PARAMETER = "service";

    /**
     * Authentication parameter for the application name of the clients
     * application. This is just used for loggin purposes
     */
    public static final String APPLICATION_PARAMETER = "source";

    /**
     * The key used for respond the auth token to the client. Either as a cookie
     * (key as cookie name) or as plain response (TOKEN_KEY=TOKEN)
     */
    public final static String TOKEN_KEY = "Auth";

    /**
     * Creates a authentication token for the given account. The token will be
     * calculated based on a part of the clients ip address, the account role
     * and the account name and the time in millisecond at the point of
     * creation.
     * 
     * @param account -
     *            the account to create the token for
     * @param requestIp -
     *            the clients request ip address
     * @return - a BASE64 encoded authentification token
     */
    public abstract String authenticatAccount(GDataAccount account,
            String requestIp);

    /**
     * Authenticates the given auth token and checks the given parameter for
     * matching the information contained inside the token.
     * <p>
     * if the given account name is <code>null</code> the authentication will
     * ignore the account name and the decision whether the token is valid or
     * not will be based on the given role compared to the role inside the token
     * </p>
     * 
     * @param token -
     *            the token to authenticate
     * @param requestIp -
     *            the client request IP address
     * @param role -
     *            the required role
     * @param accountName -
     *            the name of the account
     * @return <code>true</code> if the given values match the values inside
     *         the token and if the timestamp plus the configured timeout is
     *         greater than the current time, if one of the values does not
     *         match or the token has timed out it will return
     *         <code>false</code>
     */
    public abstract boolean authenticateToken(final String token,
            final String requestIp, AccountRole role, String accountName);

}