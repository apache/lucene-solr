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

package org.apache.lucene.gdata.servlet.handler;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.GDataAccount.AccountRole;
import org.apache.lucene.gdata.server.GDataRequest;
import org.apache.lucene.gdata.server.ServiceException;
import org.apache.lucene.gdata.server.ServiceFactory;
import org.apache.lucene.gdata.server.administration.AdminService;
import org.apache.lucene.gdata.server.authentication.AuthenticationController;
import org.apache.lucene.gdata.server.authentication.AuthenticatorException;
import org.apache.lucene.gdata.server.authentication.GDataHttpAuthenticator;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;

/**
 * The RequestAuthenticator provides access to the registered
 * {@link org.apache.lucene.gdata.server.authentication.AuthenticationController}
 * as a super class for all request handler requiereing authentication for
 * access. This class implements the
 * {@link org.apache.lucene.gdata.server.authentication.GDataHttpAuthenticator}
 * to get the auth token from the given request and call the needed Components
 * to authenticat the client.
 * <p>
 * For request handler handling common requests like entry insert or update the
 * authentication will be based on the account name verified as the owner of the
 * feed to alter. If the accountname in the token does not match the name of the
 * account which belongs to the feed the given role will be used for
 * autentication. Authentication using the
 * {@link RequestAuthenticator#authenticateAccount(HttpServletRequest, AccountRole)}
 * method, the account name will be ignored, authentication will be based on the
 * given <tt>AccountRole</tt>
 * </p>
 * 
 * @author Simon Willnauer
 * 
 */
public class RequestAuthenticator implements GDataHttpAuthenticator {
    private static final Log LOG = LogFactory
            .getLog(RequestAuthenticator.class);

    /**
     * @see org.apache.lucene.gdata.server.authentication.GDataHttpAuthenticator#authenticateAccount(org.apache.lucene.gdata.server.GDataRequest,
     *      org.apache.lucene.gdata.data.GDataAccount.AccountRole)
     */
    public boolean authenticateAccount(GDataRequest request, AccountRole role) {
        String clientIp = request.getRemoteAddress();
        if (LOG.isDebugEnabled())
            LOG
                    .debug("Authenticating Account for GDataRequest -- modifying entries -- Role: "
                            + role + "; ClientIp: " + clientIp);

        AuthenticationController controller = GDataServerRegistry.getRegistry()
                .lookup(AuthenticationController.class,
                        ComponentType.AUTHENTICATIONCONTROLLER);
        ServiceFactory factory = GDataServerRegistry.getRegistry().lookup(
                ServiceFactory.class, ComponentType.SERVICEFACTORY);
        AdminService adminService = factory.getAdminService();
        GDataAccount account;
        try {
            account = adminService.getFeedOwningAccount(request.getFeedId());
            String token = getTokenFromRequest(request.getHttpServletRequest());
            if (LOG.isDebugEnabled())
                LOG.debug("Got Token: " + token + "; for requesting account: "
                        + account);
            if (account != null && token != null)
                return controller.authenticateToken(token, clientIp,
                        AccountRole.ENTRYAMINISTRATOR, account.getName());

        } catch (ServiceException e) {
            LOG.error("can get GDataAccount for feedID -- "
                    + request.getFeedId(), e);
            throw new AuthenticatorException(" Service exception occured", e);

        }

        return false;
    }

    /**
     * @see org.apache.lucene.gdata.server.authentication.GDataHttpAuthenticator#authenticateAccount(javax.servlet.http.HttpServletRequest,
     *      org.apache.lucene.gdata.data.GDataAccount.AccountRole)
     */
    public boolean authenticateAccount(HttpServletRequest request,
            AccountRole role) {
        String clientIp = request.getRemoteAddr();
        if (LOG.isDebugEnabled())
            LOG
                    .debug("Authenticating Account for GDataRequest -- modifying entries -- Role: "
                            + role + "; ClientIp: " + clientIp);
        AuthenticationController controller = GDataServerRegistry.getRegistry()
                .lookup(AuthenticationController.class,
                        ComponentType.AUTHENTICATIONCONTROLLER);
        String token = getTokenFromRequest(request);
        if (LOG.isDebugEnabled())
            LOG.debug("Got Token: " + token + ";");
        if (token == null)
            return false;
        return controller.authenticateToken(token, clientIp, role, null);

    }

    protected String getTokenFromRequest(HttpServletRequest request) {
        String token = request
                .getHeader(AuthenticationController.AUTHORIZATION_HEADER);
        if (token == null || !token.startsWith("GoogleLogin")) {
            Cookie[] cookies = request.getCookies();
            if (cookies == null) {
                return null;
            }
            for (int i = 0; i < cookies.length; i++) {
                if (cookies[i].getName().equals(
                        AuthenticationController.TOKEN_KEY)) {
                    token = cookies[i].getValue();
                    break;
                }

            }
        }
        if (token != null)
            token = token.substring(token.indexOf("=") + 1);
        return token;
    }

}
