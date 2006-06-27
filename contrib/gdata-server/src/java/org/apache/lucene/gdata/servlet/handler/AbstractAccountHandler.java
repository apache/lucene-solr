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

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.GDataAccount.AccountRole;
import org.apache.lucene.gdata.server.ServiceException;
import org.apache.lucene.gdata.server.ServiceFactory;
import org.apache.lucene.gdata.server.administration.AccountBuilder;
import org.apache.lucene.gdata.server.administration.AdminService;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.xml.sax.SAXException;

/**
 * @author Simon Willnauer
 * 
 */
public abstract class AbstractAccountHandler extends RequestAuthenticator
        implements GDataRequestHandler {
    private static final Log LOG = LogFactory
            .getLog(AbstractAccountHandler.class);

    private boolean authenticated = false;

    private int error;

    private String errorMessage = "";

    private boolean isError = false;

    protected AdminService service;

    /**
     * @see org.apache.lucene.gdata.servlet.handler.GDataRequestHandler#processRequest(javax.servlet.http.HttpServletRequest,
     *      javax.servlet.http.HttpServletResponse)
     */
    @SuppressWarnings("unused")
    public void processRequest(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
        
            this.authenticated = authenticateAccount(request,
                AccountRole.USERADMINISTRATOR);
        
        if (this.authenticated) {
            GDataServerRegistry registry = GDataServerRegistry.getRegistry();
            ServiceFactory factory = registry.lookup(ServiceFactory.class,
                    ComponentType.SERVICEFACTORY);
            try {

                GDataAccount account = getAccountFromRequest(request);
                if (!account.requiredValuesSet()) {
                    setError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                            "requiered server component not available");
                    throw new AccountHandlerException(
                            "requiered values are not set -- account can not be saved -- "
                                    + account);
                }
                this.service = factory.getAdminService();
                processServiceAction(account);
            } catch (ServiceException e) {
                LOG.error("Can't process account action -- " + e.getMessage(),
                        e);
                setError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "");
            } catch (Exception e) {
                LOG.error("Can't process account action -- " + e.getMessage(),
                        e);
            }
        }else{
            setError(HttpServletResponse.SC_UNAUTHORIZED,"Authorization failed");
        }
        sendResponse(response);

    }
    
    
    

    protected GDataAccount getAccountFromRequest(
            final HttpServletRequest request) throws AccountHandlerException {
        try {
            GDataAccount account = AccountBuilder.buildAccount(request
                    .getReader());
            if (account == null) {
                setError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "");
                throw new AccountHandlerException(
                        "unexpected value -- parsed account is null");
            }
            return account;
        } catch (IOException e) {
            setError(HttpServletResponse.SC_BAD_REQUEST, "can not read input");
            throw new AccountHandlerException("Can't read from request reader",
                    e);
        } catch (SAXException e) {
            setError(HttpServletResponse.SC_BAD_REQUEST,
                    "can not parse gdata account");
            throw new AccountHandlerException(
                    "Can not parse incoming gdata account", e);
        }
    }

    protected void sendResponse(HttpServletResponse response) {

        if (!this.isError)
            return;
        try {
            response.sendError(this.error, this.errorMessage);
        } catch (IOException e) {
            LOG.warn("can send error in RequestHandler ", e);
        }
    }

    protected void setError(int error, String message) {
        this.error = error;
        this.errorMessage = message;
        this.isError = true;
    }

    protected int getErrorCode() {
        return this.error;
    }

    protected String getErrorMessage() {
        return this.errorMessage;
    }

    protected abstract void processServiceAction(final GDataAccount account)
            throws ServiceException;

    static class AccountHandlerException extends Exception {

        /**
         * 
         */
        private static final long serialVersionUID = 3140463271122190694L;

        /**
         * Constructs a new <tt>AccountHandlerException</tt> with an exception
         * message and the exception caused this exception.
         * 
         * @param arg0 -
         *            the exception message
         * @param arg1 -
         *            the exception cause
         */
        public AccountHandlerException(String arg0, Throwable arg1) {
            super(arg0, arg1);

        }

        /**
         * Constructs a new <tt>AccountHandlerException</tt> with an exception
         * message.
         * 
         * @param arg0 -
         *            the exception message
         */
        public AccountHandlerException(String arg0) {
            super(arg0);

        }

    }
}
