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

package org.eclipse.jetty.client;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.eclipse.jetty.client.api.AuthenticationStore;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.AbstractAuthentication;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.util.Attributes;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * <p>Implementation of the SPNEGO (or "Negotiate") authentication defined in RFC 4559.</p>
 * <p>A {@link #getUserName() user} is logged in via JAAS (either via userName/password or
 * via userName/keyTab) once only.</p>
 * <p>For every request that needs authentication, a {@link GSSContext} is initiated and
 * later established after reading the response from the server.</p>
 * <p>Applications should create objects of this class and add them to the
 * {@link AuthenticationStore} retrieved from the {@link HttpClient}
 * via {@link HttpClient#getAuthenticationStore()}.</p>
 */
public class SPNEGOAuthentication extends AbstractAuthentication
{
  private static final Logger LOG = Log.getLogger(SPNEGOAuthentication.class);
  private static final String NEGOTIATE = HttpHeader.NEGOTIATE.asString();

  private final GSSManager gssManager = GSSManager.getInstance();
  private String userName;
  private String userPassword;
  private Path userKeyTabPath;
  private String serviceName;
  private boolean useTicketCache;
  private Path ticketCachePath;
  private boolean renewTGT;

  public SPNEGOAuthentication(URI uri)
  {
    super(uri, ANY_REALM);
  }

  @Override
  public String getType()
  {
    return NEGOTIATE;
  }

  /**
   * @return the user name of the user to login
   */
  public String getUserName()
  {
    return userName;
  }

  /**
   * @param userName user name of the user to login
   */
  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  /**
   * @return the password of the user to login
   */
  public String getUserPassword()
  {
    return userPassword;
  }

  /**
   * @param userPassword the password of the user to login
   * @see #setUserKeyTabPath(Path)
   */
  public void setUserPassword(String userPassword)
  {
    this.userPassword = userPassword;
  }

  /**
   * @return the path of the keyTab file with the user credentials
   */
  public Path getUserKeyTabPath()
  {
    return userKeyTabPath;
  }

  /**
   * @param userKeyTabPath the path of the keyTab file with the user credentials
   * @see #setUserPassword(String)
   */
  public void setUserKeyTabPath(Path userKeyTabPath)
  {
    this.userKeyTabPath = userKeyTabPath;
  }

  /**
   * @return the name of the service to use
   */
  public String getServiceName()
  {
    return serviceName;
  }

  /**
   * @param serviceName the name of the service to use
   */
  public void setServiceName(String serviceName)
  {
    this.serviceName = serviceName;
  }

  /**
   * @return whether to use the ticket cache during login
   */
  public boolean isUseTicketCache()
  {
    return useTicketCache;
  }

  /**
   * @param useTicketCache whether to use the ticket cache during login
   * @see #setTicketCachePath(Path)
   */
  public void setUseTicketCache(boolean useTicketCache)
  {
    this.useTicketCache = useTicketCache;
  }

  /**
   * @return the path of the ticket cache file
   */
  public Path getTicketCachePath()
  {
    return ticketCachePath;
  }

  /**
   * @param ticketCachePath the path of the ticket cache file
   * @see #setUseTicketCache(boolean)
   */
  public void setTicketCachePath(Path ticketCachePath)
  {
    this.ticketCachePath = ticketCachePath;
  }

  /**
   * @return whether to renew the ticket granting ticket
   */
  public boolean isRenewTGT()
  {
    return renewTGT;
  }

  /**
   * @param renewTGT whether to renew the ticket granting ticket
   */
  public void setRenewTGT(boolean renewTGT)
  {
    this.renewTGT = renewTGT;
  }

  @Override
  public Result authenticate(Request request, ContentResponse response, HeaderInfo headerInfo, Attributes context)
  {
    SPNEGOContext spnegoContext = (SPNEGOContext)context.getAttribute(SPNEGOContext.ATTRIBUTE);
    if (LOG.isDebugEnabled())
      LOG.debug("Authenticate with context {}", spnegoContext);
    if (spnegoContext == null)
    {
      spnegoContext = login();
      context.setAttribute(SPNEGOContext.ATTRIBUTE, spnegoContext);
    }

    String b64Input = headerInfo.getBase64();
    byte[] input = b64Input == null ? new byte[0] : Base64.getDecoder().decode(b64Input);
    byte[] output = Subject.doAs(spnegoContext.subject, initGSSContext(spnegoContext, request.getHost(), input));
    String b64Output = output == null ? null : new String(Base64.getEncoder().encode(output), UTF_8);

    // The result cannot be used for subsequent requests,
    // so it always has a null URI to avoid being cached.
    return new SPNEGOResult(null, b64Output);
  }

  private SPNEGOContext login()
  {
    try
    {
      // First login via JAAS using the Kerberos AS_REQ call, with a client user.
      // This will populate the Subject with the client user principal and the TGT.
      String user = getUserName();
      if (LOG.isDebugEnabled())
        LOG.debug("Logging in user {}", user);
      CallbackHandler callbackHandler = new PasswordCallbackHandler();
      LoginContext loginContext = new LoginContext("", null, callbackHandler, new SPNEGOConfiguration());
      loginContext.login();
      Subject subject = loginContext.getSubject();

      SPNEGOContext spnegoContext = new SPNEGOContext();
      spnegoContext.subject = subject;
      if (LOG.isDebugEnabled())
        LOG.debug("Initialized {}", spnegoContext);
      return spnegoContext;
    }
    catch (LoginException x)
    {
      throw new RuntimeException(x);
    }
  }

  private PrivilegedAction<byte[]> initGSSContext(SPNEGOContext spnegoContext, String host, byte[] bytes)
  {
    return () ->
    {
      try
      {
        // The call to initSecContext with the service name will
        // trigger the Kerberos TGS_REQ call, asking for the SGT,
        // which will be added to the Subject credentials because
        // initSecContext() is called from within Subject.doAs().
        GSSContext gssContext = spnegoContext.gssContext;
        if (gssContext == null)
        {
          String principal = getServiceName() + "@" + host;
          GSSName serviceName = gssManager.createName(principal, GSSName.NT_HOSTBASED_SERVICE);
          Oid spnegoOid = new Oid("1.3.6.1.5.5.2");
          gssContext = gssManager.createContext(serviceName, spnegoOid, null, GSSContext.INDEFINITE_LIFETIME);
          spnegoContext.gssContext = gssContext;
          gssContext.requestMutualAuth(true);
        }
        byte[] result = gssContext.initSecContext(bytes, 0, bytes.length);
        if (LOG.isDebugEnabled())
          LOG.debug("{} {}", gssContext.isEstablished() ? "Initialized" : "Initializing", gssContext);
        return result;
      }
      catch (GSSException x)
      {
        throw new RuntimeException(x);
      }
    };
  }

  public static class SPNEGOResult implements Result
  {
    private final URI uri;
    private final HttpHeader header;
    private final String value;

    public SPNEGOResult(URI uri, String token)
    {
      this(uri, HttpHeader.AUTHORIZATION, token);
    }

    public SPNEGOResult(URI uri, HttpHeader header, String token)
    {
      this.uri = uri;
      this.header = header;
      this.value = NEGOTIATE + (token == null ? "" : " " + token);
    }

    @Override
    public URI getURI()
    {
      return uri;
    }

    @Override
    public void apply(Request request)
    {
      request.header(header, value);
    }
  }

  private static class SPNEGOContext
  {
    private static final String ATTRIBUTE = SPNEGOContext.class.getName();

    private Subject subject;
    private GSSContext gssContext;

    @Override
    public String toString()
    {
      return String.format(Locale.ROOT, "%s@%x[context=%s]", getClass().getSimpleName(), hashCode(), gssContext);
    }
  }

  private class PasswordCallbackHandler implements CallbackHandler
  {
    @Override
    public void handle(Callback[] callbacks) throws IOException
    {
      PasswordCallback callback = Arrays.stream(callbacks)
          .filter(PasswordCallback.class::isInstance)
          .map(PasswordCallback.class::cast)
          .findAny()
          .filter(c -> c.getPrompt().contains(getUserName()))
          .orElseThrow(IOException::new);
      callback.setPassword(getUserPassword().toCharArray());
    }
  }

  private class SPNEGOConfiguration extends Configuration
  {
    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name)
    {
      Map<String, Object> options = new HashMap<>();
      if (LOG.isDebugEnabled())
        options.put("debug", "true");
      options.put("refreshKrb5Config", "true");
      options.put("principal", getUserName());
      options.put("isInitiator", "true");
      Path keyTabPath = getUserKeyTabPath();
      if (keyTabPath != null)
      {
        options.put("doNotPrompt", "true");
        options.put("useKeyTab", "true");
        options.put("keyTab", keyTabPath.toAbsolutePath().toString());
        options.put("storeKey", "true");
      }
      boolean useTicketCache = isUseTicketCache();
      if (useTicketCache)
      {
        options.put("useTicketCache", "true");
        Path ticketCachePath = getTicketCachePath();
        if (ticketCachePath != null)
          options.put("ticketCache", ticketCachePath.toAbsolutePath().toString());
        options.put("renewTGT", String.valueOf(isRenewTGT()));
      }

      String moduleClass = "com.sun.security.auth.module.Krb5LoginModule";
      AppConfigurationEntry config = new AppConfigurationEntry(moduleClass, AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
      return new AppConfigurationEntry[]{config};
    }
  }
}
