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

package org.apache.solr.util.configuration;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.util.configuration.providers.EnvSSLCredentialProvider;
import org.apache.solr.util.configuration.providers.HadoopSSLCredentialProvider;
import org.apache.solr.util.configuration.providers.SysPropSSLCredentialProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class responsible to build SSL credential providers
 */
public class SSLCredentialProviderFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String DEFAULT_PROVIDER_CHAIN = "env;sysprop";
  public static final String PROVIDER_CHAIN_KEY = "solr.ssl.credential.provider.chain";

  @SuppressWarnings({"rawtypes"})
  private final static ImmutableMap<String, Class> defaultProviders = ImmutableMap.of(
      "env", EnvSSLCredentialProvider.class,
      "sysprop", SysPropSSLCredentialProvider.class,
      "hadoop", HadoopSSLCredentialProvider.class
  );

  private String providerChain;

  public SSLCredentialProviderFactory() {
    this.providerChain = System.getProperty(PROVIDER_CHAIN_KEY, DEFAULT_PROVIDER_CHAIN);
  }

  public SSLCredentialProviderFactory(String providerChain) {
    this.providerChain = providerChain;
  }

  public List<SSLCredentialProvider> getProviders() {
    ArrayList<SSLCredentialProvider> providers = new ArrayList<>();
    if (log.isDebugEnabled()) {
      log.debug(String.format(Locale.ROOT, "Processing SSL Credential Provider chain: %s", providerChain));
    }
    String classPrefix = "class://";
    for (String provider : providerChain.split(";")) {
      if (defaultProviders.containsKey(provider)) {
        providers.add(getDefaultProvider(defaultProviders.get(provider)));
      } else if (provider.startsWith(classPrefix)) {
        providers.add(getProviderByClassName(provider.substring(classPrefix.length())));
      } else {
        throw new RuntimeException("Unable to parse credential provider: " + provider);
      }
    }
    return providers;
  }

  private SSLCredentialProvider getProviderByClassName(String clazzName) {
    try {
      return (SSLCredentialProvider) Class.forName(clazzName).getConstructor().newInstance();
    } catch (InstantiationException | ClassNotFoundException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      String msg = String.format(Locale.ROOT, "Could not instantiate %s credential provider", clazzName);
      log.error(msg);
      throw new RuntimeException(msg, e);
    }
  }

  @SuppressWarnings({"unchecked"})
  private SSLCredentialProvider getDefaultProvider(@SuppressWarnings({"rawtypes"})Class aClass) {
    try {
      return (SSLCredentialProvider) aClass.getConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      String msg = String.format(Locale.ROOT, "Could not instantiate %s credential provider", aClass.getName());
      log.error(msg);
      throw new RuntimeException(msg, e);
    }
  }

}
