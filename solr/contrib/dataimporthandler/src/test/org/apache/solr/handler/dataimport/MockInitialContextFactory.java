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
package org.apache.solr.handler.dataimport;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import static org.mockito.Mockito.*;

public class MockInitialContextFactory implements InitialContextFactory {
  private static final Map<String, Object> objects = new HashMap<>();
  private final javax.naming.Context context;

  public MockInitialContextFactory() {
    context = mock(javax.naming.Context.class);

    try {
      when(context.lookup(anyString())).thenAnswer(invocation -> objects.get(invocation.getArgument(0)));

    } catch (NamingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public javax.naming.Context getInitialContext(@SuppressWarnings({"rawtypes"})Hashtable env) {
    return context;
  }

  public static void bind(String name, Object obj) {
    objects.put(name, obj);
  }
}
