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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;

/**
 * A {@link Transformer} implementation which logs messages in a given template format.
 * <p>
 * Refer to <a href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * <p>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @since solr 1.4
 */
public class LogTransformer extends Transformer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public Object transformRow(Map<String, Object> row, Context ctx) {
    String expr = ctx.getEntityAttribute(LOG_TEMPLATE);
    String level = ctx.replaceTokens(ctx.getEntityAttribute(LOG_LEVEL));

    if (expr == null || level == null) return row;

    if ("info".equals(level)) {
      if (log.isInfoEnabled())
        log.info(ctx.replaceTokens(expr));
    } else if ("trace".equals(level)) {
      if (log.isTraceEnabled())
        log.trace(ctx.replaceTokens(expr));
    } else if ("warn".equals(level)) {
      if (log.isWarnEnabled())
        log.warn(ctx.replaceTokens(expr));
    } else if ("error".equals(level)) {
      if (log.isErrorEnabled())
        log.error(ctx.replaceTokens(expr));
    } else if ("debug".equals(level)) {
      if (log.isDebugEnabled())
        log.debug(ctx.replaceTokens(expr));
    }

    return row;
  }

  public static final String LOG_TEMPLATE = "logTemplate";
  public static final String LOG_LEVEL = "logLevel";
}
