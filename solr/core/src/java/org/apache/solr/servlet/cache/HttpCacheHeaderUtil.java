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
package org.apache.solr.servlet.cache;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.lucene.util.WeakIdentityMap;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrConfig.HttpCachingConfig.LastModFrom;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import org.apache.commons.codec.binary.Base64;

public final class HttpCacheHeaderUtil {
  
  public static void sendNotModified(HttpServletResponse res) {
    res.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
  }

  public static void sendPreconditionFailed(HttpServletResponse res) {
    res.setStatus(HttpServletResponse.SC_PRECONDITION_FAILED);
  }
  
  /**
   * Weak Ref based cache for keeping track of core specific etagSeed
   * and the last computed etag.
   *
   * @see #calcEtag
   */
  private static WeakIdentityMap<UUID, EtagCacheVal> etagCoreCache = WeakIdentityMap.newConcurrentHashMap();

  /** @see #etagCoreCache */
  private static class EtagCacheVal {
    private final String etagSeed;
    
    private String etagCache = null;
    private long indexVersionCache=-1;
    
    public EtagCacheVal(final String etagSeed) {
      this.etagSeed = etagSeed;
    }

    public String calcEtag(final long currentIndexVersion) {
      if (currentIndexVersion != indexVersionCache) {
        indexVersionCache=currentIndexVersion;
        etagCache = "\""
            + new String(Base64.encodeBase64((Long.toHexString(Long.reverse(indexVersionCache)) + etagSeed)
            .getBytes(StandardCharsets.US_ASCII)), StandardCharsets.US_ASCII) + "\"";
      }
      
      return etagCache;
    }
  }
  
  /**
   * Calculates a tag for the ETag header.
   *
   * @return a tag
   */
  public static String calcEtag(final SolrQueryRequest solrReq) {
    final SolrCore core = solrReq.getCore();
    final long currentIndexVersion
      = solrReq.getSearcher().getIndexReader().getVersion();

    EtagCacheVal etagCache = etagCoreCache.get(core.uniqueId);
    if (null == etagCache) {
      final String etagSeed
        = core.getSolrConfig().getHttpCachingConfig().getEtagSeed();
      etagCache = new EtagCacheVal(etagSeed);
      etagCoreCache.put(core.uniqueId, etagCache);
    }
    
    return etagCache.calcEtag(currentIndexVersion);
    
  }

  /**
   * Checks if one of the tags in the list equals the given etag.
   * 
   * @param headerList
   *            the ETag header related header elements
   * @param etag
   *            the ETag to compare with
   * @return true if the etag is found in one of the header elements - false
   *         otherwise
   */
  public static boolean isMatchingEtag(final List<String> headerList,
      final String etag) {
    for (String header : headerList) {
      final String[] headerEtags = header.split(",");
      for (String s : headerEtags) {
        s = s.trim();
        if (s.equals(etag) || "*".equals(s)) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Calculate the appropriate last-modified time for Solr relative the current request.
   * 
   * @return the timestamp to use as a last modified time.
   */
  public static long calcLastModified(final SolrQueryRequest solrReq) {
    final SolrCore core = solrReq.getCore();
    final SolrIndexSearcher searcher = solrReq.getSearcher();
    
    final LastModFrom lastModFrom
      = core.getSolrConfig().getHttpCachingConfig().getLastModFrom();

    long lastMod;
    try {
      // assume default, change if needed (getOpenTime() should be fast)
      lastMod =
        LastModFrom.DIRLASTMOD == lastModFrom
        ? IndexDeletionPolicyWrapper.getCommitTimestamp(searcher.getIndexReader().getIndexCommit())
        : searcher.getOpenTimeStamp().getTime();
    } catch (IOException e) {
      // we're pretty freaking screwed if this happens
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
    // Get the time where the searcher has been opened
    // We get rid of the milliseconds because the HTTP header has only
    // second granularity
    return lastMod - (lastMod % 1000L);
  }

  @SuppressForbidden(reason = "Need currentTimeMillis to send out cache control headers externally")
  private static long timeNowForHeader() {
    return System.currentTimeMillis();
  }

  /**
   * Set the Cache-Control HTTP header (and Expires if needed)
   * based on the SolrConfig.
   * @param conf The config of the SolrCore handling this request
   * @param resp The servlet response object to modify
   * @param method The request method (GET, POST, ...) used by this request
   */
  public static void setCacheControlHeader(final SolrConfig conf,
                                           final HttpServletResponse resp, final Method method) {
    // We do not emit HTTP header for POST and OTHER request types
    if (Method.POST==method || Method.OTHER==method) {
      return;
    }
    final String cc = conf.getHttpCachingConfig().getCacheControlHeader();
    if (null != cc) {
      resp.setHeader("Cache-Control", cc);
    }
    Long maxAge = conf.getHttpCachingConfig().getMaxAge();
    if (null != maxAge) {
      resp.setDateHeader("Expires", timeNowForHeader() + (maxAge * 1000L));
    }

    return;
  }

  /**
   * Sets HTTP Response cache validator headers appropriately and
   * validates the HTTP Request against these using any conditional
   * request headers.
   *
   * If the request contains conditional headers, and those headers
   * indicate a match with the current known state of the system, this
   * method will return "true" indicating that a 304 Status code can be
   * returned, and no further processing is needed.
   *
   * 
   * @return true if the request contains conditional headers, and those
   *         headers indicate a match with the current known state of the
   *         system -- indicating that a 304 Status code can be returned to
   *         the client, and no further request processing is needed.  
   */
  public static boolean doCacheHeaderValidation(final SolrQueryRequest solrReq,
                                                final HttpServletRequest req,
                                                final Method reqMethod,
                                                final HttpServletResponse resp) {
    
    if (Method.POST==reqMethod || Method.OTHER==reqMethod) {
      return false;
    }
    
    final long lastMod = HttpCacheHeaderUtil.calcLastModified(solrReq);
    final String etag = HttpCacheHeaderUtil.calcEtag(solrReq);
    
    resp.setDateHeader("Last-Modified", lastMod);
    resp.setHeader("ETag", etag);

    if (checkETagValidators(req, resp, reqMethod, etag)) {
      return true;
    }

    if (checkLastModValidators(req, resp, lastMod)) {
      return true;
    }

    return false;
  }
  

  /**
   * Check for etag related conditional headers and set status 
   * 
   * @return true if no request processing is necessary and HTTP response status has been set, false otherwise.
   */
  @SuppressWarnings("unchecked")
  public static boolean checkETagValidators(final HttpServletRequest req,
                                            final HttpServletResponse resp,
                                            final Method reqMethod,
                                            final String etag) {
    
    // First check If-None-Match because this is the common used header
    // element by HTTP clients
    final List<String> ifNoneMatchList = Collections.list(req
        .getHeaders("If-None-Match"));
    if (ifNoneMatchList.size() > 0 && isMatchingEtag(ifNoneMatchList, etag)) {
      if (reqMethod == Method.GET || reqMethod == Method.HEAD) {
        sendNotModified(resp);
      } else {
        sendPreconditionFailed(resp);
      }
      return true;
    }

    // Check for If-Match headers
    final List<String> ifMatchList = Collections.list(req
        .getHeaders("If-Match"));
    if (ifMatchList.size() > 0 && !isMatchingEtag(ifMatchList, etag)) {
      sendPreconditionFailed(resp);
      return true;
    }

    return false;
  }

  /**
   * Check for modify time related conditional headers and set status 
   * 
   * @return true if no request processing is necessary and HTTP response status has been set, false otherwise.
   */
  public static boolean checkLastModValidators(final HttpServletRequest req,
                                               final HttpServletResponse resp,
                                               final long lastMod) {

    try {
      // First check for If-Modified-Since because this is the common
      // used header by HTTP clients
      final long modifiedSince = req.getDateHeader("If-Modified-Since");
      if (modifiedSince != -1L && lastMod <= modifiedSince) {
        // Send a "not-modified"
        sendNotModified(resp);
        return true;
      }
      
      final long unmodifiedSince = req.getDateHeader("If-Unmodified-Since");
      if (unmodifiedSince != -1L && lastMod > unmodifiedSince) {
        // Send a "precondition failed"
        sendPreconditionFailed(resp);
        return true;
      }
    } catch (IllegalArgumentException iae) {
      // one of our date headers was not formated properly, ignore it
      /* NOOP */
    }
    return false;
  }

   /**
   * Checks if the downstream request handler wants to avoid HTTP caching of
   * the response.
   * 
   * @param solrRsp The Solr response object
   * @param resp The HTTP servlet response object
   * @param reqMethod The HTTP request type
   */
  public static void checkHttpCachingVeto(final SolrQueryResponse solrRsp,
      HttpServletResponse resp, final Method reqMethod) {
    // For POST we do nothing. They never get cached
    if (Method.POST == reqMethod || Method.OTHER == reqMethod) {
      return;
    }
    // If the request handler has not vetoed and there is no
    // exception silently return
    if (solrRsp.isHttpCaching() && solrRsp.getException() == null) {
      return;
    }
    
    // Otherwise we tell the caches that we don't want to cache the response
    resp.setHeader("Cache-Control", "no-cache, no-store");

    // For HTTP/1.0 proxy caches
    resp.setHeader("Pragma", "no-cache");

    // This sets the expiry date to a date in the past
    // As long as no time machines get invented this is safe
    resp.setHeader("Expires", "Sat, 01 Jan 2000 01:00:00 GMT");

    long timeNowForHeader = timeNowForHeader();
    // We signal "just modified" just in case some broken
    // proxy cache does not follow the above headers
    resp.setDateHeader("Last-Modified", timeNowForHeader);
    
    // We override the ETag with something different
    resp.setHeader("ETag", '"'+Long.toHexString(timeNowForHeader)+'"');
  } 
}
