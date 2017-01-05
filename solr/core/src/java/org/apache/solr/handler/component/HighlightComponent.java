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
package org.apache.solr.handler.component;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.base.Objects;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.highlight.DefaultSolrHighlighter;
import org.apache.solr.highlight.PostingsSolrHighlighter;
import org.apache.solr.highlight.SolrHighlighter;
import org.apache.solr.highlight.UnifiedSolrHighlighter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;

import static java.util.stream.Collectors.toMap;

/**
 * TODO!
 *
 *
 * @since solr 1.3
 */
public class HighlightComponent extends SearchComponent implements PluginInfoInitialized, SolrCoreAware
{
  public enum HighlightMethod {
    UNIFIED("unified"),
    FAST_VECTOR("fastVector"),
    POSTINGS("postings"),
    ORIGINAL("original");

    private static final Map<String, HighlightMethod> METHODS = Collections.unmodifiableMap(Stream.of(values())
        .collect(toMap(HighlightMethod::getMethodName, Function.identity())));

    private final String methodName;

    HighlightMethod(String method) {
      this.methodName = method;
    }

    public String getMethodName() {
      return methodName;
    }

    public static HighlightMethod parse(String method) {
      return METHODS.get(method);
    }
  }

  public static final String COMPONENT_NAME = "highlight";

  private PluginInfo info = PluginInfo.EMPTY_INFO;

  @Deprecated // DWS: in 7.0 lets restructure the abstractions/relationships
  private SolrHighlighter solrConfigHighlighter;

  /**
   * @deprecated instead depend on {@link #process(ResponseBuilder)} to choose the highlighter based on
   * {@link HighlightParams#METHOD}
   */
  @Deprecated
  public static SolrHighlighter getHighlighter(SolrCore core) {
    HighlightComponent hl = (HighlightComponent) core.getSearchComponents().get(HighlightComponent.COMPONENT_NAME);
    return hl==null ? null: hl.getHighlighter();
  }

  @Deprecated
  public SolrHighlighter getHighlighter() {
    return solrConfigHighlighter;
  }

  @Override
  public void init(PluginInfo info) {
    this.info = info;
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    rb.doHighlights = solrConfigHighlighter.isHighlightingEnabled(params);
    if(rb.doHighlights){
      rb.setNeedDocList(true);
      String hlq = params.get(HighlightParams.Q);
      String hlparser = Objects.firstNonNull(params.get(HighlightParams.QPARSER),
                                              params.get(QueryParsing.DEFTYPE, QParserPlugin.DEFAULT_QTYPE));
      if(hlq != null){
        try {
          QParser parser = QParser.getParser(hlq, hlparser, rb.req);
          rb.setHighlightQuery(parser.getHighlightQuery());
        } catch (SyntaxError e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
        }
      }
    }
  }

  @Override
  public void inform(SolrCore core) {
    List<PluginInfo> children = info.getChildren("highlighting");
    if(children.isEmpty()) {
      PluginInfo pluginInfo = core.getSolrConfig().getPluginInfo(SolrHighlighter.class.getName()); //TODO deprecated configuration remove later
      if (pluginInfo != null) {
        solrConfigHighlighter = core.createInitInstance(pluginInfo, SolrHighlighter.class, null, DefaultSolrHighlighter.class.getName());
      } else {
        DefaultSolrHighlighter defHighlighter = new DefaultSolrHighlighter(core);
        defHighlighter.init(PluginInfo.EMPTY_INFO);
        solrConfigHighlighter = defHighlighter;
      }
    } else {
      solrConfigHighlighter = core.createInitInstance(children.get(0),SolrHighlighter.class,null, DefaultSolrHighlighter.class.getName());
    }

  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {

    if (rb.doHighlights) {
      SolrQueryRequest req = rb.req;
      SolrParams params = req.getParams();

      SolrHighlighter highlighter = getHighlighter(params);

      String[] defaultHighlightFields;  //TODO: get from builder by default?
      if (rb.getQparser() != null) {
        defaultHighlightFields = rb.getQparser().getDefaultHighlightFields();
      } else {
        defaultHighlightFields = params.getParams(CommonParams.DF);
      }
      
      Query highlightQuery = rb.getHighlightQuery();
      if(highlightQuery==null) {
        if (rb.getQparser() != null) {
          try {
            highlightQuery = rb.getQparser().getHighlightQuery();
            rb.setHighlightQuery( highlightQuery );
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
        } else {
          highlightQuery = rb.getQuery();
          rb.setHighlightQuery( highlightQuery );
        }
      }

      // No highlighting if there is no query -- consider q.alt=*:*
      if( highlightQuery != null ) {
        NamedList sumData = highlighter.doHighlighting(
                rb.getResults().docList,
                highlightQuery,
                req, defaultHighlightFields );
        
        if(sumData != null) {
          // TODO ???? add this directly to the response?
          rb.rsp.add("highlighting", sumData);
        }
      }
    }
  }

  protected SolrHighlighter getHighlighter(SolrParams params) {
    HighlightMethod method = HighlightMethod.parse(params.get(HighlightParams.METHOD));
    if (method == null) {
      return solrConfigHighlighter;
    }

    switch (method) {
      case UNIFIED:
        if (solrConfigHighlighter instanceof UnifiedSolrHighlighter) {
          return solrConfigHighlighter;
        }
        return new UnifiedSolrHighlighter(); // TODO cache one?
      case POSTINGS:
        if (solrConfigHighlighter instanceof PostingsSolrHighlighter) {
          return solrConfigHighlighter;
        }
        return new PostingsSolrHighlighter(); // TODO cache one?
      case FAST_VECTOR: // fall-through
      case ORIGINAL:
        if (solrConfigHighlighter instanceof DefaultSolrHighlighter) {
          return solrConfigHighlighter;
        } else {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "In order to use " + HighlightParams.METHOD + "=" + method.getMethodName() + " the configured" +
                  " highlighter in solrconfig must be " + DefaultSolrHighlighter.class);
        }
      default: throw new AssertionError();
    }
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    if (!rb.doHighlights) return;

    // Turn on highlighting only only when retrieving fields
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
        sreq.purpose |= ShardRequest.PURPOSE_GET_HIGHLIGHTS;
        // should already be true...
        sreq.params.set(HighlightParams.HIGHLIGHT, "true");      
    } else {
      sreq.params.set(HighlightParams.HIGHLIGHT, "false");      
    }
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (rb.doHighlights && rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {

      NamedList.NamedListEntry[] arr = new NamedList.NamedListEntry[rb.resultIds.size()];

      // TODO: make a generic routine to do automatic merging of id keyed data
      for (ShardRequest sreq : rb.finished) {
        if ((sreq.purpose & ShardRequest.PURPOSE_GET_HIGHLIGHTS) == 0) continue;
        for (ShardResponse srsp : sreq.responses) {
          if (srsp.getException() != null) {
            // can't expect the highlight content if there was an exception for this request
            // this should only happen when using shards.tolerant=true
            continue;
          }
          NamedList hl = (NamedList)srsp.getSolrResponse().getResponse().get("highlighting");
          SolrPluginUtils.copyNamedListIntoArrayByDocPosInResponse(hl, rb.resultIds, arr);
        }
      }

      // remove nulls in case not all docs were able to be retrieved
      rb.rsp.add("highlighting", SolrPluginUtils.removeNulls(arr, new SimpleOrderedMap<>()));
    }
  }

  ////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////
  
  @Override
  public String getDescription() {
    return "Highlighting";
  }
  
  @Override
  public URL[] getDocs() {
    return null;
  }
}
