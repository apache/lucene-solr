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
package org.apache.solr.handler;

import static org.apache.solr.common.params.CommonParams.ID;
import static org.apache.solr.common.params.CommonParams.SORT;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.io.ModelCache;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eval.AbsoluteValueEvaluator;
import org.apache.solr.client.solrj.io.eval.AddEvaluator;
import org.apache.solr.client.solrj.io.eval.AndEvaluator;
import org.apache.solr.client.solrj.io.eval.AnovaEvaluator;
import org.apache.solr.client.solrj.io.eval.AppendEvaluator;
import org.apache.solr.client.solrj.io.eval.ArcCosineEvaluator;
import org.apache.solr.client.solrj.io.eval.ArcSineEvaluator;
import org.apache.solr.client.solrj.io.eval.ArcTangentEvaluator;
import org.apache.solr.client.solrj.io.eval.ArrayEvaluator;
import org.apache.solr.client.solrj.io.eval.AscEvaluator;
import org.apache.solr.client.solrj.io.eval.CeilingEvaluator;
import org.apache.solr.client.solrj.io.eval.CoalesceEvaluator;
import org.apache.solr.client.solrj.io.eval.ColumnEvaluator;
import org.apache.solr.client.solrj.io.eval.ConversionEvaluator;
import org.apache.solr.client.solrj.io.eval.ConvolutionEvaluator;
import org.apache.solr.client.solrj.io.eval.CopyOfEvaluator;
import org.apache.solr.client.solrj.io.eval.CopyOfRangeEvaluator;
import org.apache.solr.client.solrj.io.eval.CorrelationEvaluator;
import org.apache.solr.client.solrj.io.eval.CosineEvaluator;
import org.apache.solr.client.solrj.io.eval.CovarianceEvaluator;
import org.apache.solr.client.solrj.io.eval.CubedRootEvaluator;
import org.apache.solr.client.solrj.io.eval.CumulativeProbabilityEvaluator;
import org.apache.solr.client.solrj.io.eval.DescribeEvaluator;
import org.apache.solr.client.solrj.io.eval.DivideEvaluator;
import org.apache.solr.client.solrj.io.eval.EmpiricalDistributionEvaluator;
import org.apache.solr.client.solrj.io.eval.EqualToEvaluator;
import org.apache.solr.client.solrj.io.eval.EuclideanDistanceEvaluator;
import org.apache.solr.client.solrj.io.eval.ExclusiveOrEvaluator;
import org.apache.solr.client.solrj.io.eval.FindDelayEvaluator;
import org.apache.solr.client.solrj.io.eval.FloorEvaluator;
import org.apache.solr.client.solrj.io.eval.GreaterThanEqualToEvaluator;
import org.apache.solr.client.solrj.io.eval.GreaterThanEvaluator;
import org.apache.solr.client.solrj.io.eval.HistogramEvaluator;
import org.apache.solr.client.solrj.io.eval.HyperbolicCosineEvaluator;
import org.apache.solr.client.solrj.io.eval.HyperbolicSineEvaluator;
import org.apache.solr.client.solrj.io.eval.HyperbolicTangentEvaluator;
import org.apache.solr.client.solrj.io.eval.IfThenElseEvaluator;
import org.apache.solr.client.solrj.io.eval.KolmogorovSmirnovEvaluator;
import org.apache.solr.client.solrj.io.eval.LengthEvaluator;
import org.apache.solr.client.solrj.io.eval.LessThanEqualToEvaluator;
import org.apache.solr.client.solrj.io.eval.LessThanEvaluator;
import org.apache.solr.client.solrj.io.eval.ModuloEvaluator;
import org.apache.solr.client.solrj.io.eval.MovingAverageEvaluator;
import org.apache.solr.client.solrj.io.eval.MultiplyEvaluator;
import org.apache.solr.client.solrj.io.eval.NaturalLogEvaluator;
import org.apache.solr.client.solrj.io.eval.NormalDistributionEvaluator;
import org.apache.solr.client.solrj.io.eval.NormalizeEvaluator;
import org.apache.solr.client.solrj.io.eval.NotEvaluator;
import org.apache.solr.client.solrj.io.eval.OrEvaluator;
import org.apache.solr.client.solrj.io.eval.PercentileEvaluator;
import org.apache.solr.client.solrj.io.eval.PowerEvaluator;
import org.apache.solr.client.solrj.io.eval.PredictEvaluator;
import org.apache.solr.client.solrj.io.eval.RankEvaluator;
import org.apache.solr.client.solrj.io.eval.RawValueEvaluator;
import org.apache.solr.client.solrj.io.eval.RegressionEvaluator;
import org.apache.solr.client.solrj.io.eval.ResidualsEvaluator;
import org.apache.solr.client.solrj.io.eval.ReverseEvaluator;
import org.apache.solr.client.solrj.io.eval.RoundEvaluator;
import org.apache.solr.client.solrj.io.eval.SampleEvaluator;
import org.apache.solr.client.solrj.io.eval.ScaleEvaluator;
import org.apache.solr.client.solrj.io.eval.SequenceEvaluator;
import org.apache.solr.client.solrj.io.eval.SineEvaluator;
import org.apache.solr.client.solrj.io.eval.SquareRootEvaluator;
import org.apache.solr.client.solrj.io.eval.SubtractEvaluator;
import org.apache.solr.client.solrj.io.eval.TangentEvaluator;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorDay;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorDayOfQuarter;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorDayOfYear;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorEpoch;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorHour;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorMinute;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorMonth;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorQuarter;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorSecond;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorWeek;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorYear;
import org.apache.solr.client.solrj.io.eval.UniformDistributionEvaluator;
import org.apache.solr.client.solrj.io.eval.UuidEvaluator;
import org.apache.solr.client.solrj.io.graph.GatherNodesStream;
import org.apache.solr.client.solrj.io.graph.ShortestPathStream;
import org.apache.solr.client.solrj.io.ops.ConcatOperation;
import org.apache.solr.client.solrj.io.ops.DistinctOperation;
import org.apache.solr.client.solrj.io.ops.GroupOperation;
import org.apache.solr.client.solrj.io.ops.ReplaceOperation;
import org.apache.solr.client.solrj.io.stream.CalculatorStream;
import org.apache.solr.client.solrj.io.stream.CartesianProductStream;
import org.apache.solr.client.solrj.io.stream.CellStream;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.client.solrj.io.stream.CommitStream;
import org.apache.solr.client.solrj.io.stream.ComplementStream;
import org.apache.solr.client.solrj.io.stream.DaemonStream;
import org.apache.solr.client.solrj.io.stream.EchoStream;
import org.apache.solr.client.solrj.io.stream.EvalStream;
import org.apache.solr.client.solrj.io.stream.ExceptionStream;
import org.apache.solr.client.solrj.io.stream.ExecutorStream;
import org.apache.solr.client.solrj.io.stream.FacetStream;
import org.apache.solr.client.solrj.io.stream.FeaturesSelectionStream;
import org.apache.solr.client.solrj.io.stream.FetchStream;
import org.apache.solr.client.solrj.io.stream.GetStream;
import org.apache.solr.client.solrj.io.stream.HashJoinStream;
import org.apache.solr.client.solrj.io.stream.HavingStream;
import org.apache.solr.client.solrj.io.stream.InnerJoinStream;
import org.apache.solr.client.solrj.io.stream.IntersectStream;
import org.apache.solr.client.solrj.io.stream.JDBCStream;
import org.apache.solr.client.solrj.io.stream.KnnStream;
import org.apache.solr.client.solrj.io.stream.LeftOuterJoinStream;
import org.apache.solr.client.solrj.io.stream.LetStream;
import org.apache.solr.client.solrj.io.stream.ListStream;
import org.apache.solr.client.solrj.io.stream.MergeStream;
import org.apache.solr.client.solrj.io.stream.ModelStream;
import org.apache.solr.client.solrj.io.stream.NullStream;
import org.apache.solr.client.solrj.io.stream.OuterHashJoinStream;
import org.apache.solr.client.solrj.io.stream.ParallelStream;
import org.apache.solr.client.solrj.io.stream.PlotStream;
import org.apache.solr.client.solrj.io.stream.PriorityStream;
import org.apache.solr.client.solrj.io.stream.RandomStream;
import org.apache.solr.client.solrj.io.stream.RankStream;
import org.apache.solr.client.solrj.io.stream.ReducerStream;
import org.apache.solr.client.solrj.io.stream.RollupStream;
import org.apache.solr.client.solrj.io.stream.ScoreNodesStream;
import org.apache.solr.client.solrj.io.stream.SelectStream;
import org.apache.solr.client.solrj.io.stream.ShuffleStream;
import org.apache.solr.client.solrj.io.stream.SignificantTermsStream;
import org.apache.solr.client.solrj.io.stream.SortStream;
import org.apache.solr.client.solrj.io.stream.SqlStream;
import org.apache.solr.client.solrj.io.stream.StatsStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TextLogitStream;
import org.apache.solr.client.solrj.io.stream.TimeSeriesStream;
import org.apache.solr.client.solrj.io.stream.TopicStream;
import org.apache.solr.client.solrj.io.stream.TupStream;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.UniqueStream;
import org.apache.solr.client.solrj.io.stream.UpdateStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MaxMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MeanMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MinMetric;
import org.apache.solr.client.solrj.io.stream.metrics.SumMetric;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamHandler extends RequestHandlerBase implements SolrCoreAware, PermissionNameProvider {

  static SolrClientCache clientCache = new SolrClientCache();
  static ModelCache modelCache = null;
  private StreamFactory streamFactory = new StreamFactory();
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private String coreName;
  private Map<String,DaemonStream> daemons = Collections.synchronizedMap(new HashMap());

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext request) {
    return PermissionNameProvider.Name.READ_PERM;
  }

  public static SolrClientCache getClientCache() {
    return clientCache;
  }

  public void inform(SolrCore core) {

    /*
     * The stream factory will always contain the zkUrl for the given collection Adds default streams with their
     * corresponding function names. These defaults can be overridden or added to in the solrConfig in the stream
     * RequestHandler def. Example config override 
     * <lst name="streamFunctions"> 
     *  <str name="group">org.apache.solr.client.solrj.io.stream.ReducerStream</str> 
     *  <str name="count">org.apache.solr.client.solrj.io.stream.RecordCountStream</str> 
     * </lst>
     */

    String defaultCollection;
    String defaultZkhost;
    CoreContainer coreContainer = core.getCoreContainer();
    this.coreName = core.getName();

    if (coreContainer.isZooKeeperAware()) {
      defaultCollection = core.getCoreDescriptor().getCollectionName();
      defaultZkhost = core.getCoreContainer().getZkController().getZkServerAddress();
      streamFactory.withCollectionZkHost(defaultCollection, defaultZkhost);
      streamFactory.withDefaultZkHost(defaultZkhost);
      modelCache = new ModelCache(250,
          defaultZkhost,
          clientCache);
    }

    streamFactory
        // source streams
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("facet", FacetStream.class)
        .withFunctionName("update", UpdateStream.class)
        .withFunctionName("jdbc", JDBCStream.class)
        .withFunctionName("topic", TopicStream.class)
        .withFunctionName("commit", CommitStream.class)
        .withFunctionName("random", RandomStream.class)
        .withFunctionName("knn", KnnStream.class)

        // decorator streams
        .withFunctionName("merge", MergeStream.class)
        .withFunctionName("unique", UniqueStream.class)
        .withFunctionName("top", RankStream.class)
        .withFunctionName("group", GroupOperation.class)
        .withFunctionName("reduce", ReducerStream.class)
        .withFunctionName("parallel", ParallelStream.class)
        .withFunctionName("rollup", RollupStream.class)
        .withFunctionName("stats", StatsStream.class)
        .withFunctionName("innerJoin", InnerJoinStream.class)
        .withFunctionName("leftOuterJoin", LeftOuterJoinStream.class)
        .withFunctionName("hashJoin", HashJoinStream.class)
        .withFunctionName("outerHashJoin", OuterHashJoinStream.class)
        .withFunctionName("intersect", IntersectStream.class)
        .withFunctionName("complement", ComplementStream.class)
        .withFunctionName(SORT, SortStream.class)
        .withFunctionName("train", TextLogitStream.class)
        .withFunctionName("features", FeaturesSelectionStream.class)
        .withFunctionName("daemon", DaemonStream.class)
        .withFunctionName("shortestPath", ShortestPathStream.class)
        .withFunctionName("gatherNodes", GatherNodesStream.class)
        .withFunctionName("nodes", GatherNodesStream.class)
        .withFunctionName("select", SelectStream.class)
        .withFunctionName("shortestPath", ShortestPathStream.class)
        .withFunctionName("gatherNodes", GatherNodesStream.class)
        .withFunctionName("nodes", GatherNodesStream.class)
        .withFunctionName("scoreNodes", ScoreNodesStream.class)
        .withFunctionName("model", ModelStream.class)
        .withFunctionName("classify", ClassifyStream.class)
        .withFunctionName("fetch", FetchStream.class)
        .withFunctionName("executor", ExecutorStream.class)
        .withFunctionName("null", NullStream.class)
        .withFunctionName("priority", PriorityStream.class)
        .withFunctionName("significantTerms", SignificantTermsStream.class)
        .withFunctionName("cartesianProduct", CartesianProductStream.class)
        .withFunctionName("shuffle", ShuffleStream.class)
        .withFunctionName("calc", CalculatorStream.class)
        .withFunctionName("eval", EvalStream.class)
        .withFunctionName("echo", EchoStream.class)
        .withFunctionName("cell", CellStream.class)
        .withFunctionName("list", ListStream.class)
        .withFunctionName("let", LetStream.class)
        .withFunctionName("get", GetStream.class)
        .withFunctionName("timeseries", TimeSeriesStream.class)
        .withFunctionName("tuple", TupStream.class)
        .withFunctionName("sql", SqlStream.class)

        // metrics
        .withFunctionName("min", MinMetric.class)
        .withFunctionName("max", MaxMetric.class)
        .withFunctionName("avg", MeanMetric.class)
        .withFunctionName("sum", SumMetric.class)
        .withFunctionName("count", CountMetric.class)

        // tuple manipulation operations
        .withFunctionName("replace", ReplaceOperation.class)
        .withFunctionName("concat", ConcatOperation.class)

        // stream reduction operations
        .withFunctionName("group", GroupOperation.class)
        .withFunctionName("distinct", DistinctOperation.class)
        .withFunctionName("having", HavingStream.class)

        // Stream Evaluators
        .withFunctionName("val", RawValueEvaluator.class)

        // New Evaluators
        .withFunctionName("anova", AnovaEvaluator.class)
        .withFunctionName("array", ArrayEvaluator.class)
        .withFunctionName("col", ColumnEvaluator.class)
        .withFunctionName("conv", ConvolutionEvaluator.class)
        .withFunctionName("copyOfRange", CopyOfRangeEvaluator.class)
        .withFunctionName("copyOf", CopyOfEvaluator.class)
        .withFunctionName("cov", CovarianceEvaluator.class)
        .withFunctionName("describe", DescribeEvaluator.class)
        .withFunctionName("distance", EuclideanDistanceEvaluator.class)
        .withFunctionName("empiricalDistribution", EmpiricalDistributionEvaluator.class)
        .withFunctionName("finddelay", FindDelayEvaluator.class)
        .withFunctionName("hist", HistogramEvaluator.class)
        .withFunctionName("length", LengthEvaluator.class)
        .withFunctionName("movingAvg", MovingAverageEvaluator.class)
        .withFunctionName("normalize", NormalizeEvaluator.class)
        .withFunctionName("percentile", PercentileEvaluator.class)
        .withFunctionName("predict", PredictEvaluator.class)
        .withFunctionName("rank", RankEvaluator.class)
        .withFunctionName("regress", RegressionEvaluator.class)
        .withFunctionName("rev", ReverseEvaluator.class)
        .withFunctionName("scale", ScaleEvaluator.class)
        .withFunctionName("sequence", SequenceEvaluator.class)
        .withFunctionName("addAll", AppendEvaluator.class)
        .withFunctionName("append", AppendEvaluator.class)
        .withFunctionName("residuals", ResidualsEvaluator.class)
        .withFunctionName("plot", PlotStream.class)
        .withFunctionName("normalDistribution", NormalDistributionEvaluator.class)
        .withFunctionName("uniformDistribution", UniformDistributionEvaluator.class)
        .withFunctionName("sample", SampleEvaluator.class)
        .withFunctionName("kolmogorovSmirnov", KolmogorovSmirnovEvaluator.class)
        .withFunctionName("ks", KolmogorovSmirnovEvaluator.class)
        .withFunctionName("asc", AscEvaluator.class)
        .withFunctionName("cumulativeProbability", CumulativeProbabilityEvaluator.class)

        // Boolean Stream Evaluators
        .withFunctionName("and", AndEvaluator.class)
        .withFunctionName("eor", ExclusiveOrEvaluator.class)
        .withFunctionName("eq", EqualToEvaluator.class)
        .withFunctionName("gt", GreaterThanEvaluator.class)
        .withFunctionName("gteq", GreaterThanEqualToEvaluator.class)
        .withFunctionName("lt", LessThanEvaluator.class)
        .withFunctionName("lteq", LessThanEqualToEvaluator.class)
        .withFunctionName("not", NotEvaluator.class)
        .withFunctionName("or", OrEvaluator.class)

        // Date Time Evaluators
        .withFunctionName(TemporalEvaluatorYear.FUNCTION_NAME, TemporalEvaluatorYear.class)
        .withFunctionName(TemporalEvaluatorMonth.FUNCTION_NAME, TemporalEvaluatorMonth.class)
        .withFunctionName(TemporalEvaluatorDay.FUNCTION_NAME, TemporalEvaluatorDay.class)
        .withFunctionName(TemporalEvaluatorDayOfYear.FUNCTION_NAME, TemporalEvaluatorDayOfYear.class)
        .withFunctionName(TemporalEvaluatorHour.FUNCTION_NAME, TemporalEvaluatorHour.class)
        .withFunctionName(TemporalEvaluatorMinute.FUNCTION_NAME, TemporalEvaluatorMinute.class)
        .withFunctionName(TemporalEvaluatorSecond.FUNCTION_NAME, TemporalEvaluatorSecond.class)
        .withFunctionName(TemporalEvaluatorEpoch.FUNCTION_NAME, TemporalEvaluatorEpoch.class)
        .withFunctionName(TemporalEvaluatorWeek.FUNCTION_NAME, TemporalEvaluatorWeek.class)
        .withFunctionName(TemporalEvaluatorQuarter.FUNCTION_NAME, TemporalEvaluatorQuarter.class)
        .withFunctionName(TemporalEvaluatorDayOfQuarter.FUNCTION_NAME, TemporalEvaluatorDayOfQuarter.class)

        // Number Stream Evaluators
        .withFunctionName("abs", AbsoluteValueEvaluator.class)
        .withFunctionName("add", AddEvaluator.class)
        .withFunctionName("div", DivideEvaluator.class)
        .withFunctionName("mult", MultiplyEvaluator.class)
        .withFunctionName("sub", SubtractEvaluator.class)
        .withFunctionName("log", NaturalLogEvaluator.class)
        .withFunctionName("pow", PowerEvaluator.class)
        .withFunctionName("mod", ModuloEvaluator.class)
        .withFunctionName("ceil", CeilingEvaluator.class)
        .withFunctionName("floor", FloorEvaluator.class)
        .withFunctionName("sin", SineEvaluator.class)
        .withFunctionName("asin", ArcSineEvaluator.class)
        .withFunctionName("sinh", HyperbolicSineEvaluator.class)
        .withFunctionName("cos", CosineEvaluator.class)
        .withFunctionName("acos", ArcCosineEvaluator.class)
        .withFunctionName("cosh", HyperbolicCosineEvaluator.class)
        .withFunctionName("tan", TangentEvaluator.class)
        .withFunctionName("atan", ArcTangentEvaluator.class)
        .withFunctionName("tanh", HyperbolicTangentEvaluator.class)
        .withFunctionName("round", RoundEvaluator.class)
        .withFunctionName("sqrt", SquareRootEvaluator.class)
        .withFunctionName("cbrt", CubedRootEvaluator.class)
        .withFunctionName("coalesce", CoalesceEvaluator.class)
        .withFunctionName("uuid", UuidEvaluator.class)
        .withFunctionName("corr", CorrelationEvaluator.class)

        // Conditional Stream Evaluators
        .withFunctionName("if", IfThenElseEvaluator.class)
        .withFunctionName("analyze", AnalyzeEvaluator.class)
        .withFunctionName("convert", ConversionEvaluator.class);

    // This pulls all the overrides and additions from the config
    List<PluginInfo> pluginInfos = core.getSolrConfig().getPluginInfos(Expressible.class.getName());
    for (PluginInfo pluginInfo : pluginInfos) {
      Class<? extends Expressible> clazz = core.getMemClassLoader().findClass(pluginInfo.className, Expressible.class);
      streamFactory.withFunctionName(pluginInfo.name, clazz);
    }

    core.addCloseHook(new CloseHook() {
      @Override
      public void preClose(SolrCore core) {
        // To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void postClose(SolrCore core) {
        clientCache.close();
      }
    });
  }

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams params = req.getParams();
    params = adjustParams(params);
    req.setParams(params);

    if (params.get("action") != null) {
      handleAdmin(req, rsp, params);
      return;
    }

    TupleStream tupleStream;

    try {
      StreamExpression streamExpression = StreamExpressionParser.parse(params.get("expr"));
      if (this.streamFactory.isEvaluator(streamExpression)) {
        StreamExpression tupleExpression = new StreamExpression("tuple");
        tupleExpression.addParameter(new StreamExpressionNamedParameter("return-value", streamExpression));
        tupleStream = this.streamFactory.constructStream(tupleExpression);
      } else {
        tupleStream = this.streamFactory.constructStream(streamExpression);
      }
    } catch (Exception e) {
      // Catch exceptions that occur while the stream is being created. This will include streaming expression parse
      // rules.
      SolrException.log(logger, e);
      rsp.add("result-set", new DummyErrorStream(e));

      return;
    }

    int worker = params.getInt("workerID", 0);
    int numWorkers = params.getInt("numWorkers", 1);
    StreamContext context = new StreamContext();
    context.put("shards", getCollectionShards(params));
    context.workerID = worker;
    context.numWorkers = numWorkers;
    context.setSolrClientCache(clientCache);
    context.setModelCache(modelCache);
    context.put("core", this.coreName);
    context.put("solr-core", req.getCore());
    tupleStream.setStreamContext(context);

    // if asking for explanation then go get it
    if (params.getBool("explain", false)) {
      rsp.add("explanation", tupleStream.toExplanation(this.streamFactory));
    }

    if (tupleStream instanceof DaemonStream) {
      DaemonStream daemonStream = (DaemonStream) tupleStream;
      if (daemons.containsKey(daemonStream.getId())) {
        daemons.remove(daemonStream.getId()).close();
      }
      daemonStream.setDaemons(daemons);
      daemonStream.open(); // This will start the deamonStream
      daemons.put(daemonStream.getId(), daemonStream);
      rsp.add("result-set", new DaemonResponseStream("Deamon:" + daemonStream.getId() + " started on " + coreName));
    } else {
      rsp.add("result-set", new TimerStream(new ExceptionStream(tupleStream)));
    }
  }

  private void handleAdmin(SolrQueryRequest req, SolrQueryResponse rsp, SolrParams params) {
    String action = params.get("action");
    if ("stop".equalsIgnoreCase(action)) {
      String id = params.get(ID);
      DaemonStream d = daemons.get(id);
      if (d != null) {
        d.close();
        rsp.add("result-set", new DaemonResponseStream("Deamon:" + id + " stopped on " + coreName));
      } else {
        rsp.add("result-set", new DaemonResponseStream("Deamon:" + id + " not found on " + coreName));
      }
    } else {
      if ("start".equalsIgnoreCase(action)) {
        String id = params.get(ID);
        DaemonStream d = daemons.get(id);
        d.open();
        rsp.add("result-set", new DaemonResponseStream("Deamon:" + id + " started on " + coreName));
      } else if ("list".equalsIgnoreCase(action)) {
        Collection<DaemonStream> vals = daemons.values();
        rsp.add("result-set", new DaemonCollectionStream(vals));
      } else if ("kill".equalsIgnoreCase(action)) {
        String id = params.get("id");
        DaemonStream d = daemons.remove(id);
        if (d != null) {
          d.close();
        }
        rsp.add("result-set", new DaemonResponseStream("Deamon:" + id + " killed on " + coreName));
      }
    }
  }

  private SolrParams adjustParams(SolrParams params) {
    ModifiableSolrParams adjustedParams = new ModifiableSolrParams();
    adjustedParams.add(params);
    adjustedParams.add(CommonParams.OMIT_HEADER, "true");
    return adjustedParams;
  }

  public String getDescription() {
    return "StreamHandler";
  }

  public String getSource() {
    return null;
  }

  public static class DummyErrorStream extends TupleStream {
    private Exception e;

    public DummyErrorStream(Exception e) {
      this.e = e;
    }

    public StreamComparator getStreamSort() {
      return null;
    }

    public void close() {}

    public void open() {}

    public void setStreamContext(StreamContext context) {}

    public List<TupleStream> children() {
      return null;
    }

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {

      return new StreamExplanation(getStreamNodeId().toString())
          .withFunctionName("error")
          .withImplementingClass(this.getClass().getName())
          .withExpressionType(ExpressionType.STREAM_DECORATOR)
          .withExpression("--non-expressible--");
    }

    public Tuple read() {
      String msg = e.getMessage();

      Throwable t = e.getCause();
      while (t != null) {
        msg = t.getMessage();
        t = t.getCause();
      }

      Map m = new HashMap();
      m.put("EOF", true);
      m.put("EXCEPTION", msg);
      return new Tuple(m);
    }
  }

  public static class DaemonCollectionStream extends TupleStream {
    private Iterator<DaemonStream> it;

    public DaemonCollectionStream(Collection<DaemonStream> col) {
      this.it = col.iterator();
    }

    public StreamComparator getStreamSort() {
      return null;
    }

    public void close() {}

    public void open() {}

    public void setStreamContext(StreamContext context) {}

    public List<TupleStream> children() {
      return null;
    }

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {

      return new StreamExplanation(getStreamNodeId().toString())
          .withFunctionName("daemon-collection")
          .withImplementingClass(this.getClass().getName())
          .withExpressionType(ExpressionType.STREAM_DECORATOR)
          .withExpression("--non-expressible--");
    }

    public Tuple read() {
      if (it.hasNext()) {
        return it.next().getInfo();
      } else {
        Map m = new HashMap();
        m.put("EOF", true);
        return new Tuple(m);
      }
    }
  }

  public static class DaemonResponseStream extends TupleStream {
    private String message;
    private boolean sendEOF = false;

    public DaemonResponseStream(String message) {
      this.message = message;
    }

    public StreamComparator getStreamSort() {
      return null;
    }

    public void close() {}

    public void open() {}

    public void setStreamContext(StreamContext context) {}

    public List<TupleStream> children() {
      return null;
    }

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {

      return new StreamExplanation(getStreamNodeId().toString())
          .withFunctionName("daemon-response")
          .withImplementingClass(this.getClass().getName())
          .withExpressionType(ExpressionType.STREAM_DECORATOR)
          .withExpression("--non-expressible--");
    }

    public Tuple read() {
      if (sendEOF) {
        Map m = new HashMap();
        m.put("EOF", true);
        return new Tuple(m);
      } else {
        sendEOF = true;
        Map m = new HashMap();
        m.put("DaemonOp", message);
        return new Tuple(m);
      }
    }
  }

  public static class TimerStream extends TupleStream {

    private long begin;
    private TupleStream tupleStream;

    public TimerStream(TupleStream tupleStream) {
      this.tupleStream = tupleStream;
    }

    public StreamComparator getStreamSort() {
      return this.tupleStream.getStreamSort();
    }

    public void close() throws IOException {
      this.tupleStream.close();
    }

    public void open() throws IOException {
      this.begin = System.nanoTime();
      this.tupleStream.open();
    }

    public void setStreamContext(StreamContext context) {
      this.tupleStream.setStreamContext(context);
    }

    public List<TupleStream> children() {
      return this.tupleStream.children();
    }

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {

      return new StreamExplanation(getStreamNodeId().toString())
          .withFunctionName("timer")
          .withImplementingClass(this.getClass().getName())
          .withExpressionType(ExpressionType.STREAM_DECORATOR)
          .withExpression("--non-expressible--");
    }

    public Tuple read() throws IOException {
      Tuple tuple = this.tupleStream.read();
      if (tuple.EOF) {
        long totalTime = (System.nanoTime() - begin) / 1000000;
        tuple.fields.put("RESPONSE_TIME", totalTime);
      }
      return tuple;
    }
  }

  private Map<String,List<String>> getCollectionShards(SolrParams params) {

    Map<String,List<String>> collectionShards = new HashMap();
    Iterator<String> paramsIt = params.getParameterNamesIterator();
    while (paramsIt.hasNext()) {
      String param = paramsIt.next();
      if (param.indexOf(".shards") > -1) {
        String collection = param.split("\\.")[0];
        String shardString = params.get(param);
        String[] shards = shardString.split(",");
        List<String> shardList = new ArrayList();
        for (String shard : shards) {
          shardList.add(shard);
        }
        collectionShards.put(collection, shardList);
      }
    }

    if (collectionShards.size() > 0) {
      return collectionShards;
    } else {
      return null;
    }
  }
}
