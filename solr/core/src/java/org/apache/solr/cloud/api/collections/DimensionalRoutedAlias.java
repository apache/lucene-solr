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

package org.apache.solr.cloud.api.collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.RoutedAliasTypes;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.update.AddUpdateCommand;

import static org.apache.solr.client.solrj.request.CollectionAdminRequest.DimensionalRoutedAlias.addDimensionIndexIfRequired;
import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

public class DimensionalRoutedAlias extends RoutedAlias {

  private final String name;
  private List<RoutedAlias> dimensions;

  // things we don't need to calc twice...
  private Set<String> reqParams = new HashSet<>();
  private Set<String> optParams = new HashSet<>();
  private Map<String, String> aliasMetadata;

  private static final Pattern SEP_MATCHER = Pattern.compile("("+
      Arrays.stream(RoutedAliasTypes.values())
          .filter(v -> v != RoutedAliasTypes.DIMENSIONAL)
          .map(RoutedAliasTypes::getSeparatorPrefix)
          .collect(Collectors.joining("|")) +
      ")");


  DimensionalRoutedAlias(List<RoutedAlias> dimensions, String name, Map<String, String> props) {
    this.dimensions = dimensions;
    this.name = name;
    this.aliasMetadata = props;
  }

  interface Deffered<T> {
    T get();
  }

  static RoutedAlias dimensionForType(Map<String, String> props, RoutedAliasTypes type,
                                      int index, Deffered<DimensionalRoutedAlias> dra) {
    // this switch must have a case for every element of the RoutedAliasTypes enum EXCEPT DIMENSIONAL
    switch (type) {
      case TIME:
        return new TimeRoutedAliasDimension(props, index, dra);
      case CATEGORY:
        return new CategoryRoutedAliasDimension(props, index, dra);
      default:
        // if we got a type not handled by the switch there's been a bogus implementation.
        throw new SolrException(SERVER_ERROR, "Router " + type + " is not fully implemented. If you see this" +
            "error in an official release please file a bug report. Available types were:"
            + Arrays.asList(RoutedAliasTypes.values()));
    }
  }


  @Override
  public boolean updateParsedCollectionAliases(ZkStateReader zkStateReader, boolean contextualize) {
    boolean result = false;
    for (RoutedAlias dimension : dimensions) {
      result |= dimension.updateParsedCollectionAliases(zkStateReader, contextualize);
    }
    return result;
  }

  @Override
  public String computeInitialCollectionName() {
    StringBuilder sb = new StringBuilder(getAliasName());
    for (RoutedAlias dimension : dimensions) {
      // N. B. getAliasName is generally safe as a regex because it must conform to collection naming rules
      // and those rules exclude regex special characters. A malicious request might do something expensive, but
      // if you have malicious users able to run admin commands and create aliases, it is very likely that you have
      // much bigger problems than an expensive regex.
      String routeString = dimension.computeInitialCollectionName().replaceAll(dimension.getAliasName() , "");
      sb.append(routeString);
    }
    return sb.toString();
  }

  @Override
  String[] formattedRouteValues(SolrInputDocument doc) {
    String[] result = new String[dimensions.size()];
    for (int i = 0; i < dimensions.size(); i++) {
      RoutedAlias dimension = dimensions.get(i);
      result[i] = dimension.formattedRouteValues(doc)[0];
    }
    return result;
  }

  @Override
  public String getAliasName() {
    return name;
  }

  @Override
  public String getRouteField() {
    throw new UnsupportedOperationException("DRA's route via their dimensions, this method should not be called");
  }

  @Override
  public RoutedAliasTypes getRoutedAliasType() {
    return RoutedAliasTypes.DIMENSIONAL;
  }

  @Override
  public void validateRouteValue(AddUpdateCommand cmd) throws SolrException {
    for (RoutedAlias dimension : dimensions) {
      dimension.validateRouteValue(cmd);
    }
  }


  @Override
  public Map<String, String> getAliasMetadata() {
    return aliasMetadata;
  }

  @Override
  public Set<String> getRequiredParams() {
    if (reqParams.size() == 0) {
      indexParams(reqParams, dimensions, RoutedAlias::getRequiredParams);
      // the top level Dimensional[foo,bar] designation needs to be retained
      reqParams.add(ROUTER_TYPE_NAME);
      reqParams.add(ROUTER_FIELD);
    }
    return reqParams;
  }

  @Override
  public Set<String> getOptionalParams() {

    if (optParams.size() == 0) {
      indexParams(optParams, dimensions, RoutedAlias::getOptionalParams);
    }
    return optParams;

  }

  @Override
  public CandidateCollection findCandidateGivenValue(AddUpdateCommand cmd) {
    contextualizeDimensions(formattedRouteValues(cmd.solrDoc));
    List<CandidateCollection> subPartCandidates = new ArrayList<>();
    for (RoutedAlias dimension : dimensions) {
      subPartCandidates.add(dimension.findCandidateGivenValue(cmd));
    }

    StringBuilder col2Create = new StringBuilder(getAliasName());
    StringBuilder destCol = new StringBuilder(getAliasName());
    CreationType max = CreationType.NONE;
    for (CandidateCollection subCol : subPartCandidates) {
      col2Create.append(subCol.getCreationCollection());
      destCol.append(subCol.getDestinationCollection());
      if (subCol.getCreationType().ordinal() > max.ordinal()) {
        max = subCol.getCreationType();
      }
    }
    return new CandidateCollection(max,destCol.toString(),col2Create.toString());
  }

  @Override
  protected String getHeadCollectionIfOrdered(AddUpdateCommand cmd) {
    StringBuilder head = new StringBuilder(getAliasName());
    for (RoutedAlias dimension : dimensions) {
      head.append(dimension.getHeadCollectionIfOrdered(cmd).substring(getAliasName().length()));
    }
    return head.toString();
  }


  /**
   * Determine the combination of adds/deletes implied by the arrival of a document destined for the
   * specified collection.
   *
   * @param targetCol the collection for which a document is destined.
   * @return A list of actions across the DRA.
   */
  @Override
  protected List<Action> calculateActions(String targetCol) {
    String[] routeValues = SEP_MATCHER.split(targetCol);
    // remove the alias name to avoid all manner of off by one errors...
    routeValues = Arrays.copyOfRange(routeValues,1,routeValues.length);
    List<List<Action>> dimActs = new ArrayList<>(routeValues.length);
    contextualizeDimensions(routeValues);
    for (int i = 0; i < routeValues.length; i++) {
      String routeValue = routeValues[i];
      RoutedAlias dim = dimensions.get(i);
      dimActs.add(dim.calculateActions(dim.getAliasName() + getSeparatorPrefix(dim)+ routeValue) );
    }
    Set <Action> result = new LinkedHashSet<>();
    StringBuilder currentSuffix = new StringBuilder();
    for (int i = routeValues.length -1; i >=0 ; i--) { // also lowest up to match
      String routeValue = routeValues[i];
      RoutedAlias dim = dimensions.get(i);
      String dimStr = dim.getRoutedAliasType().getSeparatorPrefix() + routeValue;
      List<Action> actions = dimActs.get(i);
      for (Iterator<Action> iterator = actions.iterator(); iterator.hasNext(); ) {
        Action action = iterator.next();
        iterator.remove();
        result.add(new Action(action.sourceAlias, action.actionType, action.targetCollection + currentSuffix));
      }
      result.addAll(actions);
      Set <Action> revisedResult = new LinkedHashSet<>();

      for (Action action : result) {
        if (action.sourceAlias == dim) {
          revisedResult.add(action); // should already have the present value
          continue;
        }
        // the rest are from lower dimensions and thus require a prefix.
        revisedResult.add(new Action(action.sourceAlias, action.actionType,dimStr + action.targetCollection));
      }
      result = revisedResult;
      currentSuffix.append(dimStr);
    }
    Set <Action> revisedResult = new LinkedHashSet<>();
    for (Action action : result) {
      revisedResult.add(new Action(action.sourceAlias, action.actionType,getAliasName() + action.targetCollection));
    }
    return new ArrayList<>(revisedResult);
  }

  private void contextualizeDimensions(String[] routeValues) {
    for (RoutedAlias dimension : dimensions) {
      ((DraContextualized)dimension).setContext(routeValues);
    }
  }


  private static String getSeparatorPrefix(RoutedAlias dim) {
    return dim.getRoutedAliasType().getSeparatorPrefix();
  }

  private static void indexParams(Set<String> result, List<RoutedAlias> dimensions, Function<RoutedAlias, Set<String>> supplier) {
    for (int i = 0; i < dimensions.size(); i++) {
      RoutedAlias dimension = dimensions.get(i);
      Set<String> params = supplier.apply(dimension);
      for (String param : params) {
        addDimensionIndexIfRequired(result, i, param);
      }
    }
  }

  private interface DraContextualized {

    static List<String> dimensionCollectionListView(int index, Aliases aliases, Deffered<DimensionalRoutedAlias> dra, String[] context, boolean ordered) {
      List<String> cols = aliases.getCollectionAliasListMap().get(dra.get().name);
      LinkedHashSet<String> view = new LinkedHashSet<>(cols.size());
      List<RoutedAlias> dimensions = dra.get().dimensions;
      for (String col : cols) {
        Matcher m = SEP_MATCHER.matcher(col);
        if (!m.find()) {
          throw new IllegalStateException("Invalid Dimensional Routed Alias name:" + col);
        }
        String[] split = SEP_MATCHER.split(col);
        if (split.length != dimensions.size() + 1) {
          throw new IllegalStateException("Dimension Routed Alias collection with wrong number of dimensions. (" +
              col + ") expecting " + dimensions.stream().map(d ->
              d.getRoutedAliasType().toString()).collect(Collectors.toList()));
        }
        boolean matchesAllHigherDims = index == 0;
        boolean matchesAllLowerDims =  context == null || index == context.length - 1;
        if (context != null) {
          for (int i = 0; i < context.length; i++) {
            if (i == index) {
              continue;
            }
            String s = split[i+1];
            String ctx = context[i];
            if (i <= index) {
              matchesAllHigherDims |= s.equals(ctx);
            } else {
              matchesAllLowerDims |= s.equals(ctx);
            }
          }
        } else {
          matchesAllHigherDims = true;
          matchesAllLowerDims = true;
        }
        // dimensions with an implicit order need to start from their initial configuration
        // and count up to maintain order in the alias collection list with respect to that dimension
        if (matchesAllHigherDims && !ordered || matchesAllHigherDims && matchesAllLowerDims) {
          view.add("" + getSeparatorPrefix(dimensions.get(index)) + split[index + 1]);
        }
      }
      return new ArrayList<>(view);
    }

    void setContext(String[] context);
  }

  private static class TimeRoutedAliasDimension extends TimeRoutedAlias implements DraContextualized {
    private final int index;
    private final Deffered<DimensionalRoutedAlias> dra;
    private String[] context;

    TimeRoutedAliasDimension(Map<String, String> props, int index, Deffered<DimensionalRoutedAlias> dra) throws SolrException {
      super("", props);
      this.index = index;
      this.dra = dra;
    }

    @Override
    List<String> getCollectionList(Aliases aliases) {
      return DraContextualized.dimensionCollectionListView(index, aliases, dra, context, true);
    }

    @Override
    public void setContext(String[] context) {
      this.context = context;
    }
  }

  private static class CategoryRoutedAliasDimension extends CategoryRoutedAlias implements DraContextualized {
    private final int index;
    private final Deffered<DimensionalRoutedAlias> dra;
    private String[] context;

    CategoryRoutedAliasDimension(Map<String, String> props, int index, Deffered<DimensionalRoutedAlias> dra) {
      super("", props);
      this.index = index;
      this.dra = dra;
    }

    @Override
    List<String> getCollectionList(Aliases aliases) {
      return DraContextualized.dimensionCollectionListView(index, aliases, dra, context, false);
    }

    @Override
    public void setContext(String[] context) {
      this.context = context;
    }
  }
}
