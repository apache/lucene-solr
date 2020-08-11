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

import java.lang.invoke.MethodHandles;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.solr.client.solrj.RoutedAliasTypes;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.AddUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;
import static org.apache.solr.common.params.CollectionAdminParams.ROUTER_PREFIX;

public abstract class RoutedAlias {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  @SuppressWarnings("WeakerAccess")
  public static final String ROUTER_TYPE_NAME = ROUTER_PREFIX + "name";
  @SuppressWarnings("WeakerAccess")
  public static final String ROUTER_FIELD = ROUTER_PREFIX + "field";
  public static final String CREATE_COLLECTION_PREFIX = "create-collection.";
  @SuppressWarnings("WeakerAccess")
  public static final Set<String> MINIMAL_REQUIRED_PARAMS = Sets.newHashSet(ROUTER_TYPE_NAME, ROUTER_FIELD);
  public static final String ROUTED_ALIAS_NAME_CORE_PROP = "routedAliasName"; // core prop
  private static final String DIMENSIONAL = "Dimensional[";

  // This class is created once per request and the overseer methods prevent duplicate create requests
  // from creating extra copies via locking on the alias name. All we need to track here is that we don't
  // spam preemptive creates to the overseer multiple times from *this* request.
  boolean preemptiveCreateOnceAlready = false;

  public static SolrException newAliasMustExistException(String aliasName) {
    throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
        "Routed alias " + aliasName + " appears to have been removed during request processing.");
  }

  /**
   * Factory method for implementations of this interface. There should be no reason to construct instances
   * elsewhere, and routed alias types are encouraged to have package private constructors.
   *
   * @param aliasName The alias name (will be returned by {@link #getAliasName()}
   * @param props     The properties from an overseer message.
   * @return An implementation appropriate for the supplied properties, or null if no type is specified.
   * @throws SolrException If the properties are invalid or the router type is unknown.
   */
  public static RoutedAlias fromProps(String aliasName, Map<String, String> props) throws SolrException {

    String typeStr = props.get(ROUTER_TYPE_NAME);
    if (typeStr == null) {
      return null; // non-routed aliases are being created
    }
    List<RoutedAliasTypes> routerTypes = new ArrayList<>();
    // check for Dimensional[foo,bar,baz]
    if (typeStr.startsWith(DIMENSIONAL)) {
      // multi-dimensional routed alias
      typeStr = typeStr.substring(DIMENSIONAL.length(), typeStr.length() - 1);
      String[] types = typeStr.split(",");
      java.util.List<String> fields = new ArrayList<>();
      if (types.length > 2) {
        throw new SolrException(BAD_REQUEST,"More than 2 dimensions is not supported yet. " +
            "Please monitor SOLR-13628 for progress");
      }
      for (int i = 0; i < types.length; i++) {
        String type = types[i];
        addRouterTypeOf(type, routerTypes);

        // v2 api case - the v2 -> v1 mapping mechanisms can't handle this conversion because they expect
        // strings or arrays of strings, not lists of objects.
        if (props.containsKey("router.routerList")) {
          @SuppressWarnings({"unchecked", "rawtypes"})
          HashMap tmp = new HashMap(props);
          @SuppressWarnings({"unchecked", "rawtypes"})
          List<Map<String, Object>> v2RouterList = (List<Map<String, Object>>) tmp.get("router.routerList");
          Map<String, Object> o = v2RouterList.get(i);
          for (Map.Entry<String, Object> entry : o.entrySet()) {
            props.put(ROUTER_PREFIX + i + "." + entry.getKey(), String.valueOf(entry.getValue()));
          }
        }
        // Here we need to push the type into each dimension's params. We could have eschewed the
        // "Dimensional[dim1,dim2]" style notation, to simplify this case but I think it's nice
        // to be able to understand the dimensionality at a glance without having to hunt for name properties
        // in the list of properties for each dimension.
        String typeName = ROUTER_PREFIX + i + ".name";
        // can't use computeIfAbsent because the non-dimensional case where typeName is present
        // happens to be an unmodifiable map and will fail.
        if (!props.containsKey(typeName)) {
          props.put(typeName, type);
        }
        fields.add(props.get(ROUTER_PREFIX + i + ".field"));
      }
      // this next remove is checked for key because when we build from aliases.json's data it we get an
      // immutable map which would cause  UnsupportedOperationException to be thrown. This remove is here
      // to prevent this property from making it into aliases.json
      if (props.containsKey("router.routerList")) {
        props.remove("router.routerList");
      }
      // Keep code that handles single dimensions happy by providing this value, otherwise ignored.
      if (!props.containsKey(ROUTER_FIELD)) {
        props.put(ROUTER_FIELD, String.join(",", fields));
      }
    } else {
      // non-dimensional case
      addRouterTypeOf(typeStr, routerTypes);
    }
    if (routerTypes.size() == 1) {
      RoutedAliasTypes routerType = routerTypes.get(0);
      return routedAliasForType(aliasName, props, routerType);
    } else {
      List<RoutedAlias> dimensions = new ArrayList<>();
      // this array allows us to get past the chicken/egg problem of needing access to the
      // DRA inside the dimensions, but needing the dimensions to create the DRA
      DimensionalRoutedAlias[] dra = new DimensionalRoutedAlias[1];
      for (int i = 0; i < routerTypes.size(); i++) {
        RoutedAliasTypes routerType = routerTypes.get(i);
        // NOTE setting the name to empty string is very important here, as that allows us to simply
        // concatenate the "names" of the parts to get the correct collection name for the DRA
        dimensions.add(DimensionalRoutedAlias.dimensionForType( selectForIndex(i, props), routerType, i, () -> dra[0]));
      }
      return dra[0] = new DimensionalRoutedAlias(dimensions, props.get(CommonParams.NAME), props);
    }
  }

  private static void addRouterTypeOf(String type, List<RoutedAliasTypes> routerTypes) {
    try {
      routerTypes.add(RoutedAliasTypes.valueOf(type.toUpperCase(Locale.ENGLISH)));
    } catch (IllegalArgumentException iae) {
      throw new SolrException(BAD_REQUEST, "Router name: " + type + " is not in supported types, "
          + Arrays.asList(RoutedAliasTypes.values()));
    }
  }

  private static Map<String, String> selectForIndex(int i, Map<String, String> original) {
    return original.entrySet().stream()
        .filter(e -> e.getKey().matches("(((?!^router\\.).)*$|(^router\\." + i + ".*$))"))
        .map(e -> new SimpleEntry<>(e.getKey().replaceAll("(.*\\.)" + i + "\\.(.*)", "$1$2"), e.getValue()))
        .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
  }

  private static RoutedAlias routedAliasForType(String aliasName, Map<String, String> props, RoutedAliasTypes routerType) {
    // this switch must have a case for every element of the RoutedAliasTypes enum EXCEPT DIMENSIONAL
    switch (routerType) {
      case TIME:
        return new TimeRoutedAlias(aliasName, props);
      case CATEGORY:
        return new CategoryRoutedAlias(aliasName, props);
      default:
        // if we got a type not handled by the switch there's been a bogus implementation.
        throw new SolrException(SERVER_ERROR, "Router " + routerType + " is not fully implemented. If you see this" +
            "error in an official release please file a bug report. Available types were:"
            + Arrays.asList(RoutedAliasTypes.values()));

    }
  }

  /**
   * Ensure our parsed version of the alias collection list is up to date. If it was modified, return true.
   * Note that this will return true if some other alias was modified or if properties were modified. These
   * are spurious and the caller should be written to be tolerant of no material changes.
   */
  public abstract boolean updateParsedCollectionAliases(ZkStateReader zkStateReader, boolean conextualize);

  List<String> getCollectionList(Aliases aliases) {
    return aliases.getCollectionAliasListMap().get(getAliasName());
  }

  /**
   * Create the initial collection for this RoutedAlias if applicable.
   * <p>
   * Routed Aliases do not aggregate existing collections, instead they create collections on the fly. If the initial
   * collection can be determined from initialization parameters it should be calculated here.
   *
   * @return optional string of initial collection name
   */
  abstract String computeInitialCollectionName();

  abstract String[] formattedRouteValues(SolrInputDocument doc) ;

  /**
   * The name of the alias. This name is used in place of a collection name for both queries and updates.
   *
   * @return The name of the Alias.
   */
  public abstract String getAliasName();

  abstract String getRouteField();

  abstract RoutedAliasTypes getRoutedAliasType();

  /**
   * Check that the value we will be routing on is legal for this type of routed alias.
   *
   * @param cmd the command containing the document
   */
  public abstract void validateRouteValue(AddUpdateCommand cmd) throws SolrException;

  /**
   * Create any required collections and return the name of the collection to which the current document should be sent.
   *
   * @param cmd The command that might cause collection creation
   * @return The name of the proper destination collection for the document which may or may not be a
   * newly created collection
   */
  public String createCollectionsIfRequired(AddUpdateCommand cmd) {

    // Even though it is possible that multiple requests hit this code in the 1-2 sec that
    // it takes to create a collection, it's an established anti-pattern to feed data with a very large number
    // of client connections. This in mind, we only guard against spamming the overseer within a batch of
    // updates. We are intentionally tolerating a low level of redundant requests in favor of simpler code. Most
    // super-sized installations with many update clients will likely be multi-tenant and multiple tenants
    // probably don't write to the same alias. As such, we have deferred any solution to the "many clients causing
    // collection creation simultaneously" problem until such time as someone actually has that problem in a
    // real world use case that isn't just an anti-pattern.
    CandidateCollection candidateCollectionDesc = findCandidateGivenValue(cmd);

    try {
      // It's important not to add code between here and the prior call to findCandidateGivenValue()
      // in processAdd() that invokes updateParsedCollectionAliases(). Doing so would update parsedCollectionsDesc
      // and create a race condition. When Routed aliases have an implicit sort for their collections we
      // are relying on the fact that collectionList.get(0) is returning the head of the parsed collections that
      // existed when the collection list was consulted for the candidate value. If this class updates it's notion
      // of the list of collections since candidateCollectionDesc was chosen, we could create collection n+2
      // instead of collection n+1.
      return createAllRequiredCollections( cmd, candidateCollectionDesc);
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /**
   * @return get alias related metadata
   */
  abstract Map<String, String> getAliasMetadata();

  public abstract Set<String> getRequiredParams();

  public abstract Set<String> getOptionalParams();

  abstract CandidateCollection findCandidateGivenValue(AddUpdateCommand cmd);

  class CandidateCollection {
    private final CreationType creationType;
    private final String destinationCollection;
    private final String creationCollection;

    CandidateCollection(CreationType creationType, String destinationCollection, String creationCollection) {
      this.creationType = creationType;
      this.destinationCollection = destinationCollection;
      this.creationCollection = creationCollection;
    }

    CandidateCollection(CreationType creationType, String collection) {
      this.creationType = creationType;
      this.destinationCollection = collection;
      this.creationCollection = collection;
    }

    CreationType getCreationType() {
      return creationType;
    }

    String getDestinationCollection() {
      return destinationCollection;
    }

    String getCreationCollection() {
      return creationCollection;
    }
  }

  /**
   * Create as many collections as required. This method loops to allow for the possibility that the route value
   * requires more than one collection to be created. Since multiple threads may be invoking maintain on separate
   * requests to the same alias, we must pass in a descriptor that details what collection is to be created.
   * This assumption is checked when the command is executed in the overseer. When this method
   * finds that all collections required have been created it returns the (possibly new) destination collection
   * for the document that caused the creation cycle.
   *
   * @param cmd                  the update command being processed
   * @param targetCollectionDesc the descriptor for the presently selected collection .
   * @return The destination collection, possibly created during this method's execution
   */
  private String createAllRequiredCollections(AddUpdateCommand cmd, CandidateCollection targetCollectionDesc) {

    SolrQueryRequest req = cmd.getReq();
    SolrCore core = req.getCore();
    CoreContainer coreContainer = core.getCoreContainer();
    do {
      switch (targetCollectionDesc.getCreationType()) {
        case NONE:
          return targetCollectionDesc.destinationCollection; // we don't need another collection
        case SYNCHRONOUS:
          targetCollectionDesc = doSynchronous( cmd, targetCollectionDesc, coreContainer);
          break;
        case ASYNC_PREEMPTIVE:
          return doPreemptive(targetCollectionDesc, core, coreContainer);
        default:
          throw unknownCreateType();
      }
    } while (true);
  }

  private CandidateCollection doSynchronous(AddUpdateCommand cmd, CandidateCollection targetCollectionDesc, CoreContainer coreContainer) {
    ensureCollection(targetCollectionDesc.getCreationCollection(), coreContainer); // *should* throw if fails for some reason but...
    ZkController zkController = coreContainer.getZkController();
    updateParsedCollectionAliases(zkController.zkStateReader, true);
    List<String> observedCols = zkController.zkStateReader.aliasesManager.getAliases().getCollectionAliasListMap().get(getAliasName());
    if (!observedCols.contains(targetCollectionDesc.creationCollection)) {
      // if collection creation did not occur we've failed. Bail out.
      throw new SolrException(SERVER_ERROR, "After we attempted to create " + targetCollectionDesc.creationCollection + " it did not exist");
    }
    // then recalculate the candiate, which may result in continuation or termination the loop calling this method
    targetCollectionDesc = findCandidateGivenValue(cmd);
    return targetCollectionDesc;
  }

  private String doPreemptive(CandidateCollection targetCollectionDesc, SolrCore core, CoreContainer coreContainer) {

    if (!this.preemptiveCreateOnceAlready) {
      preemptiveAsync(() -> {
        try {
          ensureCollection(targetCollectionDesc.creationCollection, coreContainer);
        } catch (Exception e) {
          log.error("Async creation of a collection for routed Alias {} failed!", this.getAliasName(), e);
        }
      }, core);
    }
    return targetCollectionDesc.destinationCollection;
  }

  /**
   * Calculate the head collection (i.e. the most recent one for a TRA) if this routed alias has an
   * implicit order, or if the collection is unordered return the appropriate collection name
   * for the value in the current document. This method should never return null.
   */
  abstract protected String getHeadCollectionIfOrdered(AddUpdateCommand cmd);

  private void preemptiveAsync(Runnable r, SolrCore core) {
    preemptiveCreateOnceAlready = true;
    core.runAsync(r);
  }

  private SolrException unknownCreateType() {
    return new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown creation type while adding " +
        "document to a Time Routed Alias! This is a bug caused when a creation type has been added but " +
        "not all code has been updated to handle it.");
  }

  void ensureCollection(String targetCollection, CoreContainer coreContainer) {
    CollectionsHandler collectionsHandler = coreContainer.getCollectionsHandler();

    // Invoke MANINTAIN_ROUTED_ALIAS (in the Overseer, locked by alias name).  It will create the collection
    //   and update the alias contingent on the requested collection name not already existing.
    //   otherwise it will return (without error).
    try {
      MaintainRoutedAliasCmd.remoteInvoke(collectionsHandler, getAliasName(), targetCollection);
      // we don't care about the response.  It's possible no collection was created because
      //  of a race and that's okay... we'll ultimately retry any way.

      // Ensure our view of the aliases has updated. If we didn't do this, our zkStateReader might
      //  not yet know about the new alias (thus won't see the newly added collection to it), and we might think
      //  we failed.
      coreContainer.getZkController().getZkStateReader().aliasesManager.update();
      updateParsedCollectionAliases(coreContainer.getZkController().getZkStateReader(),false);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /**
   * Determine the combination of adds/deletes implied by the arrival of a document destined for the
   * specified collection.
   *
   * @param targetCol the collection for which a document is destined.
   * @return A list of actions across the DRA.
   */
  protected abstract List<Action> calculateActions(String targetCol);

  protected static class Action {
    final RoutedAlias sourceAlias;
    final ActionType actionType;
    final String targetCollection; // dra's need to edit this so not final

    public Action(RoutedAlias sourceAlias, ActionType actionType, String targetCollection) {
      this.sourceAlias = sourceAlias;
      this.actionType = actionType;
      this.targetCollection = targetCollection;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Action action = (Action) o;
      return Objects.equals(sourceAlias, action.sourceAlias) &&
          actionType == action.actionType &&
          Objects.equals(targetCollection, action.targetCollection);
    }

    @Override
    public int hashCode() {
      return Objects.hash(sourceAlias, actionType, targetCollection);
    }

  }

  enum ActionType {
    ENSURE_REMOVED,
    ENSURE_EXISTS
  }

    enum CreationType {
    NONE,
    ASYNC_PREEMPTIVE,
    SYNCHRONOUS,
  }
}
