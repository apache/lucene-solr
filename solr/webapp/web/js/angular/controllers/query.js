/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

solrAdminApp.controller('QueryController',
  function($scope, $routeParams, $location, Query, Constants){
    $scope.resetMenu("query", Constants.IS_COLLECTION_PAGE);

    // @todo read URL parameters into scope
    $scope.query = {q:'*:*'};
    $scope.filters = [{fq:""}];
    $scope.dismax = {defType: "dismax"};
    $scope.edismax = {defType: "edismax", stopwords: true, lowercaseOperators: false};
    $scope.hl = {hl:"on"};
    $scope.facet = {facet: "on"};
    $scope.spatial = {};
    $scope.spellcheck = {spellcheck:"on"};
    $scope.debugQuery = {debugQuery: "on"};
    $scope.qt = "/select";

    $scope.doQuery = function() {
      var params = {};

      var set = function(key, value) {
        if (params[key]) {
          params[key].push(value);
        } else {
          params[key] = [value];
        }
      }
      var copy = function(params, query) {
        for (var key in query) {
          terms = query[key];
          // Booleans have no length property - only set them if true
          if (((typeof(terms) == typeof(true) && terms) || terms.length > 0) && key[0]!="$") {
            set(key, terms);
          }
        }
      };

      copy(params, $scope.query);

      if ($scope.isDismax)     copy(params, $scope.dismax);
      if ($scope.isEdismax)    copy(params, $scope.edismax);
      if ($scope.isHighlight)  copy(params, $scope.hl);
      if ($scope.isFacet)      copy(params, $scope.facet);
      if ($scope.isSpatial)    copy(params, $scope.spatial);
      if ($scope.isSpellcheck) copy(params, $scope.spellcheck);
      if ($scope.isDebugQuery) copy(params, $scope.debugQuery);

      if ($scope.rawParams) {
        var rawParams = $scope.rawParams.split(/[&\n]/);
        for (var i in rawParams) {
          var param = rawParams[i];
          var equalPos = param.indexOf("=");
          if (equalPos > -1) {
            set(param.substring(0, equalPos), param.substring(equalPos+1));
          } else {
            set(param, ""); // Use empty value for params without "="
          }
        }
      }

      var qt = $scope.qt ? $scope.qt : "/select";

      for (var filter in $scope.filters) {
        copy(params, $scope.filters[filter]);
      }

      params.core = $routeParams.core;
      if (qt[0] == '/') {
        params.handler = qt.substring(1);
      } else { // Support legacy style handleSelect=true configs
        params.handler = "select";
        set("qt", qt);
      }
      var url = Query.url(params);
      Query.query(params, function(data) {
        $scope.lang = $scope.query.wt;
        if ($scope.lang == undefined || $scope.lang == '') {
          $scope.lang = "json";
        }
        $scope.response = data;
        // Use relative URL to make it also work through proxies that may have a different host/port/context
        $scope.url = url;
        $scope.hostPortContext = $location.absUrl().substr(0,$location.absUrl().indexOf("#")); // For display only
      });
    };

    if ($location.search().q) {
      $scope.query.q = $location.search()["q"];
      $scope.doQuery();
    }

    $scope.removeFilter = function(index) {
      if ($scope.filters.length === 1) {
        $scope.filters = [{fq: ""}];
      } else {
        $scope.filters.splice(index, 1);
      }
    };

    $scope.addFilter = function(index) {
      $scope.filters.splice(index+1, 0, {fq:""});
    };
  }
);
