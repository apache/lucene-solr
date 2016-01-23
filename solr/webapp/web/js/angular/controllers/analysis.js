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

solrAdminApp.controller('AnalysisController',
  function($scope, $location, $routeParams, Luke, Analysis, Constants) {
      $scope.resetMenu("analysis", Constants.IS_COLLECTION_PAGE);

      $scope.refresh = function() {
        Luke.schema({core: $routeParams.core}, function(data) {
          $scope.fieldsAndTypes = [];
          for (var field in data.schema.fields) {
            $scope.fieldsAndTypes.push({
              group: "Fields",
              value: "fieldname=" + field,
              label: field});
          }
          for (var type in data.schema.types) {
            $scope.fieldsAndTypes.push({
              group: "Types",
              value: "fieldtype=" + type,
              label: type});
          }
          $scope.core = $routeParams.core;
        });

        $scope.parseQueryString();
        // @todo - set defaultSearchField either to context["analysis.fieldname"] or context["analysis.fieldtype"]

      };
      $scope.verbose = true;

      var getShortComponentName = function(longname) {
        var short = -1 !== longname.indexOf( '$' )
                         ? longname.split( '$' )[1]
                         : longname.split( '.' ).pop();
        return short.match( /[A-Z]/g ).join( '' );
      };

      var getCaptionsForComponent = function(data) {
        var captions = [];
        for (var key in data[0]) {
          key = key.replace(/.*#/,'');
          if (key != "match" && key!="positionHistory") {
            captions.push(key.replace(/.*#/,''));
          }
        }
        return captions;
      };

      var getTokensForComponent = function(data) {
        var tokens = [];
        var previousPosition = 0;
        var index=0;
        for (var i in data) {
          var tokenhash = data[i];
          var positionDifference = tokenhash.position - previousPosition;
          for (var j=positionDifference; j>1; j--) {
            tokens.push({position: tokenhash.position - j+1, blank:true, index:index++});
          }

          var token = {position: tokenhash.position, keys:[], index:index++};

          for (key in tokenhash) {
            if (key == "match" || key=="positionHistory") {
              //@ todo do something
            } else {
              token.keys.push({name:key, value:tokenhash[key]});
            }
          }
          tokens.push(token);
          previousPosition = tokenhash.position;
        }
        return tokens;
      };

      var extractComponents = function(data, result, name) {
        if (data) {
            result[name] = [];
            for (var i = 0; i < data.length; i += 2) {
                var component = {
                    name: data[i],
                    short: getShortComponentName(data[i]),
                    captions: getCaptionsForComponent(data[i + 1]),
                    tokens: getTokensForComponent(data[i + 1])
                };
                result[name].push(component);
            }
        }
      };

      var processAnalysisData = function(analysis, fieldOrType) {
        var fieldname;
        for (fieldname in analysis[fieldOrType]) {console.log(fieldname);break;}
        var response = {};
        extractComponents(analysis[fieldOrType][fieldname].index, response, "index");
        extractComponents(analysis[fieldOrType][fieldname].query, response, "query");
        return response;
      };

      $scope.updateQueryString = function() {

        var parts = $scope.fieldOrType.split("=");
        var fieldOrType = parts[0];
        var name = parts[1];

        if ($scope.indexText) {
            $location.search("analysis.fieldvalue", $scope.indexText);
        } else if ($location.search()["analysis.fieldvalue"]) {
            $location.search("analysis.fieldvalue", null);
        }
        if ($scope.queryText) {
          $location.search("analysis.query", $scope.queryText);
        } else if ($location.search()["analysis.query"]) {
            $location.search("analysis.query", null);
        }

        if (fieldOrType == "fieldname") {
          $location.search("analysis.fieldname", name);
          $location.search("analysis.fieldtype", null);
        } else {
          $location.search("analysis.fieldtype", name);
          $location.search("analysis.fieldname", null);
        }
        $location.search("verbose_output", $scope.verbose ? "1" : "0");
      };

      $scope.parseQueryString = function () {
          var params = {};
          var search = $location.search();

          if (Object.keys(search).length == 0) {
              return;
          }
          for (var key in search) {
              params[key]=search[key];
          }
          $scope.indexText = search["analysis.fieldvalue"];
          $scope.queryText = search["analysis.query"];
          if (search["analysis.fieldname"]) {
              $scope.fieldOrType = "fieldname=" + search["analysis.fieldname"];
              $scope.schemaBrowserUrl = "field=" + search["analysis.fieldname"];
          } else {
              $scope.fieldOrType = "fieldtype=" + search["analysis.fieldtype"];
              $scope.schemaBrowserUrl = "type=" + search["analysis.fieldtype"];
          }
          if (search["verbose_output"] == undefined) {
            $scope.verbose = true;
          } else {
            $scope.verbose = search["verbose_output"] == "1";
          }

          if ($scope.fieldOrType || $scope.indexText || $scope.queryText) {
            params.core = $routeParams.core;
            var parts = $scope.fieldOrType.split("=");
            var fieldOrType = parts[0] == "fieldname" ? "field_names" : "field_types";

            Analysis.field(params, function(data) {
              $scope.result = processAnalysisData(data.analysis, fieldOrType);
            });
          }
      };

      $scope.changeFieldOrType = function() {
        var parts = $scope.fieldOrType.split("=");
        if (parts[0]=='fieldname') {
          $scope.schemaBrowserUrl = "field=" + parts[1];
        } else {
          $scope.schemaBrowserUrl = "type=" + parts[1];
        }
      };

      $scope.toggleVerbose = function() {
        $scope.verbose = !$scope.verbose;
        $scope.updateQueryString();
      };

      $scope.refresh();
    }
);

/***************

function(error) {
  if (error.status == 404) {
    $scope.isHandlerMissing = true;
  } else {
    $scope.analysisError = error.error.msg;
  }
****/