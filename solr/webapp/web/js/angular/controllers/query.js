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

    $scope._models = [];
    $scope.filters = [{fq:""}];
    $scope.val = {};
    $scope.val['q'] = "*:*";
    $scope.val['q.op'] = "OR";
    $scope.val['defType'] = "";
    $scope.val['indent'] = true;

    // get list of ng-models that have a form element
    function setModels(argTagName){
        var fields = document.getElementsByTagName(argTagName);
        for( var i = 0, iLen = fields.length; i<iLen; i++ ){
            var model = fields[i].getAttribute("ng-model");
            if( model ){
                $scope._models.push({modelName: model, element: fields[i]});
            }
        }
    }
    function setUrlParams(){
      var urlParams = $location.search();
      for( var p in urlParams ){
        if( !urlParams.hasOwnProperty(p) ){
          continue;
        }
        if( p === "fq" ) {
            // filters are handled specially because of possible multiple values
            addFilters(urlParams[p]);
        } else {
            setParam(p, urlParams[p]);
        }
      }
    }
    function setParam(argKey, argValue, argProperty){
      if( argProperty ){
        $scope[argProperty][argKey] = argValue;
      } else if ( $scope._models.map(function(field){return field.modelName}).indexOf("val['" + argKey + "']") > -1 ) {
        // parameters stored in "val" will be used in both links (admin link and REST-request)
        var index = $scope._models.map(function(field){return field.modelName}).indexOf("val['" + argKey + "']");
        var field = $scope._models[index].element;
        if( argValue === "true" || argValue === "false"){
          // use real booleans
          argValue = argValue === "true";
        } else if( field.tagName.toUpperCase() === "INPUT" && field.type === "checkbox" && (argValue === "on" || argValue === "off") ){
          argValue = argValue === "on";
        }
        $scope.val[argKey] = argValue;
      } else if( $scope._models.map(function(field){return field.modelName}).indexOf(argKey) > -1 ) {
        // parameters that will only be used to generate the admin link
        $scope[argKey] = argValue;
      } else {
        insertToRawParams(argKey, argValue);
      }
    }
    // store not processed values to be displayed in a field
    function insertToRawParams(argKey, argValue){
      if( $scope.rawParams == null ){
        $scope.rawParams = "";
      } else {
        $scope.rawParams += "&";
      }
      $scope.rawParams += argKey + "=" + argValue;
    }
    function addFilters(argObject){
      if( argObject ){
        $scope.filters = [];
          if( Array.isArray(argObject) ){
            for( var i = 0, iLen = argObject.length; i<iLen; i++ ){
              $scope.filters.push({fq: argObject[i]});
            }
          } else {
            $scope.filters.push({fq: argObject});
          }
      }
    }

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
          if (typeof(terms) == typeof(true) || (terms.length && terms.length > 0 && key[0]!=="$") ) {
            set(key, terms);
          }
        }
      }
      // remove non-visible fields from the request-params
      var purgeParams = function(params, fields, bRemove){
        if( !bRemove ){
            return;
        }
        for( var i = 0, iLen = fields.length; i<iLen; i++ ){
            if( params.hasOwnProperty(fields[i]) ){
                delete params[fields[i]];
            }
        }
      }
      var getDependentFields = function(argParam){
          return Object.keys($scope.val)
              .filter(function(param){
                  return param.indexOf(argParam) === 0;
              });
      }

      copy(params, $scope.val);

      purgeParams(params, ["q.alt", "qf", "mm", "pf", "ps", "qs", "tie", "bq", "bf"], $scope.val.defType !== "dismax" && $scope.val.defType !== "edismax");
      purgeParams(params, ["uf", "pf2", "pf3", "ps2", "ps3", "boost", "stopwords", "lowercaseOperators"], $scope.val.defType !== "edismax");
      purgeParams(params, getDependentFields("hl"), $scope.val.hl !== true);
      purgeParams(params, getDependentFields("facet"), $scope.val.facet !== true);
      purgeParams(params, getDependentFields("spatial"), $scope.val.spatial !== true);
      purgeParams(params, getDependentFields("spellcheck"), $scope.val.spellcheck !== true);

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
      // create rest result url
      var url = Query.url(params);

      // create admin page url
      var adminParams = {...params};
      delete adminParams.handler;
      delete adminParams.core
      if( $scope.qt != null ) {
        adminParams.qt = [$scope.qt];
      }

      Query.query(params, function(data) {
        $scope.lang = $scope.val['wt'];
        if ($scope.lang == undefined || $scope.lang == '') {
          $scope.lang = "json";
        }
        $scope.response = data;
        // Use relative URL to make it also work through proxies that may have a different host/port/context
        $scope.url = url;
        $scope.hostPortContext = $location.absUrl().substr(0,$location.absUrl().indexOf("#")); // For display only
        for( key in $location.search() ){
            $location.search(key, null);
        }
        for( var key in adminParams ){
          if( Array.isArray(adminParams[key]) && adminParams[key].length === 1 ){
            adminParams[key] = adminParams[key][0];
          }
          if( typeof adminParams[key] === typeof true ){
            adminParams[key] = adminParams[key].toString();
          }
          $location.search(key, adminParams[key]);
        }
      });
    };
    setModels("input");
    setModels("textarea");
    setModels("select");
    setUrlParams();

    if ($location.search().q) {
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
