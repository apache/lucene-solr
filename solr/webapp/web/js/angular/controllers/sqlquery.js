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
solrAdminApp.controller('SQLQueryController',
  function($scope, $routeParams, $location, Query, Constants) {

    $scope.resetMenu("sqlquery", Constants.IS_COLLECTION_PAGE);
    $scope.qt = "sql";
    $scope.doExplanation = false
    $scope.gridOptions = {
        enableSorting: false,
        enableRowHashing:false,
        enableColumnMenus:false,
        columnDefs:[],
        data:[],
        onRegisterApi: function(gridApi) {
            $scope.gridApi = gridApi;
        }
    };
    $scope.hostPortContext = $location.absUrl().substr(0,$location.absUrl().indexOf("#")); // For display only
    $scope.doQuery = function() {

      var params = {};
      params.core = $routeParams.core;
      params.handler = $scope.qt;
      params.stmt = [$scope.stmt]

      $scope.lang = "json";
      $scope.response = null;
      $scope.url = "";
      $scope.gridOptions.data =[]
      $scope.gridOptions.columnDefs = []

      var url = Query.url(params);
      Query.query(params, function(data) {

        var jsonData = JSON.parse(data.toJSON().data);
        $scope.lang = "json";
        $scope.url = url;
        $scope.sqlError = null;
        $scope.sqlData = [];
          if(jsonData != undefined){
              var docs = jsonData['result-set'].docs
              //get all docs
              for (var i = 0; i < docs.length; i++) {
                  var doc = docs[i]
                  //get all the properties
                  if(doc.hasOwnProperty("EOF")){
                      if(doc.hasOwnProperty("EXCEPTION")){
                          $scope.sqlError = doc.EXCEPTION
                      }
                  } else {
                      $scope.gridOptions.data.push(doc);
                  }
              }
          }
          //Build the columnFields from data
          var fields = $scope.gridOptions.data[1];
          for (var property in fields) {
              if (fields.hasOwnProperty(property)) {
                  $scope.gridOptions.columnDefs.push({"name":property, "type":{}})
              }
          }
          $scope.gridApi.core.notifyDataChange
      });
    };
  }
);
