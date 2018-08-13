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
solrAdminApp.controller('ClusterSuggestionsController',
function($scope, $http, Constants) {
    $scope.resetMenu("cluster-suggestion", Constants.IS_COLLECTION_PAGE);
    $scope.data={};
    var dataArr =[];
    var dataJson = {};
    //function to display suggestion
    $http({
       method: 'GET',
       url: '/api/cluster/autoscaling/suggestions'
    }).then(function successCallback(response) {
         $scope.data = response.data;
         $scope.parsedData = $scope.data.suggestions;
      }, function errorCallback(response) {
      });
    //function to perform operation
    $scope.postdata = function (x) {
        x.loading = true;
        var path=x.operation.path;
        var command=x.operation.command;
        var fullPath='/api/'+path;
        console.log(fullPath);
        console.log(command);
        $http.post(fullPath, JSON.stringify(command)).then(function (response) {
            if (response.data)
            console.log(response.data);
            x.loading = false;
            x.done = true;
            x.run=true;
            $scope.msg = "Post Data Submitted Successfully!";
        }, function (response) {
            x.failed=true;
            $scope.msg = "Service not Exists";
            $scope.statusval = response.status;
            $scope.statustext = response.statusText;
            $scope.headers = response.headers();
        });
    };
    $scope.showPopover = function() {
      $scope.popup = true;
    };

    $scope.hidePopover = function () {
      $scope.popup = false;
    };
});
