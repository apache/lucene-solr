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

solrAdminApp.controller('CoreOverviewController',
function($scope, $rootScope, $routeParams, Luke, CoreSystem, Update, Replication, Ping, Constants) {
  $scope.resetMenu("overview", Constants.IS_CORE_PAGE);
  $scope.refreshIndex = function() {
    Luke.index({core: $routeParams.core},
      function(data) {
        $scope.index = data.index;
        delete $scope.statsMessage;
      },
      function(error) {
        $scope.statsMessage = "Luke is not configured";
      }
    );
  };

  $scope.refreshReplication = function() {
    Replication.details({core: $routeParams.core},
      function(data) {
        $scope.isFollower = data.details.isFollower == "true";
        $scope.isLeader = data.details.isLeader == "true";
        $scope.replication = data.details;
      },
      function(error) {
        $scope.replicationMessage = "Replication is not configured";
      });
  };

  $scope.refreshSystem = function() {
    CoreSystem.get({core: $routeParams.core},
      function(data) {
        $scope.core = data.core;
        delete $scope.systemMessage;
      },
      function(error) {
        $scope.systemMessage = "/admin/system Handler is not configured";
      }
    );
  };

  $scope.refreshPing = function() {
    Ping.status({core: $routeParams.core}, function(data) {
      if (data.error) {
        $scope.healthcheckStatus = false;
        if (data.error.code == 503) {
          $scope.healthcheckMessage = 'Ping request handler is not configured with a healthcheck file.';
        }
      } else {
        $scope.healthcheckStatus = data.status == "enabled";
      }
    });
  };

  $scope.toggleHealthcheck = function() {
    if ($scope.healthcheckStatus) {
      Ping.disable(
        {core: $routeParams.core},
        function(data) {$scope.healthcheckStatus = false},
        function(error) {$scope.healthcheckMessage = error}
      );
    } else {
      Ping.enable(
        {core: $routeParams.core},
        function(data) {$scope.healthcheckStatus = true},
        function(error) {$scope.healthcheckMessage = error}
      );
    }
  };

  $scope.refresh = function() {
    $scope.refreshIndex();
    $scope.refreshReplication();
    $scope.refreshSystem();
    $scope.refreshPing();
  };

  $scope.refresh();
});

