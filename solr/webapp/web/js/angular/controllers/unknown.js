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

/**
 * This controller is called whenever no other routes match.
 * It is a place to intercept to look for special flows such as authentication callbacks (that do not support fragment in URL).
 * Normal action is to redirect to dashboard "/" if no login is in progress
 */
solrAdminApp.controller('UnknownController',
  ['$scope', '$window', '$routeParams', '$location', 'Constants', 'AuthenticationService',
    function($scope, $window, $routeParams, $location, Constants, AuthenticationService) {
      var fragment = $window.location.hash.startsWith("#/") ? $window.location.hash.substring(2) : $window.location.hash;
      // Check if the URL is actually a callback from Identiy provider 
      if (AuthenticationService.isJwtCallback(fragment)) {
        console.log("Detected an authentication callback, redirecting to /#/login/callback");
        $location.path("/login/callback").hash(fragment);
      } else {
        console.log("Redirecting from unknown path " + fragment + " to Dashboard");
        $location.path("/").hash("");
      }
    }
  ]
);
