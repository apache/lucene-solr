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

solrAdminApp.controller('LoginController',
    ['$scope', '$routeParams', '$rootScope', '$location', '$window', 'AuthenticationService', 'Constants',
      function ($scope, $routeParams, $rootScope, $location, $window, AuthenticationService, Constants) {
        $scope.resetMenu("login", Constants.IS_ROOT_PAGE);
        $scope.subPath = $routeParams.route;
        $rootScope.exceptions = {};

        // Session variables set in app.js 401 interceptor
        var wwwAuthHeader = sessionStorage.getItem("auth.wwwAuthHeader");
        var authScheme = sessionStorage.getItem("auth.scheme");
        if (wwwAuthHeader) {
          // Parse www-authenticate header
          var wwwHeader = wwwAuthHeader.match(/(\w+)(\s+)?(.*)/);
          authScheme = "unknown";
          var authParams = {};
          if (wwwHeader && wwwHeader.length >= 1)
            authScheme = wwwHeader[1]; 
          if (wwwHeader && wwwHeader.length >= 3)
            authParams = www_auth_parse_params(wwwHeader[3]);
          if (typeof authParams === 'string' || authParams instanceof String) {
            $scope.authParamsError = authParams;
          } else {
            $scope.authParamsError = undefined;
          }
          var realm = authParams['realm'];
          sessionStorage.setItem("auth.realm", realm);
          if (authScheme === 'Basic' || authScheme === 'xBasic') {
            authScheme = 'Basic';
          }
          sessionStorage.setItem("auth.scheme", authScheme);
        }

        var supportedSchemes = ['Basic', 'Bearer', 'Negotiate'];
        $scope.authSchemeSupported = supportedSchemes.includes(authScheme);
        $scope.authScheme = sessionStorage.getItem("auth.scheme");
        $scope.authRealm = sessionStorage.getItem("auth.realm");
        $scope.wwwAuthHeader = sessionStorage.getItem("auth.wwwAuthHeader");
        $scope.statusText = sessionStorage.getItem("auth.statusText");
        $scope.authConfig = sessionStorage.getItem("auth.config");
        $scope.authLocation = sessionStorage.getItem("auth.location");
        $scope.authLoggedinUser = sessionStorage.getItem("auth.username");
        $scope.authHeader = sessionStorage.getItem("auth.header");

        $scope.login = function () {
          AuthenticationService.SetCredentials($scope.username, $scope.password);
          $location.path($scope.authLocation); // Redirect to the location that caused the login prompt
        };

        $scope.logout = function() {
          // reset login status
          AuthenticationService.ClearCredentials();
          $location.path("/");
        };

        $scope.isLoggedIn = function() {
          return (sessionStorage.getItem("auth.username") !== null);
        };
      }]);

// This function is copied and adapted from MIT-licensed https://github.com/randymized/www-authenticate/blob/master/lib/parsers.js
www_auth_parse_params= function (header) {
  // This parser will definitely fail if there is more than one challenge
  var params = {};
  var tok, last_tok, _i, _len, key, value;
  var state= 0;   //0: token,
  var m= header.split(/([",=])/);
  for (_i = 0, _len = m.length; _i < _len; _i++) {
    last_tok= tok;
    tok = m[_i];
    if (!tok.length) continue;
    switch (state) {
      case 0: // token
        key= tok.trim();
        state= 1; // expect equals
        continue;
      case 1: // expect equals
        if ('=' != tok) return 'Equal sign was expected after '+key;
        state= 2;
        continue;
      case 2: // expect value
        if ('"' == tok) {
          value= '';
          state= 3; // expect quoted
          continue;
        }
        else {
          params[key]= value= tok.trim();
          state= 9; // expect comma or end
          continue;
        }
      case 3: // handling quoted string
        if ('"' == tok) {
          state= 8; // end quoted
          continue;
        }
        else {
          value+= tok;
          state= 3; // continue accumulating quoted string
          continue;
        }
      case 8: // end quote encountered
        if ('"' == tok) {
          // double quoted
          value+= '"';
          state= 3; // back to quoted string
          continue;
        }
        if (',' == tok) {
          params[key]= value;
          state= 0;
          continue;
        }
        else {
          return 'Unexpected token ('+tok+') after '+value+'"';
        }
        continue;
      case 9: // expect commma
        if (',' != tok) return 'Comma expected after '+value;
        state= 0;
        continue;
    }
  }
  switch (state) {  // terminal state
    case 0:   // Empty or ignoring terminal comma
    case 9:   // Expecting comma or end of header
      return params;
    case 8:   // Last token was end quote
      params[key]= value;
      return params;
    default:
      return 'Unexpected end of www-authenticate value.';
  }
};
