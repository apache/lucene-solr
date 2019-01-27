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

        if (authScheme === 'Bearer') {
          // Check for OpenId redirect response
          var errorText = "";
          $scope.isCallback = false;
          if ($scope.subPath === 'callback') {
            $scope.isCallback = true;
            var hash = $location.hash();
            var hp = AuthenticationService.decodeHashParams(hash);
            var expectedState = sessionStorage.getItem("auth.stateRandom") + "_" + sessionStorage.getItem("auth.location");
            sessionStorage.setItem("auth.state", "error");
            if (hp['access_token'] && hp['token_type'] && hp['state']) {
              // Validate state
              if (hp['state'] !== expectedState) {
                $scope.error = "Problem with auth callback";
                console.log("Expected state param " + expectedState + " but got " + hp['state']);
                errorText += "Invalid values in state parameter. ";
              }
              // Validate token type
              if (hp['token_type'].toLowerCase() !== "bearer") {
                console.log("Expected token_type param 'bearer', but got " + hp['token_type']);
                errorText += "Invalid values in token_type parameter. ";
              }
              // Unpack ID token and validate nonce, get username
              if (hp['id_token']) {
                var idToken = hp['id_token'].split(".");
                if (idToken.length === 3) {
                  var payload = AuthenticationService.decodeJwtPart(idToken[1]);
                  if (!payload['nonce'] || payload['nonce'] !== sessionStorage.getItem("auth.nonce")) {
                    errorText += "Invalid 'nonce' value, possible attack detected. Please log in again. ";
                  }

                  if (errorText === "") {
                    sessionStorage.setItem("auth.username", payload['sub']);
                    sessionStorage.setItem("auth.header", "Bearer " + hp['access_token']);
                    sessionStorage.removeItem("auth.statusText");
                    sessionStorage.removeItem("auth.stateRandom");
                    sessionStorage.removeItem("auth.wwwAuthHeader");
                    console.log("User " + payload['sub'] + " is logged in");
                    var redirectTo = sessionStorage.getItem("auth.location");
                    console.log("Redirecting to stored location " + redirectTo);
                    sessionStorage.setItem("auth.state", "authenticated");
                    sessionStorage.removeItem("http401");
                    $location.path(redirectTo).hash("");
                  }
                } else {
                  console.log("Expected JWT compact id_token param but got " + idToken);
                  errorText += "Invalid values in id_token parameter. ";
                }
              } else {
                console.log("Callback was missing the id_token parameter, could not validate nonce.");
                errorText += "Callback was missing the id_token parameter, could not validate nonce. ";
              }
              if (hp['access_token'].split(".").length !== 3) {
                console.log("Expected JWT compact access_token param but got " + hp['access_token']);
                errorText += "Invalid values in access_token parameter. ";
              }
              if (errorText !== "") {
                $scope.error = "Problems with OpenID callback";
                $scope.errorDescription = errorText;
                $scope.http401 = "true";
              }
              // End callback processing
            } else if (hp['error']) {
              // The callback had errors
              console.log("Error received from idp: " + hp['error']);
              var errorDescriptions = {};
              errorDescriptions['invalid_request'] = "The request is missing a required parameter, includes an invalid parameter value, includes a parameter more than once, or is otherwise malformed.";
              errorDescriptions['unauthorized_client'] = "The client is not authorized to request an access token using this method.";
              errorDescriptions['access_denied'] = "The resource owner or authorization server denied the request.";
              errorDescriptions['unsupported_response_type'] = "The authorization server does not support obtaining an access token using this method.";
              errorDescriptions['invalid_scope'] = "The requested scope is invalid, unknown, or malformed.";
              errorDescriptions['server_error'] = "The authorization server encountered an unexpected condition that prevented it from fulfilling the request.";
              errorDescriptions['temporarily_unavailable'] = "The authorization server is currently unable to handle the request due to a temporary overloading or maintenance of the server.";
              $scope.error = "Callback from Id Provider contained error. ";
              if (hp['error_description']) {
                $scope.errorDescription = decodeURIComponent(hp['error_description']);
              } else {
                $scope.errorDescription = errorDescriptions[hp['error']];
              }
              if (hp['error_uri']) {
                $scope.errorDescription += " More information at " + hp['error_uri'] + ". ";
              }
              if (hp['state'] !== expectedState) {
                $scope.errorDescription += "The state parameter returned from ID Provider did not match the one we sent.";
              }
              sessionStorage.setItem("auth.state", "error");
            }
          }
        }

        if (errorText === "" && !$scope.error && authParams) {
          $scope.error = authParams['error'];
          $scope.errorDescription = authParams['error_description'];
          $scope.authData = AuthenticationService.getAuthDataHeader();
        }
        
        $scope.authScheme = sessionStorage.getItem("auth.scheme");
        $scope.authRealm = sessionStorage.getItem("auth.realm");
        $scope.wwwAuthHeader = sessionStorage.getItem("auth.wwwAuthHeader");
        $scope.statusText = sessionStorage.getItem("auth.statusText");
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

        $scope.jwtLogin = function () {
          var stateRandom = Math.random().toString(36).substr(2);
          sessionStorage.setItem("auth.stateRandom", stateRandom);
          var authState = stateRandom + "_" + sessionStorage.getItem("auth.location");
          var authNonce = Math.random().toString(36).substr(2) + Math.random().toString(36).substr(2) + Math.random().toString(36).substr(2);
          sessionStorage.setItem("auth.nonce", authNonce);
          var params = {
            "response_type" : "id_token token",
            "client_id" : $scope.authData['client_id'],
            "redirect_uri" : $window.location.href.split('#')[0],
            "scope" : "openid " + $scope.authData['scope'],
            "state" : authState,
            "nonce" : authNonce
          };

          var endpointBaseUrl = $scope.authData['authorizationEndpoint'];
          var loc = endpointBaseUrl + "?" + paramsToString(params);
          console.log("Redirecting to " + loc);
          sessionStorage.setItem("auth.state", "expectCallback");
          $window.location.href = loc;

          function paramsToString(params) {
            var arr = [];
            for (var p in params) {
               if( params.hasOwnProperty(p) ) {
                 arr.push(p + "=" + encodeURIComponent(params[p]));
               } 
             }
             return arr.join("&");
          }
        };

        $scope.jwtIsLoginNode = function() {
          var redirect = $scope.authData ? $scope.authData['redirect_uris'] : undefined;
          if (redirect && Array.isArray(redirect) && redirect.length > 0) {
            var isLoginNode = false;
            redirect.forEach(function(uri) { // Check that current node URL is among the configured callback URIs
              if ($window.location.href.startsWith(uri)) isLoginNode = true;
            });
            return isLoginNode; 
          } else {
            return true; // no redirect UIRs configured, all nodes are potential login nodes
          }
        };

        $scope.jwtFindLoginNode = function() {
          var redirect = $scope.authData ? $scope.authData['redirect_uris'] : undefined;
          if (redirect && Array.isArray(redirect) && redirect.length > 0) {
            var loginNode = redirect[0];
            redirect.forEach(function(uri) { // if current node is in list, return its callback uri
              if ($window.location.href.startsWith(uri)) loginNode = uri;
            });
            return loginNode; 
          } else {
             return $window.location.href.split('#')[0]; // Return base url of current URL as the url to use 
          }
        };

        // Redirect to login node if this is not a valid one
        $scope.jwtGotoLoginNode = function() {
          if (!$scope.jwtIsLoginNode()) {
            $window.location.href = $scope.jwtFindLoginNode();
          }
        };

        $scope.jwtLogout = function() {
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
