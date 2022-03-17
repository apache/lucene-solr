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

solrAdminApp.controller('SecurityController', function ($scope, $timeout, $cookies, $window, Constants, System, Security) {
  $scope.resetMenu("security", Constants.IS_ROOT_PAGE);

  $scope.params = [];
  $scope.filteredPredefinedPermissions = [];

  var strongPasswordRegex = /^(?=.*[0-9])(?=.*[!@#$%^&*\-_()[\]])[a-zA-Z0-9!@#$%^&*\-_()[\]]{8,30}$/;

  function toList(str) {
    if (Array.isArray(str)) {
      return str; // already a list
    }
    return str.trim().split(",").map(s => s.trim()).filter(s => s !== "");
  }

  function asList(listOrStr) {
    return Array.isArray(listOrStr) ? listOrStr : (listOrStr ? [listOrStr] : []);
  }

  function transposeUserRoles(userRoles) {
    var roleUsers = {};
    for (var u in userRoles) {
      var roleList = asList(userRoles[u]);
      for (var i in roleList) {
        var role = roleList[i];
        if (!roleUsers[role]) roleUsers[role] = []
        roleUsers[role].push(u);
      }
    }

    var roles = [];
    for (var r in roleUsers) {
      roles.push({"name":r, "users":Array.from(new Set(roleUsers[r]))});
    }
    return roles.sort((a, b) => (a.name > b.name) ? 1 : -1);
  }

  function roleMatch(roles, rolesForUser) {
    for (r in rolesForUser) {
      if (roles.includes(rolesForUser[r]))
        return true;
    }
    return false;
  }

  function permRow(perm, i) {
    var roles = asList(perm.role);
    var paths = asList(perm.path);

    var collectionNames = "";
    var collections = [];
    if ("collection" in perm) {
      if (perm["collection"] == null) {
        collectionNames = "null";
      } else {
        collections = asList(perm.collection);
        collectionNames = collections.sort().join(", ");
      }
    } else {
      // no collection property on the perm, so the default "*" applies
      collectionNames = "";
      collections.push("*");
    }

    var method = asList(perm.method);

    // perms don't always have an index ?!?
    var index = "index" in perm ? perm["index"] : ""+i;

    return { "index": index, "name": perm.name, "collectionNames": collectionNames, "collections": collections,
      "roles": roles, "paths": paths, "method": method, "params": perm.params };
  }

  function checkError(data) {
    var cause = null;
    if ("errorMessages" in data && Array.isArray(data["errorMessages"]) && data["errorMessages"].length > 0) {
      cause = "?";
      if ("errorMessages" in data["errorMessages"][0] && Array.isArray(data["errorMessages"][0]["errorMessages"]) && data["errorMessages"][0]["errorMessages"].length > 0) {
        cause = data["errorMessages"][0]["errorMessages"][0];
      }
    }
    return cause;
  }

  function truncateTo(str, maxLen, delim) {
    // allow for a little on either side of maxLen for better display
    var varLen = Math.min(Math.round(maxLen * 0.1), 15);
    if (str.length <= maxLen + varLen) {
      return str;
    }

    var total = str.split(delim).length;
    var at = str.indexOf(delim, maxLen - varLen);
    str = (at !== -1 && at < maxLen + varLen) ? str.substring(0, at) : str.substring(0, maxLen);
    var trimmed = str.split(delim).length;
    var diff = total - trimmed;
    str += " ... "+(diff > 1 ? "(+"+diff+" more)" : "");
    return str;
  }

  $scope.closeErrorDialog = function () {
    delete $scope.securityAPIError;
    delete $scope.securityAPIErrorDetails;
  };

  $scope.displayList = function(listOrStr) {
    if (!listOrStr) return "";
    var str = Array.isArray(listOrStr) ? listOrStr.sort().join(", ") : (""+listOrStr).trim();
    return truncateTo(str, 160, ", ");
  };

  $scope.displayParams = function(obj) {
    if (!obj) return "";
    if (Array.isArray(obj)) return obj.sort().join(", ");

    var display = "";
    for (const [key, value] of Object.entries(obj)) {
      if (display.length > 0) display += "; ";
      display += (key + "=" + (Array.isArray(value)?value.sort().join(","):value+""));
    }
    return truncateTo(display, 160, "; ");
  };

  $scope.displayRoles = function(obj) {
    return (!obj || (Array.isArray(obj) && obj.length === 0)) ? "null" : $scope.displayList(obj);
  };

  $scope.predefinedPermissions = ["collection-admin-edit", "collection-admin-read", "core-admin-read", "core-admin-edit", "zk-read",
    "read", "update", "all", "config-edit", "config-read", "schema-read", "schema-edit", "security-edit", "security-read",
    "metrics-read", "filestore-read", "filestore-write", "package-edit", "package-read"].sort();

  $scope.predefinedPermissionCollection = {"read":"*", "update":"*", "config-edit":"*", "config-read":"*", "schema-edit":"*", "schema-read":"*"};

  $scope.errorHandler = function (e) {
    var error = e.data && e.data.error ? e.data.error : null;
    if (error && error.msg) {
      $scope.securityAPIError = error.msg;
      $scope.securityAPIErrorDetails = e.data.errorDetails;
    } else if (e.data && e.data.message) {
      $scope.securityAPIError = e.data.message;
      $scope.securityAPIErrorDetails = JSON.stringify(e.data);
    }
  };

  $scope.showHelp = function (id) {
    if ($scope.helpId && ($scope.helpId === id || id === '')) {
      delete $scope.helpId;
    } else {
      $scope.helpId = id;
    }
  };

  $scope.wrapSchemeCmd = function(cmd, data) {
    var schemeCmd = {};
    schemeCmd[cmd] = $scope.multiAuthWithBasic ? { "basic": data } : data;
    return schemeCmd;
  };

  $scope.findEditableAuthz = function(data) {
    var authz = data.authorization;
    if (!authz) {
      return null;
    }
    
    var authzClass = authz["class"];
    if (authzClass.endsWith(".RuleBasedAuthorizationPlugin")) {
      return authz;
    }

    // go dig around in the schemes list looking for the editable one ...
    if (authzClass.endsWith(".MultiAuthRuleBasedAuthorizationPlugin")) {
      if ("schemes" in authz) {
        for (var i in authz.schemes) {
          if (authz.schemes[i]["class"].endsWith(".RuleBasedAuthorizationPlugin")) {
            return authz.schemes[i];
          }
        }
      }
    }

    return null;
  };

  $scope.validatePermConfig = function() {
    $scope.hasPermWarnings = false;
    $scope.permWarnings = [];

    var namedPerms = $scope.permissionsTable.filter(p => $scope.predefinedPermissions.includes(p.name));
    // look for named perms with -edit but w/o a -read
    for (var n in namedPerms) {
      var perm = namedPerms[n];
      if (perm.name.endsWith("-edit")) {
        // see if there is a "-read" too
        var pfx = perm.name.substring(0, perm.name.indexOf("-edit"));
        const readPermName = pfx + "-read";
        var readPerm = namedPerms.find(p => p.name === readPermName);
        if (!readPerm) {
          $scope.permWarnings.push(readPermName+" is not protected! In general, if you protect "+perm.name+", you should also protect "+readPermName);
        }
      }
    }

    var hasAll = namedPerms.find(p => p.name === "all");
    if (hasAll) {
      if ($scope.permissionsTable[$scope.permissionsTable.length-1].name !== "all") {
        $scope.permWarnings.push("The 'all' permission should always be the last permission in your config so that more specific permissions are applied first.");
      }
    } else {
      $scope.permWarnings.push("The 'all' permission is not configured! In general, you should assign the 'all' permission to an admin role and list it as the last permission in your config.");
    }

    $scope.hasPermWarnings = $scope.permWarnings.length > 0;
  };

  $scope.refresh = function () {
    $scope.hideAll();

    $scope.tls = false;
    $scope.blockUnknown = "false"; // default setting
    $scope.realmName = "solr";
    $scope.forwardCredentials = "false";
    $scope.multiAuthWithBasic = false;

    $scope.currentUser = sessionStorage.getItem("auth.username");

    $scope.userFilter = "";
    $scope.userFilterOption = "";
    $scope.userFilterText = "";
    $scope.userFilterOptions = [];

    $scope.permFilter = "";
    $scope.permFilterOption = "";
    $scope.permFilterOptions = [];
    $scope.permFilterTypes = ["", "name", "role", "path", "collection"];

    System.get(function(data) {
      $scope.tls = data.security ? data.security["tls"] : false;
      $scope.authenticationPlugin = data.security ? data.security["authenticationPlugin"] : null;
      $scope.authorizationPlugin = data.security ? data.security["authorizationPlugin"] : null;
      $scope.myRoles = data.security ? data.security["roles"] : [];
      $scope.isSecurityAdminEnabled = $scope.authenticationPlugin != null;
      $scope.isCloudMode = data.mode.match( /solrcloud/i ) != null;
      $scope.zkHost = $scope.isCloudMode ? data["zkHost"] : "";
      $scope.solrHome = data["solr_home"];
      $scope.refreshSecurityPanel();
    }, function(e) {
      if (e.status === 401 || e.status === 403) {
        $scope.isSecurityAdminEnabled = true;
        $scope.hasSecurityEditPerm = false;
        $scope.hideAll();
      }
    });
  };

  $scope.hideAll = function () {
    // add more dialogs here
    delete $scope.validationError;
    $scope.showUserDialog = false;
    $scope.showPermDialog = false;
    delete $scope.helpId;
  };

  $scope.getCurrentUserRoles = function() {
    return $scope.myRoles;
  };

  $scope.hasPermission = function(permissionName) {
    var matched = $scope.permissionsTable.filter(p => permissionName === p.name);
    if (matched.length === 0 && permissionName !== "all") {
      // this permission is not explicitly defined, but "all" will apply if it is defined
      matched = $scope.permissionsTable.filter(p => "all" === p.name);
    }
    return matched.length > 0 && roleMatch(matched.flatMap(p => p.roles), $scope.getCurrentUserRoles());
  };

  $scope.refreshSecurityPanel = function() {

    // determine if the authorization plugin supports CRUD permissions
    $scope.managePermissionsEnabled =
        ($scope.authorizationPlugin === "org.apache.solr.security.RuleBasedAuthorizationPlugin" ||
         $scope.authorizationPlugin === "org.apache.solr.security.ExternalRoleRuleBasedAuthorizationPlugin" ||
         $scope.authorizationPlugin === "org.apache.solr.security.MultiAuthRuleBasedAuthorizationPlugin");

    // don't allow CRUD on roles if using external
    $scope.manageUserRolesEnabled = false;

    Security.get({path: "authorization"}, function (data) {
      //console.log(">> authorization: "+JSON.stringify(data));
      
      if (!data.authorization) {
        $scope.isSecurityAdminEnabled = false;
        $scope.hasSecurityEditPerm = false;
        return;
      }

      var authz = $scope.findEditableAuthz(data);
      //console.log(">> authz: "+JSON.stringify(authz));
      
      if (authz) {
        $scope.manageUserRolesEnabled = true;
      }

      if ($scope.manageUserRolesEnabled) {
        $scope.userRoles = authz["user-role"];
        $scope.roles = transposeUserRoles($scope.userRoles);
        $scope.filteredRoles = $scope.roles;
        $scope.roleNames = $scope.roles.map(r => r.name).sort();
        $scope.roleNamesWithWildcard = ["*"].concat($scope.roleNames);
        if (!$scope.permFilterTypes.includes("user")) {
          $scope.permFilterTypes.push("user"); // can only filter perms by user if we have a role to user mapping
        }
      } else {
        $scope.userRoles = {};
        $scope.roles = [];
        $scope.filteredRoles = [];
        $scope.roleNames = [];
      }

      $scope.permissions = data.authorization["permissions"];
      $scope.permissionsTable = [];
      for (p in $scope.permissions) {
        $scope.permissionsTable.push(permRow($scope.permissions[p], parseInt(p)+1));
      }
      $scope.filteredPerms = $scope.permissionsTable;

      // check for issues with perm config
      $scope.validatePermConfig();

      // use the current user's roles (obtained from System.get) to check if they have the security permissions
      // Note: the backend will check too so this is only for display purposes
      $scope.hasSecurityEditPerm = $scope.hasPermission("security-edit");
      $scope.hasSecurityReadPerm = $scope.hasSecurityEditPerm || $scope.hasPermission("security-read");

      // authentication
      if ($scope.authenticationPlugin === "org.apache.solr.security.BasicAuthPlugin" || $scope.authenticationPlugin === "org.apache.solr.security.MultiAuthPlugin") {
        $scope.manageUsersEnabled = true;

        Security.get({path: "authentication"}, function (data) {
          // console.log(">> authentication: "+JSON.stringify(data));
          
          if (!data.authentication) {
            $scope.manageUsersEnabled = false;
            $scope.users = [];
            $scope.filteredUsers = $scope.users;
            return;
          }

          // find the "basic" scheme if using multi-auth
          var authn = data.authentication;
          if ("schemes" in data.authentication) {
            for (var a in data.authentication.schemes) {
              if (data.authentication.schemes[a]["scheme"] === "basic") {
                authn = data.authentication.schemes[a];
                $scope.multiAuthWithBasic = true;
                break;
              }
            }
          }

          //console.log(">> authn: "+JSON.stringify(authn));

          $scope.blockUnknown = authn["blockUnknown"] === true ? "true" : "false";
          $scope.forwardCredentials = authn["forwardCredentials"] === true ? "true" : "false";

          if ("realm" in authn) {
            $scope.realmName = authn["realm"];
          }

          var users = [];
          if (authn.credentials) {
            for (var u in authn.credentials) {
              var roles = $scope.userRoles[u];
              if (!roles) roles = [];
              users.push({"username":u, "roles":roles});
            }
          }

          $scope.users = users.sort((a, b) => (a.username > b.username) ? 1 : -1);
          $scope.filteredUsers = $scope.users.slice(0,100); // only display first 100
        }, $scope.errorHandler);
      } else {
        $scope.users = [];
        $scope.filteredUsers = $scope.users;
        $scope.manageUsersEnabled = false;
      }
    }, $scope.errorHandler);
  };

  $scope.validatePassword = function() {
    var password = $scope.upsertUser.password.trim();
    var password2 = $scope.upsertUser.password2 ? $scope.upsertUser.password2.trim() : "";
    if (password !== password2) {
      $scope.validationError = "Passwords do not match!";
      return false;
    }

    if (password.length < 15 && !password.match(strongPasswordRegex)) {
      $scope.validationError = "Password not strong enough! Must have length >= 15 or contain at least one lowercase letter, one uppercase letter, one digit, and one of these special characters: !@#$%^&*_-[]()";
      return false;
    }

    return true;
  };

  $scope.updateUserRoles = function() {
    var setUserRoles = {};
    var roles = [];
    if ($scope.upsertUser.selectedRoles) {
      roles = roles.concat($scope.upsertUser.selectedRoles);
    }
    if ($scope.upsertUser.newRole && $scope.upsertUser.newRole.trim() !== "") {
      var newRole = $scope.upsertUser.newRole.trim();
      if (newRole !== "null" && newRole !== "*" && newRole.length <= 30) {
        roles.push(newRole);
      } // else, no new role for you!
    }
    var userRoles = Array.from(new Set(roles));
    setUserRoles[$scope.upsertUser.username] = userRoles.length > 0 ? userRoles : null;
    var cmdJson = $scope.wrapSchemeCmd("set-user-role", setUserRoles);
    Security.post({path: "authorization"}, cmdJson, function (data) {
      $scope.toggleUserDialog();
      $scope.refreshSecurityPanel();
    });
  };

  $scope.doUpsertUser = function() {
    if (!$scope.upsertUser) {
      delete $scope.validationError;
      $scope.showUserDialog = false;
      return;
    }

    if (!$scope.upsertUser.username || $scope.upsertUser.username.trim() === "") {
      $scope.validationError = "Username is required!";
      return;
    }

    // keep username to a reasonable length? but allow for email addresses
    var username = $scope.upsertUser.username.trim();
    if (username.length > 30) {
      $scope.validationError = "Username must be 30 characters or less!";
      return;
    }

    var doSetUser = false;
    if ($scope.userDialogMode === 'add') {
      if ($scope.users) {
        var existing = $scope.users.find(u => u.username === username);
        if (existing) {
          $scope.validationError = "User '"+username+"' already exists!";
          return;
        }
      }

      if (!$scope.upsertUser.password) {
        $scope.validationError = "Password is required!";
        return;
      }

      if (!$scope.validatePassword()) {
        return;
      }
      doSetUser = true;
    } else {
      if ($scope.upsertUser.password) {
        if ($scope.validatePassword()) {
          doSetUser = true;
        } else {
          return; // update to password is invalid
        }
      } // else no update to password
    }

    if ($scope.upsertUser.newRole && $scope.upsertUser.newRole.trim() !== "") {
      var newRole = $scope.upsertUser.newRole.trim();
      if (newRole === "null" || newRole === "*" || newRole.length > 30) {
        $scope.validationError = "Invalid new role: "+newRole;
        return;
      }
    }

    delete $scope.validationError;

    if (doSetUser) {
      var setUserJson = {};
      setUserJson[username] = $scope.upsertUser.password.trim();
      var cmdJson = $scope.wrapSchemeCmd("set-user", setUserJson);
      Security.post({path: "authentication"}, cmdJson, function (data) {
        var errorCause = checkError(data);
        if (errorCause != null) {
          $scope.securityAPIError = "create user "+username+" failed due to: "+errorCause;
          $scope.securityAPIErrorDetails = JSON.stringify(data);
          return;
        }
        // TODO: shouldn't need this extra GET, but sometimes the config back from the server doesn't have our new user
        // and doing this seems to avoid what looks like a race?
        Security.get({path: "authentication"}, function (data2) {
          $scope.updateUserRoles();
        });
      });
    } else {
      $scope.updateUserRoles();
    }
  };

  $scope.confirmDeleteUser = function() {
    if (window.confirm("Confirm delete the '"+$scope.upsertUser.username+"' user?")) {
      // remove all roles for the user and the delete the user
      var removeRoles = {};
      removeRoles[$scope.upsertUser.username] = null;
      var cmdJson = $scope.wrapSchemeCmd("set-user-role", removeRoles);
      Security.post({path: "authorization"}, cmdJson, function (data) {
        var deleteUserCmd = $scope.wrapSchemeCmd("delete-user", [$scope.upsertUser.username]);
        Security.post({path: "authentication"}, deleteUserCmd, function (data2) {
          $scope.toggleUserDialog();
          $scope.refreshSecurityPanel();
        });
      });
    }
  };

  $scope.showAddUserDialog = function() {
    $scope.userDialogMode = "add";
    $scope.userDialogHeader = "Add New User";
    $scope.userDialogAction = "Add User";
    $scope.upsertUser = {};
    $scope.toggleUserDialog();
  };

  $scope.toggleUserDialog = function() {
    if ($scope.showUserDialog) {
      delete $scope.upsertUser;
      delete $scope.validationError;
      $scope.showUserDialog = false;
      return;
    }

    $scope.hideAll();
    $('#user-dialog').css({left: 132, top: 132});
    $scope.showUserDialog = true;
  };

  $scope.onPredefinedChanged = function() {
    if (!$scope.upsertPerm) {
      return;
    }

    if ($scope.upsertPerm.name && $scope.upsertPerm.name.trim() !== "") {
      delete $scope.selectedPredefinedPermission;
    } else {
      $scope.upsertPerm.name = "";
    }

    if ($scope.selectedPredefinedPermission && $scope.selectedPredefinedPermission in $scope.predefinedPermissionCollection) {
      $scope.upsertPerm.collection = $scope.predefinedPermissionCollection[$scope.selectedPredefinedPermission];
    }

    $scope.isPermFieldDisabled = ($scope.upsertPerm.name === "" && $scope.selectedPredefinedPermission);
  };

  $scope.showAddPermDialog = function() {
    $scope.permDialogMode = "add";
    $scope.permDialogHeader = "Add New Permission";
    $scope.permDialogAction = "Add Permission";
    $scope.upsertPerm = {};
    $scope.upsertPerm.name = "";
    $scope.upsertPerm.index = "";
    $scope.upsertPerm["method"] = {"get":"true", "post":"true", "put":"true", "delete":"true"};
    $scope.isPermFieldDisabled = false;
    delete $scope.selectedPredefinedPermission;

    $scope.params = [{"name":"", "value":""}];

    var permissionNames = $scope.permissions.map(p => p.name);
    $scope.filteredPredefinedPermissions = $scope.predefinedPermissions.filter(p => !permissionNames.includes(p));

    $scope.togglePermDialog();
  };

  $scope.togglePermDialog = function() {
    if ($scope.showPermDialog) {
      delete $scope.upsertPerm;
      delete $scope.validationError;
      $scope.showPermDialog = false;
      $scope.isPermFieldDisabled = false;
      delete $scope.selectedPredefinedPermission;
      return;
    }

    $scope.hideAll();

    var leftPos = $scope.permDialogMode === "add" ? 500 : 100;
    var topPos = $('#permissions').offset().top - 320;
    if (topPos < 0) topPos = 0;
    $('#add-permission-dialog').css({left: leftPos, top: topPos});

    $scope.showPermDialog = true;
  };

  $scope.getMethods = function() {
    var methods = [];
    if ($scope.upsertPerm.method.get === "true") {
      methods.push("GET");
    }
    if ($scope.upsertPerm.method.put === "true") {
      methods.push("PUT");
    }
    if ($scope.upsertPerm.method.post === "true") {
      methods.push("POST");
    }
    if ($scope.upsertPerm.method.delete === "true") {
      methods.push("DELETE");
    }
    return methods;
  };

  $scope.confirmDeletePerm = function() {
    var permName = $scope.selectedPredefinedPermission ? $scope.selectedPredefinedPermission : $scope.upsertPerm.name.trim();
    if (window.confirm("Confirm delete the '"+permName+"' permission?")) {
      var index = parseInt($scope.upsertPerm.index);
      Security.post({path: "authorization"}, { "delete-permission": index }, function (data) {
        $scope.togglePermDialog();
        $scope.refreshSecurityPanel();
      });
    }
  };

  $scope.doUpsertPermission = function() {
    if (!$scope.upsertPerm) {
      $scope.upsertPerm = {};
    }

    var isAdd = $scope.permDialogMode === "add";
    var name = $scope.selectedPredefinedPermission ? $scope.selectedPredefinedPermission : $scope.upsertPerm.name.trim();

    if (isAdd) {
      if (!name) {
        $scope.validationError = "Either select a predefined permission or provide a name for a custom permission";
        return;
      }
      var permissionNames = $scope.permissions.map(p => p.name);
      if (permissionNames.includes(name)) {
        $scope.validationError = "Permission '"+name+"' already exists!";
        return;
      }

      if (name === "*") {
        $scope.validationError = "Invalid permission name!";
        return;
      }
    }

    var role = null;
    if ($scope.manageUserRolesEnabled) {
      role = $scope.upsertPerm.selectedRoles;
      if (!role || role.length === 0) {
        role = null;
      } else if (role.includes("*")) {
        role = ["*"];
      }
    } else if ($scope.upsertPerm.manualRoles && $scope.upsertPerm.manualRoles.trim() !== "") {
      var manualRoles = $scope.upsertPerm.manualRoles.trim();
      role = (manualRoles === "null") ? null : toList(manualRoles);
    }

    var setPermJson = {"name": name, "role": role };

    if ($scope.selectedPredefinedPermission) {
      $scope.params = [{"name":"","value":""}];
    } else {
      // collection
      var coll = null;
      if ($scope.upsertPerm.collection != null && $scope.upsertPerm.collection !== "null") {
        if ($scope.upsertPerm.collection === "*") {
          coll = "*";
        } else {
          coll = $scope.upsertPerm.collection && $scope.upsertPerm.collection.trim() !== "" ? toList($scope.upsertPerm.collection) : "";
        }
      }
      setPermJson["collection"] = coll;

      // path
      if (!$scope.upsertPerm.path || (Array.isArray($scope.upsertPerm.path) && $scope.upsertPerm.path.length === 0)) {
        $scope.validationError = "Path is required for custom permissions!";
        return;
      }

      setPermJson["path"] = toList($scope.upsertPerm.path);

      if ($scope.upsertPerm.method) {
        var methods = $scope.getMethods();
        if (methods.length === 0) {
          $scope.validationError = "Must specify at least one request method for a custom permission!";
          return;
        }

        if (methods.length < 4) {
          setPermJson["method"] = methods;
        } // else no need to specify, rule applies to all methods
      }

      // params
      var params = {};
      if ($scope.params && $scope.params.length > 0) {
        for (i in $scope.params) {
          var p = $scope.params[i];
          var name = p.name.trim();
          if (name !== "" && p.value) {
            if (name in params) {
              params[name].push(p.value);
            } else {
              params[name] = [p.value];
            }
          }
        }
      }
      setPermJson["params"] = params;
    }

    var indexUpdated = false;
    if ($scope.upsertPerm.index) {
      var indexOrBefore = isAdd ? "before" : "index";
      var indexInt = parseInt($scope.upsertPerm.index);
      if (indexInt < 1) indexInt = 1;
      if (indexInt >= $scope.permissions.length) indexInt = null;
      if (indexInt != null) {
        setPermJson[indexOrBefore] = indexInt;
      }
      indexUpdated = (!isAdd && indexInt !== parseInt($scope.upsertPerm.originalIndex));
    }

    if (indexUpdated) {
      // changing position is a delete + re-add in new position
      Security.post({path: "authorization"}, { "delete-permission": parseInt($scope.upsertPerm.originalIndex) }, function (remData) {
        if (setPermJson.index) {
          var before = setPermJson.index;
          delete setPermJson.index;
          setPermJson["before"] = before;
        }

        // add perm back in new position
        Security.post({path: "authorization"}, { "set-permission": setPermJson }, function (data) {
          var errorCause = checkError(data);
          if (errorCause != null) {
            $scope.securityAPIError = "set-permission "+name+" failed due to: "+errorCause;
            $scope.securityAPIErrorDetails = JSON.stringify(data);
            return;
          }
          $scope.togglePermDialog();
          // avoids a weird race with not getting the latest config after an update
          Security.get({path: "authorization"}, function (ignore) {
            $scope.refreshSecurityPanel();
          });
        });
      });
    } else {
      var action = isAdd ? "set-permission" : "update-permission";
      var postBody = {};
      postBody[action] = setPermJson;

      // if they have the "all" permission in the last position, then keep it there when adding a new permission
      if (!setPermJson["before"] && !setPermJson["index"] && $scope.permissionsTable && $scope.permissionsTable.length > 0 && $scope.permissionsTable[$scope.permissionsTable.length-1].name === "all") {
        setPermJson["before"] = $scope.permissionsTable.length;
      }

      Security.post({path: "authorization"}, postBody, function (data) {
        var errorCause = checkError(data);
        if (errorCause != null) {
          $scope.securityAPIError = action+" "+name+" failed due to: "+errorCause;
          $scope.securityAPIErrorDetails = JSON.stringify(data);
          return;
        }

        $scope.togglePermDialog();
        // avoids a weird race with not getting the latest config after an update
        Security.get({path: "authorization"}, function (ignore) {
          $scope.refreshSecurityPanel();
        });
      });
    }
  };

  $scope.applyUserFilter = function() {
    $scope.userFilterText = "";
    $scope.userFilterOption = "";
    $scope.userFilterOptions = [];
    $scope.filteredUsers = $scope.users; // reset the filtered when the filter type changes

    if ($scope.userFilter === "name" || $scope.userFilter === "path") {
      // no-op: filter is text input
    } else if ($scope.userFilter === "role") {
      $scope.userFilterOptions = $scope.roleNames;
    } else if ($scope.userFilter === "perm") {
      $scope.userFilterOptions = $scope.permissions.map(p => p.name).sort();
    } else {
      $scope.userFilter = "";
    }
  };

  $scope.onUserFilterTextChanged = function() {
    // don't fire until we have at least 2 chars ...
    if ($scope.userFilterText && $scope.userFilterText.trim().length >= 2) {
      $scope.userFilterOption = $scope.userFilterText.toLowerCase();
      $scope.onUserFilterOptionChanged();
    } else {
      $scope.filteredUsers = $scope.users;
    }
  };

  function pathMatch(paths, filter) {
    for (p in paths) {
      if (paths[p].includes(filter)) {
        return true;
      }
    }
    return false;
  }

  $scope.onUserFilterOptionChanged = function() {
    var filter = $scope.userFilterOption ? $scope.userFilterOption.trim() : "";
    if (filter.length === 0) {
      $scope.filteredUsers = $scope.users;
      return;
    }

    if ($scope.userFilter === "name") {
      $scope.filteredUsers = $scope.users.filter(u => u.username.toLowerCase().includes(filter));
    } else if ($scope.userFilter === "role") {
      $scope.filteredUsers = $scope.users.filter(u => u.roles.includes(filter));
    } else if ($scope.userFilter === "path") {
      var rolesForPath = Array.from(new Set($scope.permissionsTable.filter(p => p.roles && pathMatch(p.paths, filter)).flatMap(p => p.roles)));
      var usersForPath = Array.from(new Set($scope.roles.filter(r => r.users && r.users.length > 0 && rolesForPath.includes(r.name)).flatMap(r => r.users)));
      $scope.filteredUsers = $scope.users.filter(u => usersForPath.includes(u.username));
    } else if ($scope.userFilter === "perm") {
      var rolesForPerm = Array.from(new Set($scope.permissionsTable.filter(p => p.name === filter).flatMap(p => p.roles)));
      var usersForPerm = Array.from(new Set($scope.roles.filter(r => r.users && r.users.length > 0 && rolesForPerm.includes(r.name)).flatMap(r => r.users)));
      $scope.filteredUsers = $scope.users.filter(u => usersForPerm.includes(u.username));
    } else {
      // reset
      $scope.userFilter = "";
      $scope.userFilterOption = "";
      $scope.userFilterText = "";
      $scope.filteredUsers = $scope.users;
    }
  };

  $scope.applyPermFilter = function() {
    $scope.permFilterText = "";
    $scope.permFilterOption = "";
    $scope.permFilterOptions = [];
    $scope.filteredPerms = $scope.permissionsTable;

    if ($scope.permFilter === "name" || $scope.permFilter === "path") {
      // no-op: filter is text input
    } else if ($scope.permFilter === "role") {
      var roles = $scope.manageUserRolesEnabled ? $scope.roleNames : Array.from(new Set($scope.permissionsTable.flatMap(p => p.roles))).sort();
      $scope.permFilterOptions = ["*", "null"].concat(roles);
    } else if ($scope.permFilter === "user") {
      $scope.permFilterOptions = Array.from(new Set($scope.roles.flatMap(r => r.users))).sort();
    } else if ($scope.permFilter === "collection") {
      $scope.permFilterOptions = Array.from(new Set($scope.permissionsTable.flatMap(p => p.collections))).sort();
      $scope.permFilterOptions.push("null");
    } else {
      // no perm filtering
      $scope.permFilter = "";
    }
  };

  $scope.onPermFilterTextChanged = function() {
    // don't fire until we have at least 2 chars ...
    if ($scope.permFilterText && $scope.permFilterText.trim().length >= 2) {
      $scope.permFilterOption = $scope.permFilterText.trim().toLowerCase();
      $scope.onPermFilterOptionChanged();
    } else {
      $scope.filteredPerms = $scope.permissionsTable;
    }
  };

  $scope.onPermFilterOptionChanged = function() {
    var filterCriteria = $scope.permFilterOption ? $scope.permFilterOption.trim() : "";
    if (filterCriteria.length === 0) {
      $scope.filteredPerms = $scope.permissionsTable;
      return;
    }

    if ($scope.permFilter === "name") {
      $scope.filteredPerms = $scope.permissionsTable.filter(p => p.name.toLowerCase().includes(filterCriteria));
    } else if ($scope.permFilter === "role") {
      if (filterCriteria === "null") {
        $scope.filteredPerms = $scope.permissionsTable.filter(p => p.roles.length === 0);
      } else {
        $scope.filteredPerms = $scope.permissionsTable.filter(p => p.roles.includes(filterCriteria));
      }
    } else if ($scope.permFilter === "path") {
      $scope.filteredPerms = $scope.permissionsTable.filter(p => pathMatch(p.paths, filterCriteria));
    } else if ($scope.permFilter === "user") {
      // get the user's roles and then find all the permissions mapped to each role
      var rolesForUser = $scope.roles.filter(r => r.users.includes(filterCriteria)).map(r => r.name);
      $scope.filteredPerms = $scope.permissionsTable.filter(p => p.roles.length > 0 && roleMatch(p.roles, rolesForUser));
    } else if ($scope.permFilter === "collection") {
      function collectionMatch(collNames, colls, filter) {
        return (filter === "null") ?collNames === "null" : colls.includes(filter);
      }
      $scope.filteredPerms = $scope.permissionsTable.filter(p => collectionMatch(p.collectionNames, p.collections, filterCriteria));
    } else {
      // reset
      $scope.permFilter = "";
      $scope.permFilterOption = "";
      $scope.permFilterText = "";
      $scope.filteredPerms = $scope.permissionsTable;
    }
  };

  $scope.editUser = function(row) {
    if (!row || !$scope.hasSecurityEditPerm) {
      return;
    }

    var userId = row.username;
    $scope.userDialogMode = "edit";
    $scope.userDialogHeader = "Edit User: "+userId;
    $scope.userDialogAction = "Update";
    var userRoles = userId in $scope.userRoles ? $scope.userRoles[userId] : [];
    if (!Array.isArray(userRoles)) {
      userRoles = [userRoles];
    }

    $scope.upsertUser = { username: userId, selectedRoles: userRoles };
    $scope.toggleUserDialog();
  };

  function buildMethods(m) {
    return {"get":""+m.includes("GET"), "post":""+m.includes("POST"), "put":""+m.includes("PUT"), "delete":""+m.includes("DELETE")};
  }

  $scope.editPerm = function(row) {
    if (!$scope.managePermissionsEnabled || !$scope.hasSecurityEditPerm || !row) {
      return;
    }

    var name = row.name;
    $scope.permDialogMode = "edit";
    $scope.permDialogHeader = "Edit Permission: "+name;
    $scope.permDialogAction = "Update";

    var perm = $scope.permissionsTable.find(p => p.name === name);
    var isPredefined = $scope.predefinedPermissions.includes(name);
    if (isPredefined) {
      $scope.selectedPredefinedPermission = name;
      $scope.upsertPerm = { };
      $scope.filteredPredefinedPermissions = [];
      $scope.filteredPredefinedPermissions.push(name);
      if ($scope.selectedPredefinedPermission && $scope.selectedPredefinedPermission in $scope.predefinedPermissionCollection) {
        $scope.upsertPerm.collection = $scope.predefinedPermissionCollection[$scope.selectedPredefinedPermission];
      }
      $scope.isPermFieldDisabled = true;
    } else {
      $scope.upsertPerm = { name: name, collection: perm.collectionNames, path: perm.paths };
      $scope.params = [];
      if (perm.params) {
        for (const [key, value] of Object.entries(perm.params)) {
          if (Array.isArray(value)) {
            for (i in value) {
              $scope.params.push({"name":key, "value":value[i]});
            }
          } else {
            $scope.params.push({"name":key, "value":value});
          }
        }
      }
      if ($scope.params.length === 0) {
        $scope.params = [{"name":"","value":""}];
      }

      $scope.upsertPerm["method"] = perm.method.length === 0 ? {"get":"true", "post":"true", "put":"true", "delete":"true"} : buildMethods(perm.method);
      $scope.isPermFieldDisabled = false;
      delete $scope.selectedPredefinedPermission;
    }

    $scope.upsertPerm.index = perm["index"];
    $scope.upsertPerm.originalIndex = perm["index"];

    // roles depending on authz plugin support
    if ($scope.manageUserRolesEnabled) {
      $scope.upsertPerm["selectedRoles"] = asList(perm.roles);
    } else {
      $scope.upsertPerm["manualRoles"] = asList(perm.roles).sort().join(", ");
    }

    $scope.togglePermDialog();
  };

  $scope.applyRoleFilter = function() {
    $scope.roleFilterText = "";
    $scope.roleFilterOption = "";
    $scope.roleFilterOptions = [];
    $scope.filteredRoles = $scope.roles; // reset the filtered when the filter type changes

    if ($scope.roleFilter === "name" || $scope.roleFilter === "path") {
      // no-op: filter is text input
    } else if ($scope.roleFilter === "user") {
      $scope.roleFilterOptions = Array.from(new Set($scope.roles.flatMap(r => r.users))).sort();
    } else if ($scope.roleFilter === "perm") {
      $scope.roleFilterOptions = $scope.permissions.map(p => p.name).sort();
    } else {
      $scope.roleFilter = "";
    }
  };

  $scope.onRoleFilterTextChanged = function() {
    // don't fire until we have at least 2 chars ...
    if ($scope.roleFilterText && $scope.roleFilterText.trim().length >= 2) {
      $scope.roleFilterOption = $scope.roleFilterText.toLowerCase();
      $scope.onRoleFilterOptionChanged();
    } else {
      $scope.filteredRoles = $scope.roles;
    }
  };

  $scope.onRoleFilterOptionChanged = function() {
    var filter = $scope.roleFilterOption ? $scope.roleFilterOption.trim() : "";
    if (filter.length === 0) {
      $scope.filteredRoles = $scope.roles;
      return;
    }

    if ($scope.roleFilter === "name") {
      $scope.filteredRoles = $scope.roles.filter(r => r.name.toLowerCase().includes(filter));
    } else if ($scope.roleFilter === "user") {
      $scope.filteredRoles = $scope.roles.filter(r => r.users.includes(filter));
    } else if ($scope.roleFilter === "path") {
      var rolesForPath = Array.from(new Set($scope.permissionsTable.filter(p => p.roles && pathMatch(p.paths, filter)).flatMap(p => p.roles)));
      $scope.filteredRoles = $scope.roles.filter(r => rolesForPath.includes(r.name));
    } else if ($scope.roleFilter === "perm") {
      var rolesForPerm = Array.from(new Set($scope.permissionsTable.filter(p => p.name === filter).flatMap(p => p.roles)));
      $scope.filteredRoles = $scope.roles.filter(r => rolesForPerm.includes(r.name));
    } else {
      // reset
      $scope.roleFilter = "";
      $scope.roleFilterOption = "";
      $scope.roleFilterText = "";
      $scope.filteredRoles = $scope.roles;
    }
  };

  $scope.showAddRoleDialog = function() {
    $scope.roleDialogMode = "add";
    $scope.roleDialogHeader = "Add New Role";
    $scope.roleDialogAction = "Add Role";
    $scope.upsertRole = {};
    $scope.userNames = $scope.users.map(u => u.username);
    $scope.grantPermissionNames = Array.from(new Set($scope.predefinedPermissions.concat($scope.permissions.map(p => p.name)))).sort();
    $scope.toggleRoleDialog();
  };

  $scope.toggleRoleDialog = function() {
    if ($scope.showRoleDialog) {
      delete $scope.upsertRole;
      delete $scope.validationError;
      delete $scope.userNames;
      $scope.showRoleDialog = false;
      return;
    }
    $scope.hideAll();
    $('#role-dialog').css({left: 680, top: 139});
    $scope.showRoleDialog = true;
  };

  $scope.doUpsertRole = function() {
    if (!$scope.upsertRole) {
      delete $scope.validationError;
      $scope.showRoleDialog = false;
      return;
    }

    if (!$scope.upsertRole.name || $scope.upsertRole.name.trim() === "") {
      $scope.validationError = "Role name is required!";
      return;
    }

    // keep role name to a reasonable length? but allow for email addresses
    var name = $scope.upsertRole.name.trim();
    if (name.length > 30) {
      $scope.validationError = "Role name must be 30 characters or less!";
      return;
    }

    if (name === "null" || name === "*") {
      $scope.validationError = "Role name '"+name+"' is invalid!";
      return;
    }

    if ($scope.roleDialogMode === "add") {
      if ($scope.roleNames.includes(name)) {
        $scope.validationError = "Role '"+name+"' already exists!";
        return;
      }
    }

    var usersForRole = [];
    if ($scope.upsertRole.selectedUsers && $scope.upsertRole.selectedUsers.length > 0) {
      usersForRole = usersForRole.concat($scope.upsertRole.selectedUsers);
    }
    usersForRole = Array.from(new Set(usersForRole));
    if (usersForRole.length === 0) {
      $scope.validationError = "Must assign new role '"+name+"' to at least one user.";
      return;
    }

    var perms = [];
    if ($scope.upsertRole.grantedPerms && Array.isArray($scope.upsertRole.grantedPerms) && $scope.upsertRole.grantedPerms.length > 0) {
      perms = $scope.upsertRole.grantedPerms;
    }

    // go get the latest role mappings ...
    Security.get({path: "authorization"}, function (data) {
      var authz = $scope.findEditableAuthz(data);
      if (!authz) {
        $scope.validationError = "User roles not editable via the UI!";
        return;
      }

      var userRoles = authz["user-role"];
      var setUserRoles = {};
      for (u in usersForRole) {
        var user = usersForRole[u];
        var currentRoles = user in userRoles ? asList(userRoles[user]) : [];
        // add the new role for this user if needed
        if (!currentRoles.includes(name)) {
          currentRoles.push(name);
        }
        setUserRoles[user] = currentRoles;
      }

      var cmdJson = $scope.wrapSchemeCmd("set-user-role", setUserRoles);
      Security.post({path: "authorization"}, cmdJson, function (data2) {

        var errorCause = checkError(data2);
        if (errorCause != null) {
          $scope.securityAPIError = "set-user-role for "+username+" failed due to: "+errorCause;
          $scope.securityAPIErrorDetails = JSON.stringify(data2);
          return;
        }

        if (perms.length === 0) {
          // close dialog and refresh the tables ...
          $scope.toggleRoleDialog();
          $scope.refreshSecurityPanel();
          return;
        }

        var currentPerms = data.authorization["permissions"];
        for (i in perms) {
          var permName = perms[i];
          var existingPerm = currentPerms.find(p => p.name === permName);

          if (existingPerm) {
            var roleList = [];
            if (existingPerm.role) {
              if (Array.isArray(existingPerm.role)) {
                roleList = existingPerm.role;
              } else {
                roleList.push(existingPerm.role);
              }
            }
            if (!roleList.includes(name)) {
              roleList.push(name);
            }
            existingPerm.role = roleList;
            Security.post({path: "authorization"}, { "update-permission": existingPerm }, function (data3) {
              $scope.refreshSecurityPanel();
            });
          } else {
            // new perm ... must be a predefined ...
            if ($scope.predefinedPermissions.includes(permName)) {
              var setPermission = {name: permName, role:[name]};
              Security.post({path: "authorization"}, { "set-permission": setPermission }, function (data3) {
                $scope.refreshSecurityPanel();
              });
            } // else ignore it
          }
        }
        $scope.toggleRoleDialog();
      });
    });

  };

  $scope.editRole = function(row) {
    if (!row || !$scope.hasSecurityEditPerm) {
      return;
    }

    var roleName = row.name;
    $scope.roleDialogMode = "edit";
    $scope.roleDialogHeader = "Edit Role: "+roleName;
    $scope.roleDialogAction = "Update";
    var role = $scope.roles.find(r => r.name === roleName);
    var perms = $scope.permissionsTable.filter(p => p.roles.includes(roleName)).map(p => p.name);
    $scope.upsertRole = { name: roleName, selectedUsers: role.users, grantedPerms: perms };
    $scope.userNames = $scope.users.map(u => u.username);
    $scope.grantPermissionNames = Array.from(new Set($scope.predefinedPermissions.concat($scope.permissions.map(p => p.name)))).sort();
    $scope.toggleRoleDialog();
  };

  $scope.onBlockUnknownChange = function() {
    var cmdJson = $scope.wrapSchemeCmd("set-property", { "blockUnknown": $scope.blockUnknown === "true" });
    Security.post({path: "authentication"}, cmdJson, function (data) {
      $scope.refreshSecurityPanel();
    });
  };

  $scope.onForwardCredsChange = function() {
    var cmdJson = $scope.wrapSchemeCmd("set-property", { "forwardCredentials": $scope.forwardCredentials === "true" });
    Security.post({path: "authentication"}, cmdJson, function (data) {
      $scope.refreshSecurityPanel();
    });
  };

  $scope.removeParam= function(index) {
    if ($scope.params.length === 1) {
      $scope.params = [{"name":"","value":""}];
    } else {
      $scope.params.splice(index, 1);
    }
  };
  
  $scope.addParam = function(index) {
    $scope.params.splice(index+1, 0, {"name":"","value":""});
  };

  $scope.refresh();
})
