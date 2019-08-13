(function () {
  "use strict";

  angular
    .module('mnServers', [
      'mnPoolDefault',
      'ui.router',
      'mnAutocompleteOff',
      'ui.bootstrap',
      'mnServersService',
      'mnHelper',
      'mnBarUsage',
      'mnServersListItemDetailsService',
      'mnFilters',
      'mnSortableTable',
      'mnServices',
      'mnSpinner',
      'ngMessages',
      'mnMemoryQuotaService',
      'mnGsiService',
      'mnPromiseHelper',
      'mnGroupsService',
      'mnStorageMode',
      'mnBucketsService',
      'mnPoll',
      'mnFocus',
      'mnPools',
      'mnSettingsAutoFailoverService',
      'mnTasksDetails',
      'mnWarmupProgress',
      'mnElementCrane',
      'mnSearch',
      'mnSelectableNodesList',
      'mnRootCertificateService'
    ])
    .controller('mnServersController', mnServersController)
    .filter("formatFailoverWarnings", formatFailoverWarnings);

  function formatFailoverWarnings() {
    return function (warning) {
      switch (warning) {
      case 'rebalanceNeeded': return 'Rebalance required, some data is not currently replicated.';
      case 'hardNodesNeeded': return 'At least two servers with the data service are required to provide replication.';
      case 'softNodesNeeded': return 'Additional active servers required to provide the desired number of replicas.';
      case 'softRebalanceNeeded': return 'Rebalance recommended, some data does not have the desired replicas configuration.';
      }
    };
  }

  function mnServersController($scope, $state, $uibModal, mnPoolDefault, mnPoller, mnServersService, mnHelper, mnGroupsService, mnPromiseHelper, mnPools, mnSettingsAutoFailoverService, mnTasksDetails, permissions, mnFormatServicesFilter, $filter, mnBucketsService) {
    var vm = this;
    vm.mnPoolDefault = mnPoolDefault.latestValue();

    vm.postStopRebalance = postStopRebalance;
    vm.onStopRecovery = onStopRecovery;
    vm.postRebalance = postRebalance;
    vm.addServer = addServer;
    vm.filterField = "";
    vm.sortByGroup = sortByGroup;
    vm.multipleFailoverDialog = multipleFailoverDialog;

    function sortByGroup(node) {
      return vm.getGroupsByHostname[node.hostname] && vm.getGroupsByHostname[node.hostname].name;
    }

    activate();

    function activate() {
      mnHelper.initializeDetailsHashObserver(vm, 'openedServers', 'app.admin.servers.list');

      if (permissions.cluster.server_groups.read) {
        new mnPoller($scope, function () {
          return mnGroupsService.getGroupsByHostname();
        })
          .subscribe("getGroupsByHostname", vm)
          .reloadOnScopeEvent(["serverGroupsUriChanged", "reloadServersPoller"])
          .cycle();
      }

      new mnPoller($scope, function () {
        return mnBucketsService.findMoxiBucket();
      })
        .subscribe("moxiBucket", vm)
        .reloadOnScopeEvent(["reloadBucketStats"])
        .cycle();

      new mnPoller($scope, function () {
        return mnServersService.getNodes();
      })
        .subscribe(function (nodes) {
          vm.showSpinner = false;
          vm.nodes = nodes;
        })
        .reloadOnScopeEvent(["mnPoolDefaultChanged", "reloadNodes"])
        .cycle();


      if (permissions.cluster.settings.read) {
        new mnPoller($scope, function () {
          return mnSettingsAutoFailoverService.getAutoFailoverSettings();
        })
          .setInterval(10000)
          .subscribe("autoFailoverSettings", vm)
          .reloadOnScopeEvent(["reloadServersPoller", "rebalanceFinished"])
          .cycle();
      }

      // $scope.$on("reloadServersPoller", function () {
      //   vm.showSpinner = true;
      // });
    }
    function multipleFailoverDialog() {
      $uibModal.open({
        templateUrl: 'app/mn_admin/mn_servers/mn_multiple_failover_dialog.html',
        controller: 'mnMultipleFailoverDialogController as multipleFailoverDialogCtl',
        resolve: {
          groups: function () {
            return mnPoolDefault.get().then(function (poolDefault) {
              if (poolDefault.isGroupsAvailable && permissions.cluster.server_groups.read) {
                return mnGroupsService.getGroupsByHostname();
              }
            });
          },
          nodes: function () {
            return vm.nodes.reallyActive;
          }
        }
      });
    }
    function addServer() {
      $uibModal.open({
        templateUrl: 'app/mn_admin/mn_servers/add_dialog/mn_servers_add_dialog.html',
        controller: 'mnServersAddDialogController as serversAddDialogCtl',
        resolve: {
          groups: function () {
            return mnPoolDefault.get().then(function (poolDefault) {
              if (poolDefault.isGroupsAvailable) {
                return mnGroupsService.getGroups();
              }
            });
          }
        }
      });
    }
    function postRebalance() {
      mnPromiseHelper(vm, mnServersService.postRebalance(vm.nodes.allNodes))
        .onSuccess(function () {
          $state.go('app.admin.servers.list', {list: 'active'});
        })
        .broadcast("reloadServersPoller")
        .catchGlobalErrors()
        .showErrorsSensitiveSpinner();
    }
    function onStopRecovery() {
      mnPromiseHelper(vm, mnServersService.stopRecovery($scope.adminCtl.tasks.tasksRecovery.stopURI))
        .broadcast("reloadServersPoller")
        .showErrorsSensitiveSpinner();
    }
    function postStopRebalance() {
      return mnPromiseHelper(vm, mnServersService.stopRebalanceWithConfirm())
        .broadcast("reloadServersPoller");
    }
  }
})();
