export default mnXDCRDeleteDialogController;

function mnXDCRDeleteDialogController($uibModalInstance, mnPromiseHelper, mnXDCRService, id) {
  var vm = this;

  vm.deleteReplication = deleteReplication;

  function deleteReplication() {
    var promise = mnXDCRService.deleteReplication(id);
    mnPromiseHelper(vm, promise, $uibModalInstance)
      .showGlobalSpinner()
      .closeFinally()
      .broadcast("reloadTasksPoller")
      .showGlobalSuccess("Replication deleted successfully!");
  }
}
