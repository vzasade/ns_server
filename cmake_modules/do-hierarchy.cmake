EXECUTE_PROCESS(RESULT_VARIABLE _failure
  COMMAND "${ERL_EXECUTABLE}"
  -pa ./ebin ./deps/ns_couchdb/ebin
  -noshell -kernel error_logger silent -shutdown_time 10000
  -eval "case ns_server:doc(\"${CMAKE_CURRENT_SOURCE_DIR}/doc/ns-server-hierarchy.txt\") of ok -> init:stop(); _ -> init:stop(1) end.")
IF (_failure)
  MESSAGE (FATAL_ERROR "failed running doc generator")
ENDIF (_failure)
