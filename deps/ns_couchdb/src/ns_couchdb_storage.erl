%% @author Couchbase <info@couchbase.com>
%% @copyright 2014 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% @doc module that contains storage related functions to be executed on ns_couchdb node
%%

-module(ns_couchdb_storage).

-include("ns_common.hrl").

-export([delete_databases_and_files/1,
         delete_couch_database/1]).

delete_databases_and_files(Bucket) ->
    AllDBs = bucket_databases(Bucket),
    MasterDB = iolist_to_binary([Bucket, <<"/master">>]),
    {MaybeMasterDb, RestDBs} = lists:partition(
                            fun (Name) ->
                                    Name =:= MasterDB
                            end, AllDBs),
    RV = case delete_databases_loop(MaybeMasterDb ++ RestDBs) of
             ok ->
                 ?log_info("Couch dbs are deleted. Proceeding with bucket directory"),
                 {ok, DbDir} = ns_storage_conf:this_node_dbdir(),
                 Path = filename:join(DbDir, Bucket),
                 case misc:rm_rf(Path) of
                     ok -> ok;
                     Error ->
                         ale:error(?USER_LOGGER, "Unable to rm -rf bucket database directory ~s~n~p", [Bucket, Error]),
                         Error
                 end;
             Error ->
                 ale:error(?USER_LOGGER, "Unable to delete some DBs for bucket ~s. Leaving bucket directory undeleted~n~p", [Bucket, Error]),
                 Error
         end,
    do_delete_bucket_indexes(Bucket),
    RV.

do_delete_bucket_indexes(Bucket) ->
    {ok, BaseIxDir} = ns_storage_conf:this_node_ixdir(),
    couch_set_view:delete_index_dir(BaseIxDir, list_to_binary(Bucket)).

delete_databases_loop([]) ->
    ok;
delete_databases_loop([Db | Rest]) ->
    case delete_couch_database(Db) of
        ok ->
            delete_databases_loop(Rest);
        Error ->
            Error
    end.

bucket_databases(Bucket) when is_list(Bucket) ->
    bucket_databases(list_to_binary(Bucket));
bucket_databases(Bucket) when is_binary(Bucket) ->
    couch_server:all_known_databases_with_prefix(iolist_to_binary([Bucket, $/])).

delete_couch_database(DB) ->
    RV = couch_server:delete(DB, []),
    ?log_info("Deleting database ~p: ~p~n", [DB, RV]),
    RV.
