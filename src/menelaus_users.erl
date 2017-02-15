%% @author Couchbase <info@couchbase.com>
%% @copyright 2016 Couchbase, Inc.
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
%% @doc implementation of builtin and saslauthd users

-module(menelaus_users).

-include("ns_common.hrl").
-include("ns_config.hrl").
-include("rbac.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([get_users_45/1,
         select_users/1,
         select_auth_infos/1,
         store_user/4,
         delete_user/1,
         change_password/2,
         authenticate/2,
         get_memcached_auth/1,
         get_roles/1,
         get_user_name/1,
         upgrade_to_4_5/1,
         build_memcached_auth_info/1,
         get_users_version/0,
         get_auth_version/0]).

%% callbacks for replicated_dets
-export([init/1, on_save/2]).

-export([start_storage/0, start_replicator/0]).

replicator_name() ->
    users_replicator.

storage_name() ->
    users_storage.

versions_name() ->
    menelaus_users_versions.

start_storage() ->
    Replicator = erlang:whereis(replicator_name()),
    Path = filename:join(path_config:component_path(data, "config"), "users.dets"),
    replicated_dets:start_link(?MODULE, [], storage_name(), Path, Replicator).

get_users_version() ->
    [{user_version, V, Base}] = ets:lookup(versions_name(), user_version),
    {V, Base}.

get_auth_version() ->
    [{auth_version, V, Base}] = ets:lookup(versions_name(), auth_version),
    {V, Base}.

start_replicator() ->
    GetRemoteNodes =
        fun () ->
                ns_node_disco:nodes_actual_other()
        end,
    doc_replicator:start_link(replicated_dets, replicator_name(), GetRemoteNodes, storage_name()).

init([]) ->
    Base = crypto:rand_uniform(0, 16#100000000),

    _ = ets:new(versions_name(), [protected, named_table]),
    ets:insert_new(versions_name(), [{user_version, 0, Base}, {auth_version, 0, Base}]),
    gen_event:notify(user_storage_events, {user_version, {0, Base}}),
    gen_event:notify(user_storage_events, {auth_version, {0, Base}}),
    Base.

on_save({user, _}, Base) ->
    Ver = ets:update_counter(versions_name(), user_version, 1),
    gen_event:notify(user_storage_events, {user_version, {Ver, Base}}),
    Base;
on_save({auth, _}, Base) ->
    Ver = ets:update_counter(versions_name(), auth_version, 1),
    gen_event:notify(user_storage_events, {auth_version, {Ver, Base}}),
    Base.

-spec get_users_45(ns_config()) -> [{rbac_identity(), []}].
get_users_45(Config) ->
    ns_config:search(Config, user_roles, []).

select_users(KeySpec) ->
    replicated_dets:select(storage_name(), {user, KeySpec}, 100).

select_auth_infos(KeySpec) ->
    replicated_dets:select(storage_name(), {auth, KeySpec}, 100).

build_auth(false, undefined, _UserName) ->
    password_required;
build_auth(false, Password, UserName) ->
    [{ns_server, ns_config_auth:hash_password(Password)},
     {memcached, build_memcached_auth(UserName, Password)}];
build_auth({_, _}, undefined, _UserName) ->
    same;
build_auth({_, CurrentAuth}, Password, UserName) ->
    {Salt, Mac} = get_salt_and_mac(CurrentAuth),
    case ns_config_auth:hash_password(Salt, Password) of
        Mac ->
            case get_memcached_auth(CurrentAuth) of
                undefined ->
                    [{memcached, build_memcached_auth(UserName, Password)} | CurrentAuth];
                _ ->
                    same
            end;
        _ ->
            [{ns_server, ns_config_auth:hash_password(Password)},
             {memcached, build_memcached_auth(UserName, Password)}]
    end.

build_memcached_auth(User, Password) ->
    [MemcachedAuth] = build_memcached_auth_info([{User, Password}]),
    MemcachedAuth.

-spec store_user(rbac_identity(), rbac_user_name(), rbac_password(), [rbac_role()]) -> run_txn_return().
store_user(Identity, Name, Password, Roles) ->
    Props = case Name of
                undefined ->
                    [];
                _ ->
                    [{name, Name}]
            end,
    case cluster_compat_mode:is_cluster_spock() of
        true ->
            store_user_spock(Identity, Props, Password, Roles, ns_config:get());
        false ->
            store_user_45(Identity, Props, Roles)
    end.

store_user_45({_UserName, saslauthd} = Identity, Props, Roles) ->
    ns_config:run_txn(
      fun (Config, SetFn) ->
              case menelaus_roles:validate_roles(Roles, Config) of
                  ok ->
                      Users = get_users_45(Config),
                      NewUsers = lists:keystore(Identity, 1, Users,
                                                {Identity, [{roles, Roles} | Props]}),
                      {commit, SetFn(user_roles, NewUsers, Config)};
                  Error ->
                      {abort, Error}
              end
      end).

store_user_spock({UserName, Type} = Identity, Props, Password, Roles, Config) ->
    CurrentAuth = replicated_dets:get(storage_name(), {auth, Identity}),
    case Type of
        saslauthd ->
            store_user_spock_with_auth(Identity, Props, same, Roles, Config);
        builtin ->
            case build_auth(CurrentAuth, Password, UserName) of
                password_required ->
                    {abort, password_required};
                Auth ->
                    store_user_spock_with_auth(Identity, Props, Auth, Roles, Config)
            end
    end.

store_user_spock_with_auth(Identity, Props, Auth, Roles, Config) ->
    case menelaus_roles:validate_roles(Roles, Config) of
        ok ->
            ok = replicated_dets:set(storage_name(), {user, Identity}, [{roles, Roles} | Props]),
            case Auth of
                same ->
                    ok;
                _ ->
                    ok = replicated_dets:set(storage_name(), {auth, Identity}, Auth)
            end,
            {commit, ok};
        Error ->
            {abort, Error}
    end.

change_password({UserName, builtin} = Identity, Password) when is_list(Password) ->
    case replicated_dets:get(storage_name(), {user, Identity}) of
        false ->
            user_not_found;
        _ ->
            CurrentAuth = replicated_dets:get(storage_name(), {auth, Identity}),
            Auth = build_auth(CurrentAuth, Password, UserName),
            replicated_dets:set(storage_name(), {auth, Identity}, Auth)
    end.

-spec delete_user(rbac_identity()) -> run_txn_return().
delete_user(Identity) ->
    case cluster_compat_mode:is_cluster_spock() of
        true ->
            delete_user_spock(Identity);
        false ->
            delete_user_45(Identity)
    end.

delete_user_45(Identity) ->
    ns_config:run_txn(
      fun (Config, SetFn) ->
              case ns_config:search(Config, user_roles) of
                  false ->
                      {abort, {error, not_found}};
                  {value, Users} ->
                      case lists:keytake(Identity, 1, Users) of
                          false ->
                              {abort, {error, not_found}};
                          {value, _, NewUsers} ->
                              {commit, SetFn(user_roles, NewUsers, Config)}
                      end
              end
      end).

delete_user_spock({_, Type} = Identity) ->
    case Type of
        builtin ->
            _ = replicated_dets:delete(storage_name(), {auth, Identity});
        saslauthd ->
            ok
    end,
    case replicated_dets:delete(storage_name(), {user, Identity}) of
        {not_found, _} ->
            {abort, {error, not_found}};
        ok ->
            {commit, ok}
    end.

get_salt_and_mac(Auth) ->
    proplists:get_value(ns_server, Auth).

get_memcached_auth(Auth) ->
    proplists:get_value(memcached, Auth).

-spec authenticate(rbac_user_id(), rbac_password()) -> boolean().
authenticate(Username, Password) ->
    case cluster_compat_mode:is_cluster_spock() of
        true ->
            Identity = {Username, builtin},
            case replicated_dets:get(storage_name(), {user, Identity}) of
                false ->
                    false;
                _ ->
                    case replicated_dets:get(storage_name(), {auth, Identity}) of
                        false ->
                            false;
                        {_, Auth} ->
                            {Salt, Mac} = get_salt_and_mac(Auth),
                            ns_config_auth:hash_password(Salt, Password) =:= Mac
                    end
            end;
        false ->
            false
    end.

-spec get_roles(rbac_identity()) -> [rbac_role()].
get_roles(Identity) ->
    Props =
        case cluster_compat_mode:is_cluster_spock() of
            true ->
                replicated_dets:get(storage_name(), {user, Identity}, []);
            false ->
                ns_config:search_prop(ns_config:latest(), user_roles, Identity, [])
        end,
    proplists:get_value(roles, Props, []).

-spec get_user_name(rbac_identity()) -> rbac_user_name().
get_user_name(Identity) ->
    Props =
        case cluster_compat_mode:is_cluster_spock() of
            false ->
                ns_config:search_prop(ns_config:latest(), user_roles, Identity, []);
            true ->
                replicated_dets:get(storage_name(), {user, Identity}, [])
        end,
    proplists:get_value(name, Props).

collect_result(Port, Acc) ->
    receive
        {Port, {exit_status, Status}} ->
            {Status, lists:flatten(lists:reverse(Acc))};
        {Port, {data, Msg}} ->
            collect_result(Port, [Msg | Acc])
    end.

remove_struct({struct, KVs}) when is_list(KVs) ->
    remove_struct({KVs});
remove_struct({KVs}) when is_list(KVs) ->
    {[remove_struct(KV) || KV <- KVs]};
remove_struct(List) when is_list(List) ->
    [remove_struct(Elem) || Elem <- List];
remove_struct({K, V}) ->
    {K, remove_struct(V)};
remove_struct(V) ->
    V.

build_memcached_auth_info(UserPasswords) ->
    Iterations = ns_config:read_key_fast(memcached_password_hash_iterations, 4000),
    Port = ns_ports_setup:run_cbsasladm(Iterations),
    lists:foreach(
      fun ({User, Password}) ->
              PasswordStr = User ++ " " ++ Password ++ "\n",
              Port ! {self(), {command, list_to_binary(PasswordStr)}}
      end, UserPasswords),
    Port ! {self(), {command, <<"\n">>}},
    {0, Json} = collect_result(Port, []),
    {struct, [{<<"users">>, Infos}]} = mochijson2:decode(Json),
    [remove_struct(I) || I <- Infos].

collect_users(asterisk, _Role, Dict) ->
    Dict;
collect_users([], _Role, Dict) ->
    Dict;
collect_users([User | Rest], Role, Dict) ->
    NewDict = dict:update(User, fun (Roles) ->
                                        ordsets:add_element(Role, Roles)
                                end, ordsets:from_list([Role]), Dict),
    collect_users(Rest, Role, NewDict).

-spec upgrade_to_4_5(ns_config()) -> [{set, user_roles, _}].
upgrade_to_4_5(Config) ->
    case ns_config:search(Config, saslauthd_auth_settings) of
        false ->
            [];
        {value, Props} ->
            case proplists:get_value(enabled, Props, false) of
                false ->
                    [];
                true ->
                    Dict = dict:new(),
                    Dict1 = collect_users(proplists:get_value(admins, Props, []), admin, Dict),
                    Dict2 = collect_users(proplists:get_value(roAdmins, Props, []), ro_admin, Dict1),
                    [{set, user_roles,
                      lists:map(fun ({User, Roles}) ->
                                        {{binary_to_list(User), saslauthd},
                                         [{roles, ordsets:to_list(Roles)}]}
                                end, dict:to_list(Dict2))}]
            end
    end.

upgrade_to_4_5_test() ->
    Config = [[{saslauthd_auth_settings,
                [{enabled,true},
                 {admins,[<<"user1">>, <<"user2">>, <<"user1">>, <<"user3">>]},
                 {roAdmins,[<<"user4">>, <<"user1">>]}]}]],
    UserRoles = [{{"user1", saslauthd}, [{roles, [admin, ro_admin]}]},
                 {{"user2", saslauthd}, [{roles, [admin]}]},
                 {{"user3", saslauthd}, [{roles, [admin]}]},
                 {{"user4", saslauthd}, [{roles, [ro_admin]}]}],
    Upgraded = upgrade_to_4_5(Config),
    ?assertMatch([{set, user_roles, _}], Upgraded),
    [{set, user_roles, UpgradedUserRoles}] = Upgraded,
    ?assertMatch(UserRoles, lists:sort(UpgradedUserRoles)).

upgrade_to_4_5_asterisk_test() ->
    Config = [[{saslauthd_auth_settings,
                [{enabled,true},
                 {admins, asterisk},
                 {roAdmins,[<<"user1">>]}]}]],
    UserRoles = [{{"user1", saslauthd}, [{roles, [ro_admin]}]}],
    Upgraded = upgrade_to_4_5(Config),
    ?assertMatch([{set, user_roles, _}], Upgraded),
    [{set, user_roles, UpgradedUserRoles}] = Upgraded,
    ?assertMatch(UserRoles, lists:sort(UpgradedUserRoles)).
