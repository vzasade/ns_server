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
%% @doc unified access api for admin and ro_admin credentials

-module(ns_config_auth).

-include("ns_common.hrl").

-export([authenticate/2,
         set_credentials/3,
         get_user/1,
         get_password/1,
         credentials_changed/3,
         unset_credentials/1,
         get_creds/2,
         is_system_provisioned/0,
         is_system_provisioned/1,
         hash_password/1,
         hash_password/2]).

get_key(admin) ->
    rest_creds;
get_key(ro_admin) ->
    read_only_user_creds.

set_credentials(Role, User, Password) ->
    ns_config:set(get_key(Role), {User, {password, hash_password(Password)}}).

is_system_provisioned() ->
    is_system_provisioned(ns_config:latest()).

is_system_provisioned(Config) ->
    case ns_config:search(Config, get_key(admin)) of
        {value, {_U, _}} ->
            true;
        _ ->
            false
    end.

get_user(special) ->
    "@";
get_user(Role) ->
    case ns_config:search(get_key(Role)) of
        {value, {U, _}} ->
            U;
        _ ->
            undefined
    end.

get_password(special) ->
    ns_config:search_node_prop(ns_config:latest(), memcached, admin_pass).

get_creds(Config, Role) ->
    case ns_config:search(Config, get_key(Role)) of
        {value, {User, {password, {Salt, Mac}}}} ->
            {User, Salt, Mac};
        _ ->
            undefined
    end.

credentials_changed(Role, User, Password) ->
    case ns_config:search(get_key(Role)) of
        {value, {User, {password, {Salt, Mac}}}} ->
            hash_password(Salt, Password) =/= Mac;
        _ ->
            true
    end.

authenticate(admin, [$@ | _] = User, Password) ->
    Password =:= ns_config:search_node_prop(ns_config:latest(), memcached, admin_pass)
        orelse authenticate_non_special(admin, User, Password);
authenticate(Role, User, Password) ->
    authenticate_non_special(Role, User, Password).

authenticate(Username, Password) ->
    case authenticate(admin, Username, Password) of
        true ->
            {ok, {Username, admin}};
        false ->
            case authenticate(ro_admin, Username, Password) of
                true ->
                    {ok, {Username, ro_admin}};
                false ->
                    case menelaus_users:authenticate(Username, Password) of
                        true ->
                            {ok, {Username, builtin}};
                        false ->
                            false
                    end
            end
    end.

authenticate_non_special(Role, User, Password) ->
    do_authenticate(Role, ns_config:search(get_key(Role)), User, Password).

do_authenticate(_Role, {value, {User, {password, {Salt, Mac}}}}, User, Password) ->
    hash_password(Salt, Password) =:= Mac;
do_authenticate(admin, {value, null}, _User, _Password) ->
    true;
do_authenticate(_Role, _Creds, _User, _Password) ->
    false.

unset_credentials(Role) ->
    ns_config:set(get_key(Role), null).

hash_password(Password) ->
    Salt = crypto:rand_bytes(16),
    {Salt, hash_password(Salt, Password)}.

hash_password(Salt, Password) ->
    {module, crypto} = code:ensure_loaded(crypto),
    {F, A} =
        case erlang:function_exported(crypto, hmac, 3) of
            true ->
                {hmac, [sha, Salt, list_to_binary(Password)]};
            false ->
                {sha_mac, [Salt, list_to_binary(Password)]}
        end,

    erlang:apply(crypto, F, A).
