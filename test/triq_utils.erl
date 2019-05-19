%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-2018 Couchbase, Inc.
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
-module(triq_utils).

-include("triq.hrl").

-export([smaller/1, min_size/2]).

smaller(Domain) ->
    ?SIZED(Size,
           resize(Size div 2, Domain)).

min_size(Domain, MinSize) ->
    ?SIZED(Size,
           case Size >= MinSize of
               true ->
                   Domain;
               false ->
                   resize(MinSize, Domain)
           end).
