%%%===================================================================
%% @author Erdem Aksu
%% @copyright 2016 Pundun Labs AB
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
%% implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%% -------------------------------------------------------------------
%% @doc
%% Module Description:
%% @end
%%%===================================================================

-module(gb_reg).

%% API
-export([start/0,
	 insert/2,
	 insert/3,
	 add/2,
	 add_keys/2,
	 delete/2,
	 lookup/2,
	 all/1]).

-export([new/1,
	 new/2,
	 purge/1]).

%%%===================================================================
%%% API
%%%===================================================================
%%--------------------------------------------------------------------
%% @doc
%% Insert a new register entry for Key -> Value mapping to module Mod.
%% @end
%%--------------------------------------------------------------------
-spec insert(Mod :: module(), Key :: term(), Value :: term()) ->
    ok | {error, Reason :: term()}.
insert(Mod, Key, Value) ->
    insert(Mod, Key, Value, is_literal_term([Key, Value])).

-spec insert(Mod :: module(),
	     Key :: term(),
	     Value :: term(),
	     Bool :: true | false) ->
    {ok, Beam :: binary()} | {error, Reason :: term()}.
insert(Mod, Key, Value, true) ->
    gen_server:call(Mod, {insert, Mod, Key, Value});
insert(_, _, Value, false) ->
    {error, {data_not_literal, Value}}.

%%--------------------------------------------------------------------
%% @doc
%% Insert a list of KVL entries to module Mod.
%% @end
%%--------------------------------------------------------------------
-spec insert(Mod :: module(), Kvl :: [{term(), term()}]) ->
    ok | {error, Reason :: term()}.
insert(Mod, Kvl) ->
    insert_kvl(Mod, Kvl, is_literal_term(Kvl)).

-spec insert_kvl(Mod :: module(),
		 Kvl :: [{term(), term()}],
		 Bool :: true | false) ->
    {ok, Beam :: binary()} | {error, Reason :: term()}.
insert_kvl(Mod, Kvl, true) ->
    gen_server:call(Mod, {insert_kvl, Mod, Kvl});
insert_kvl(_, Kvl, false) ->
    {error, {data_not_literal, Kvl}}.

%%--------------------------------------------------------------------
%% @doc
%% Add a list of KVL entries to module Mod only if Keys do not exist
%% already.
%% @end
%%--------------------------------------------------------------------
-spec add(Mod :: module(), Kvl :: [{term(), term()}]) ->
    ok | {error, Reason :: term()}.
add(Mod, Kvl) ->
    add_kvl(Mod, Kvl, is_literal_term(Kvl)).

-spec add_kvl(Mod :: module(),
	      Kvl :: [{term(), term()}],
	      Bool :: true | false) ->
    ok | {error, Reason :: term()}.
add_kvl(Mod, Kvl, true) ->
    gen_server:call(Mod, {add_kvl, Mod, Kvl});
add_kvl(_, Kvl, false) ->
    {error, {data_not_literal, Kvl}}.

%%--------------------------------------------------------------------
%% @doc
%% Add a list of Keys to module Mod only if Keys do not exist already.
%% Value will be added as an incremented integer, encoded as unsigned
%% integer in binary. Additionally {Value, Key} entries will be added.
%% @end
%%--------------------------------------------------------------------
-spec add_keys(Mod :: module(), Keys :: [term()]) ->
    ok | {error, Reason :: term()}.
add_keys(Mod, Keys) ->
    add_keys(Mod, Keys, is_literal_term(Keys)).

-spec add_keys(Mod :: module(),
	       Keys :: [term()],
	       Bool :: true | false) ->
    ok | {error, Reason :: term()}.
add_keys(Mod, Keys, true) ->
    gen_server:call(Mod, {add_keys, Mod, Keys});
add_keys(_, Keys, false) ->
    {error, {data_not_literal, Keys}}.

%%--------------------------------------------------------------------
%% @doc
%% Delete a register entry from module Mod specified by Key.
%% @end
%%--------------------------------------------------------------------
-spec delete(Mod :: module(), Key :: string()) ->
    {ok, Beam :: binary()} | {error, Reason ::term()}.
delete(Mod, Key) ->
    gen_server:call(Mod, {delete, Mod, Key}).

%%--------------------------------------------------------------------
%% @doc
%% Lookup for a register entry from module Mod specified by Key.
%% @end
%%--------------------------------------------------------------------
-spec lookup(Mod :: module(), Key :: string()) ->
    Value :: term() | undefined.
lookup(Mod, Key) ->
    Mod:lookup(Key).

%%--------------------------------------------------------------------
%% @doc
%% Lookup for all entries from module Mod.
%% @end
%%--------------------------------------------------------------------
-spec all(Mod :: module()) ->
    Entries :: map().
all(Mod) ->
    Mod:entries().

%%--------------------------------------------------------------------
%% @doc
%% Generate new register module.
%% @end
%%--------------------------------------------------------------------
-spec new(Name :: string()) ->
    {ok, Module :: module()} | {error, Reason ::term()}.
new(Name) ->
    Dir = get_registry_dir(),
    Module = get_module_name(Name),
    Args = [{dir, Dir}, {mod, Module}],
    {ok, _} = supervisor:start_child(gb_reg_worker_sup, [Args]),
    {ok, Module}.

%%--------------------------------------------------------------------
%% @doc
%% Generate new register module and initialize with given entries.
%% @end
%%--------------------------------------------------------------------
-spec new(Name :: string(), Tuples :: [{Key :: term(), Value :: term()}]) ->
    {ok, Mod :: module()} | {error, Reason ::term()}.
new(Name, Tuples) ->
    case lists:usort([is_literal_term(KV) || KV <- Tuples]) of
	[true] ->
	    Dir = get_registry_dir(),
	    Module = get_module_name(Name),
	    Args = [{dir, Dir}, {mod, Module},{entries, Tuples}],
	    {ok, _} = supervisor:start_child(gb_reg_worker_sup, [Args]),
	    {ok, Module};
	_ ->
	    {error, non_literal_term}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Pruge a register module Mod.
%% @end
%%--------------------------------------------------------------------
-spec purge(Mod :: module()) ->
    ok | {error, Reason ::term()}.
purge(Mod) ->
    gen_server:call(Mod, {purge, Mod}).

%%--------------------------------------------------------------------
%% @doc
%% Initialize workers if any stored beam exist.
%% @end
%%--------------------------------------------------------------------
-spec start() ->  ignore.
start() ->
    RegDir = get_registry_dir(),
    ok = filelib:ensure_dir(RegDir),
    {ok, Files} = file:list_dir(RegDir),
    Args = [{load, true}, {dir, RegDir}],
    [supervisor:start_child(gb_reg_worker_sup, 
	[[{file, F}, {mod, erlang:list_to_atom(F)} | Args]]) || F <- Files],
    ignore.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Taken from cerl.erl and added maps support.
%% @spec is_literal_term(Term::term()) -> boolean()
%%
%% @doc Returns <code>true</code> if <code>Term</code> can be
%% represented as a literal, otherwise <code>false</code>. This
%% function takes time proportional to the size of <code>Term</code>.
%% This function is a copy from cerl.erl with added support for
%% maps.
%% @see abstract/1

-spec is_literal_term(term()) -> boolean().

is_literal_term(T) when is_integer(T) -> true;
is_literal_term(T) when is_float(T) -> true;
is_literal_term(T) when is_atom(T) -> true;
is_literal_term([]) -> true;
is_literal_term([H | T]) ->
    is_literal_term(H) andalso is_literal_term(T);
is_literal_term(T) when is_tuple(T) ->
    is_literal_term_list(tuple_to_list(T));
is_literal_term(T) when is_map(T) ->
    is_literal_term_list(maps:to_list(T));
is_literal_term(B) when is_bitstring(B) -> true;
is_literal_term(_) ->
    false.

-spec is_literal_term_list([term()]) -> boolean().

is_literal_term_list([T | Ts]) ->
    case is_literal_term(T) of
	true ->
	    is_literal_term_list(Ts);
	false ->
	    false
    end;
is_literal_term_list([]) ->
    true.

-spec get_registry_dir()->
    string().
get_registry_dir() ->
    ROOTDIR = os:getenv("ROOTDIR"),
    filename:join(ROOTDIR, "data/gb_reg")++"/".

-spec get_module_name(Name :: string()) ->
    Module :: module().
get_module_name(Name) ->
    erlang:list_to_atom("$"++Name).
