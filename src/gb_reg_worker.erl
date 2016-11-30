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

-module(gb_reg_worker).

-behaviour(gen_server).

-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
    Module = proplists:get_value(mod, Args),
    case proplists:get_value(load, Args, false) of
	true ->
	    gen_server:start_link({local, Module}, ?MODULE,
				  [load | Args], []);
	false ->
	    Module = proplists:get_value(mod, Args),
	    gen_server:start_link({local, Module}, ?MODULE,
				  [new | Args], [])
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([load | Args]) ->
    File = proplists:get_value(file, Args),
    Module = proplists:get_value(mod, Args),
    Dir = proplists:get_value(dir, Args),
    Filename = filename:join([Dir, File]),
    {ok, Beam} =  file:read_file(Filename),
    load_register(Module, Filename, Beam),
    {ok, #{filename => Filename}};
init([new | Args]) ->
    Dir = proplists:get_value(dir, Args),
    Module = proplists:get_value(mod, Args),
    Filename = filename:join([Dir, Module]),
    Entries = proplists:get_value(entries, Args, []),
    {ok, _, Beam} = gen_beam(Module, Entries, 0),
    store_beam(Filename, Beam),
    load_register(Module, Filename, Beam),
    {ok, #{filename => Filename}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({add_keys, Mod, Keys}, _From, State = #{filename := Filename}) ->
    Filter = fun(Key) ->
		case Mod:lookup(Key) of
		    undefined -> true;
		    _ -> false
		end
	    end,
    NewKeys = lists:filter(Filter, Keys),
    case generate_entries(Mod:ref(), NewKeys, []) of
	{_, Add} when map_size(Add) == 0 ->
	    {reply, ok, State};
	{Ref, Add} ->
	    Merged = maps:merge(Mod:entries(), Add),
	    Reply = regen_register(Mod, Filename, Merged, Ref),
	    {reply, element(1, Reply), State}
	end;
handle_call({add_kvl, Mod, Kvl}, _From, State = #{filename := Filename}) ->
    Filter = fun({Key, _}) ->
		case Mod:lookup(Key) of
		    undefined -> true;
		    _ -> false
		end
	    end,
    NewKvl = lists:filter(Filter, Kvl),
    Add = maps:from_list(NewKvl),
    Merged = maps:merge(Mod:entries(), Add),
    Reply = regen_register(Mod, Filename, Merged, Mod:ref()),
    {reply, element(1, Reply), State};
handle_call({insert, Mod, Key, Val}, _From, State = #{filename := Filename}) ->
    case Mod:entries() of
    #{Key := Val} ->
	{reply, ok, State};
    Entries ->
	Reply = regen_register(Mod, Filename, Entries#{Key => Val}, Mod:ref()),
	{reply, element(1,Reply), State}
    end;
handle_call({insert_kvl, Mod, Kvl}, _From, State = #{filename := Filename}) ->
    Add = maps:from_list(Kvl),
    Entries = Mod:entries(),
    case maps:merge(Entries, Add) of
    Entries ->
	{reply, ok, State};
    Merged ->
	Reply = regen_register(Mod, Filename, Merged, Mod:ref()),
	{reply, element(1, Reply), State}
    end;
handle_call({delete, Mod, Key}, _From, State = #{filename := Filename}) ->
    case Mod:entries() of
	#{Key := _} = Entries ->
	    Reply = regen_register(Mod, Filename,
				   maps:remove(Key, Entries), Mod:ref()),
	    {reply, element(1,Reply), State};
	_ ->
	    {reply, ok, State}
    end;
handle_call({purge, Mod}, _From, State = #{filename := Filename}) ->
    code:purge(Mod),
    code:delete(Mod),
    file:delete(Filename),
    {stop, normal,ok, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
   ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Generate register with given Entries.
%% @end
%%--------------------------------------------------------------------
-spec gen_beam(Mod :: module(),
	       Tuples :: [{term(), term()}],
	       Ref :: integer()) ->
    {ok, Mod :: module(), Beam :: binary()}.
gen_beam(Mod, Tuples, Ref) ->
    CEForms = make_mod(Mod, maps:from_list(Tuples), Ref),
    compile:forms(CEForms, [from_core, binary]).

%%--------------------------------------------------------------------
%% @doc
%% Re-generate register with given Entries.
%% @end
%%--------------------------------------------------------------------
-spec regen_register(Mod :: module(),
		     Filename :: string(),
		     Entries :: map(),
		     Ref :: integer()) ->
    {ok, Beam :: binary()}.
regen_register(Mod, Filename, Entries, Ref) ->
    CEForms = make_mod(Mod, Entries, Ref),
    {ok, _, Beam} = compile:forms(CEForms, [from_core, binary]),
    store_beam(Filename, Beam),
    load_register(Mod, Filename, Beam).

%%--------------------------------------------------------------------
%% @doc
%% Write object code to file to store persistent.
%% @end
%%--------------------------------------------------------------------
-spec store_beam(Filename :: string(),
		 Bin :: binary()) ->
    ok.
store_beam(Filename, Bin) ->
    file:write_file(Filename, Bin).

%%--------------------------------------------------------------------
%% @doc
%% Load object code of register module code on node().
%% @end
%%--------------------------------------------------------------------
-spec load_register(Mod :: module(),
		    Filename :: string(),
		    Bin :: binary()) ->
    {ok, Bin :: binary()}.
load_register(Mod, Filename, Bin) ->
    {module, _ } = code:load_binary(Mod, Filename, Bin),
    {ok, Bin}.

%%--------------------------------------------------------------------
%% @doc
%% Make module Mod with lookup function that matches
%% terms in Entries.
%% @end
%%--------------------------------------------------------------------
-spec make_mod(Mod :: module(),
	       Entries :: map(),
	       Ref :: integer()) ->
    term().
make_mod(Mod, Entries, Ref) ->
    ModuleName = cerl:c_atom(Mod),
    cerl:c_module(ModuleName,
		  [cerl:c_fname(entries, 0),
		   cerl:c_fname(ref, 0),
		   cerl:c_fname(lookup, 1),
		   cerl:c_fname(module_info, 0),
		   cerl:c_fname(module_info, 1)],
		  [make_entries_fun(Entries),
		   make_ref_fun(Ref),
		   make_lookup_fun(Entries) | mod_info(ModuleName)]).

%%--------------------------------------------------------------------
%% @doc
%% Make entries/0 function.
%% @end
%%--------------------------------------------------------------------
make_entries_fun(Entries) ->
    {cerl:c_fname(entries,0), cerl:c_fun([], cerl:abstract(Entries))}.

%%--------------------------------------------------------------------
%% @doc
%% Make ref/0 function.
%% @end
%%--------------------------------------------------------------------
make_ref_fun(Ref) ->
    {cerl:c_fname(ref,0), cerl:c_fun([], cerl:c_int(Ref))}.

%%--------------------------------------------------------------------
%% @doc
%% Make lookup/1 function.
%% @end
%%--------------------------------------------------------------------
make_lookup_fun(Entries) ->
    Arg1 = cerl:c_var('FuncArg1'),
    Else = cerl:c_var('Else'),
    True = cerl:c_atom(true),
    Undefined = cerl:c_atom(undefined),
    Clauses = make_lookup_clauses(Arg1, Entries),
    LastClause = cerl:c_clause([Else], True, Undefined),
    Case = cerl:c_case(Arg1, Clauses ++ [LastClause]),
    {cerl:c_fname(lookup,1), cerl:c_fun([Arg1], Case)}.

%%--------------------------------------------------------------------
%% @doc
%% Make case clauses for lookup/1 function.
%% @end
%%--------------------------------------------------------------------
make_lookup_clauses(Arg1, Entries) ->
    {_, Acc} = maps:fold(fun make_lookup_clauses/3, {Arg1, []}, Entries),
    Acc.

%%--------------------------------------------------------------------
%% @doc
%% Make case clauses for lookup/1 function.
%% @end
%%--------------------------------------------------------------------
make_lookup_clauses(Key, Value, {Arg1, Acc}) ->
    Pattern = [cerl:abstract(Key)],
    Guard = cerl:c_atom(true),
    Body = cerl:abstract(Value),
    Clause = cerl:c_clause(Pattern, Guard, Body),
    {Arg1, [Clause | Acc]}.

%%--------------------------------------------------------------------
%% @doc
%% Make module_info/1 function.
%% @end
%%--------------------------------------------------------------------
mod_info(Name) ->
    M = cerl:c_atom(erlang),
    F = cerl:c_atom(get_module_info),
    Info0 = {cerl:c_fname(module_info, 0),
	     cerl:c_fun([], cerl:c_call(M, F, [Name]))},
    Key = cerl:c_var('Key'),
    Info1 = {cerl:c_fname(module_info, 1),
	     cerl:c_fun([Key], cerl:c_call(M, F, [Name, Key]))},
    [Info0, Info1].

-spec generate_entries(Ref :: integer(),
		       Keys :: [term()],
		       Acc :: [{term(), term()}]) ->
    {NewRef :: integer(), Map :: map()}.
generate_entries(Ref, [Key | Rest], Acc) ->
    generate_entries(Ref + 1, Rest, [{Key, Ref}, {Ref, Key} | Acc]);
generate_entries(Ref, [], Acc) ->
    {Ref, maps:from_list(Acc)}.

