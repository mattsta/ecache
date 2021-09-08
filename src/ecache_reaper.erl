-module(ecache_reaper).

-behaviour(gen_server).

-export([start/2, start_link/2]).
-export([start_link/1]).
-deprecated({start_link, 1, next_major_release}).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(reaper, {name :: atom(), size :: pos_integer()}).
-define(TIMEOUT, 4000).

-spec start(Name::atom(), Size::pos_integer()) -> {ok, pid()} | {error, term()}.
start(Name, Size) -> gen_server:start(?MODULE, [Name, Size], []).

-spec start_link(Name::atom(), Size::pos_integer()) -> {ok, pid()} | {error, term()}.
start_link(Name, Size) -> gen_server:start_link(?MODULE, [Name, Size], []).

-spec start_link(Name::atom()) -> {ok, pid()} | {error, term()}.
start_link(Name) -> gen_server:start_link(?MODULE, [Name, 8], []).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_server
%%%----------------------------------------------------------------------

-spec init([atom()|pos_integer]) -> {ok, #reaper{}, ?TIMEOUT}.
init([Name, Size]) -> {ok, #reaper{name = Name, size = Size}, ?TIMEOUT}.

-spec handle_call(Arbitrary, {pid(), term()}, State) -> {reply, {arbitrary, Arbitrary}, State, ?TIMEOUT}
        when Arbitrary::term(), State::#reaper{}.
handle_call(Arbitrary, _From, State) -> {reply, {arbitrary, Arbitrary}, State, ?TIMEOUT}.

-spec handle_cast(term(), State) -> {noreply, State, ?TIMEOUT} when State::#reaper{}.
handle_cast(_Request, State) -> {noreply, State, ?TIMEOUT}.

-spec handle_info(term(), State) -> {noreply, State, ?TIMEOUT} when State::#reaper{}.
handle_info(timeout, #reaper{name = Name, size = Size} = State) ->
    shrink_cache_to_size(Name, ecache:total_size(Name), Size),
    {noreply, State, ?TIMEOUT};
handle_info(Info, State) ->
    error_logger:warning_msg("Other info of: ~p~n", [Info]),
    {noreply, State, ?TIMEOUT}.

-spec terminate(term(), #reaper{}) -> ok.
terminate(_Reason, _State) -> ok.

-spec code_change(term(), State, term()) -> {ok, State} when State::#reaper{}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%----------------------------------------------------------------------
%%% Internal functions
%%%----------------------------------------------------------------------

shrink_cache_to_size(Name, CurrentSize, Size) ->
    CurrentSize > Size andalso shrink_cache_to_size(Name, gen_server:call(Name, reap_oldest), Size).
