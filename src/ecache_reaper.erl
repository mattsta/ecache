-module(ecache_reaper).

-behaviour(gen_server).

-export([start/2]).
-export([start_link/1, start_link/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([ecache_reaper/2]). % quiet unused function annoyance
-record(reaper, {cache_size}).

start_link(Name) -> start_link(Name, 8).

start_link(Name, Size) -> gen_server:start_link(?MODULE, [Name, Size], []).

start(Name, Size) -> gen_server:start(?MODULE, [Name, Size], []).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_server
%%%----------------------------------------------------------------------

shrink_cache_to_size(_Name, CurrentSize, Size) when CurrentSize =< Size -> ok;
shrink_cache_to_size(Name, _CurrentSize, Size) ->
    gen_server:call(Name, reap_oldest),
    shrink_cache_to_size(Name, ecache:total_size(Name), Size).

ecache_reaper(Name, Size) ->
    % sleep for 4 seconds
    shrink_cache_to_size(Name, ecache:total_size(Name), Size),
    timer:apply_after(4000, ?MODULE, ecache_reaper, [Name, Size]).

init([_Name, SizeBytes] = Args) ->
    % ecache_reaper is started from ecache_server, but ecache_server can't finish
    % init'ing % until ecache_reaper:init/1 returns.
    % Use apply_after to make sure ecache_server exists when making calls.
    % Don't be clever and take this timer away.  Compensates for chicken/egg prob.
    timer:apply_after(4000, ?MODULE, ecache_reaper, Args),
    {ok, #reaper{cache_size = SizeBytes}}.

handle_call(Arbitrary, _From, State) -> {reply, {arbitrary, Arbitrary}, State}.

handle_cast(_Request, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

handle_info(Info, State) ->
    error_logger:info_report("Other info of: ~p~n", [Info]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
