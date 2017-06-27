-module(ecache_server).

-behaviour(gen_server).

-export([start_link/3, start_link/4, start_link/5, start_link/6]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(cache, {name :: atom(),
                datum_index :: ets:tid(),
                table_pad = 0 :: non_neg_integer(),
                data_module :: module(),
                reaper_pid :: undefined|pid(),
                data_accessor :: atom(),
                size :: unlimited|non_neg_integer(),
                found = 0 :: non_neg_integer(),
                launched = 0 :: non_neg_integer(),
                policy = mru :: mru|actual_time,
                ttl = unlimited :: unlimited|non_neg_integer(),
                update_key_locks = #{} :: #{term() => pid()}}).

-record(datum, {key, mgr, data, started, ttl_reaper,
                last_active, ttl, type = mru, remaining_ttl}).

% make 8 MB cache
start_link(Name, Mod, Fun) -> start_link(Name, Mod, Fun, 8).

% make 5 minute expiry cache
start_link(Name, Mod, Fun, CacheSize) -> start_link(Name, Mod, Fun, CacheSize, 300000).

% make MRU policy cache
start_link(Name, Mod, Fun, CacheSize, CacheTime) -> start_link(Name, Mod, Fun, CacheSize, CacheTime, mru).

start_link(Name, Mod, Fun, CacheSize, CacheTime, Policy) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, Mod, Fun, CacheSize, CacheTime, Policy], []).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_server
%%%----------------------------------------------------------------------

init([Name, Mod, Fun, CacheSize, CacheTime, Policy]) ->
    DatumIndex = ets:new(Name, [set, compressed,
                                public,      % public because we spawn writers
                                {keypos, #datum.key}, % use Key stored in record
                                {read_concurrency, true}]),
    init(#cache{name = Name,
                datum_index = DatumIndex,
                table_pad = ets:info(DatumIndex, memory),
                data_module = Mod,
                data_accessor = Fun,
                ttl = CacheTime,
                policy = Policy,
                size = if
                           CacheSize =:= unlimited -> unlimited;
                           true -> CacheSize * (1024 * 1024)
                       end});
init(#cache{size = unlimited} = State) -> {ok, State};
init(#cache{name = Name, size = CacheSizeBytes} = State) ->
    {ok, State#cache{reaper_pid = begin
                                  {ok, ReaperPid} = ecache_reaper:start(Name, CacheSizeBytes),
                                  erlang:monitor(process, ReaperPid)
                                  end}}.

handle_call({generic_get, M, F, Key}, From, #cache{datum_index = Index} = State) ->
    P = self(),
    case fetch_data(key(M, F, Key), Index) of
        {ecache, notfound} ->
            {noreply,
             case State#cache.update_key_locks of
                 #{Key := CurrentLockPid} when is_pid(CurrentLockPid) ->
                     spawn(fun() ->
                               Ref = monitor(process, CurrentLockPid),
                               receive
                                   {'DOWN', Ref, process, _, _} -> gen_server:reply(From, gen_server:call(P, {get, Key}))
                               end
                           end),
                     State;
                 Locks ->
                     #cache{ttl = TTL, policy = Policy} = State,
                     State#cache{update_key_locks = Locks#{Key => spawn(fun() ->
                                                                            Data = launch_memoize_datum(Key, Index, M, F,
                                                                                                        TTL, Policy),
                                                                            gen_server:cast(P, {launched, Key}),
                                                                            gen_server:reply(From, Data)
                                                                        end)}}
            end};
        Data ->
            spawn(fun() -> gen_server:cast(P, found) end),
            {reply, Data, State}
    end;
handle_call({get, Key}, From, #cache{datum_index = Index} = State) ->
    P = self(),
    case fetch_data(key(Key), Index) of
        {ecache, notfound} ->
            {noreply,
             case State#cache.update_key_locks of
                 #{Key := CurrentLockPid} when is_pid(CurrentLockPid) ->
                     spawn(fun() ->
                               Ref = erlang:monitor(process, CurrentLockPid),
                               receive
                                   {'DOWN', Ref, process, _, _} -> gen_server:reply(From, gen_server:call(P, {get, Key}))
                               end
                           end),
                     State;
                 Locks ->
                     #cache{data_module = Module, data_accessor = Accessor, ttl = TTL, policy = Policy} = State,
                     State#cache{update_key_locks = Locks#{Key => spawn(fun() ->
                                                                            Data = launch_datum(Key, Index, Module,
                                                                                                Accessor, TTL, Policy),
                                                                            gen_server:cast(P, {launched, Key}),
                                                                            gen_server:reply(From, Data)
                                                                        end)}}
             end};
        Data ->
            spawn(fun() -> gen_server:cast(P, found) end),
            {reply, Data, State}
    end;
% NB: total_size using ETS includes ETS overhead.  An empty table still
% has a size.
handle_call(total_size, _From, #cache{} = State) -> {reply, cache_bytes(State), State};
handle_call(stats, _From, #cache{datum_index = DatumIndex, found = Found, launched = Launched} = State) ->
    EtsInfo = ets:info(DatumIndex),
    {reply,
     [{cache_name, proplists:get_value(name, EtsInfo)},
      {memory_size_bytes, cache_bytes(State, proplists:get_value(memory, EtsInfo))},
      {datum_count, proplists:get_value(size, EtsInfo)},
      {found, Found}, {launched, Launched}],
     State};
handle_call(empty, _From, #cache{datum_index = DatumIndex} = State) ->
    lists:foreach(fun([Reaper]) when is_pid(Reaper) -> exit(Reaper, kill);
                     (_) -> ok
                  end, ets:match(DatumIndex, #datum{_ = '_', ttl_reaper = '$1'})),
    ets:delete_all_objects(DatumIndex),
    {reply, ok, State};
handle_call(reap_oldest, _From, #cache{datum_index = DatumIndex} = State) ->
    DatumNow = #datum{last_active = os:timestamp()},
    LeastActive = ets:foldl(fun(#datum{last_active = LA} = A, #datum{last_active = Acc}) when LA < Acc -> A;
                               (_, Acc) -> Acc
                            end, DatumNow, DatumIndex),
    LeastActive =:= DatumNow orelse delete_object(DatumIndex, LeastActive),
    {reply, ok, State};
handle_call({rand, Type, Count}, From, #cache{datum_index = DatumIndex} = State) ->
    spawn(fun() ->
            AllKeys = get_all_keys(DatumIndex),
            Length = length(AllKeys),
            FoundData = 
            case Length =< Count of
              true  -> case Type of
                         data -> [fetch_data(P, DatumIndex) || P <- AllKeys];
                         keys -> [unkey(K) || K <- AllKeys]
                       end;
              false ->  RandomSet  = [crypto:rand_uniform(1, Length) || 
                                        _ <- lists:seq(1, Count)],
                        RandomKeys = [lists:nth(Q, AllKeys) || Q <- RandomSet],
                        case Type of
                          data -> [fetch_data(P, DatumIndex) || P <- RandomKeys];
                          keys -> [unkey(K) || K <- RandomKeys]
                        end
            end,
            gen_server:reply(From, FoundData)
          end),
    {noreply, State};
handle_call(Arbitrary, _From, State) -> {reply, {arbitrary, Arbitrary}, State}.

handle_cast({dirty, Id, NewData}, #cache{datum_index = DatumIndex} = State) ->
    replace_datum(key(Id), NewData, DatumIndex),
    {noreply, State};
handle_cast(found, #cache{found = Found} = State) -> {noreply, State#cache{found = Found + 1}};
handle_cast({launched, DatumKey}, #cache{launched = Launched, update_key_locks = Locks} = State) ->
    {noreply, State#cache{launched = Launched + 1, update_key_locks = maps:remove(DatumKey, Locks)}};
handle_cast({dirty, Id}, #cache{datum_index = DatumIndex} = State) ->
    delete_datum(DatumIndex, key(Id)),
    {noreply, State};
handle_cast({generic_dirty, M, F, A}, #cache{datum_index = DatumIndex} = State) ->
    delete_datum(DatumIndex, key(M, F, A)),
    {noreply, State}.

handle_info({destroy,_DatumPid, ok}, State) -> {noreply, State};
handle_info({'DOWN', _Ref, process, ReaperPid, _Reason},
            #cache{reaper_pid = ReaperPid, name = Name, size = Size} = State) ->
    {NewReaperPid, _Mon} = ecache_reaper:start_link(Name, Size),
    {noreply, State#cache{reaper_pid = NewReaperPid}};
handle_info({'DOWN', _Ref, process, _Pid, _Reason}, State) -> {noreply, State};
handle_info(Info, State) ->
    error_logger:info_report("Other info of: ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

-compile({inline, [{key, 1}, {key, 3}]}).
-compile({inline, [{unkey, 1}]}).
% keys are tagged/boxed so you can't cross-pollute a cache when using
% memoize mode versus the normal one-key-per-arg mode.
% Implication: *always* add key(Key) to keys from a user.  Don't pass user
% created keys directly to ets.
% The boxing overhead on 64 bit systems is: atom + tuple = 8 + 16 = 24 bytes
% The boxing overhead on 32 bit systems is: atom + tuple = 4 +  8 = 12 bytes
key(M, F, A) -> {ecache_multi, {M, F, A}}.
key(Key)     -> {ecache_plain, Key}.
unkey({ecache_plain, Key}) -> Key;
unkey({ecache_multi, {_, _, _} = MFA}) -> MFA.

%% ===================================================================
%% Private
%% ===================================================================

cache_bytes(#cache{datum_index = DatumIndex} = State) -> cache_bytes(State, ets:info(DatumIndex, memory)).

cache_bytes(#cache{table_pad = TabPad}, Mem) -> (Mem - TabPad) * erlang:system_info(wordsize).

delete_datum(DatumIndex, Key) ->
  case ets:take(DatumIndex, Key) of
    [#datum{ttl_reaper = Reaper}] when is_pid(Reaper) -> exit(Reaper, kill);
    _ -> true
  end.

delete_object(DatumIndex, #datum{ttl_reaper = Reaper} = Datum) ->
  is_pid(Reaper) andalso exit(Reaper, kill),
  ets:delete_object(DatumIndex, Datum).

-compile({inline, [{create_datum, 4}]}).
create_datum(DatumKey, Data, TTL, Type) ->
  Timestamp = os:timestamp(),
  #datum{key = DatumKey, data = Data, started = Timestamp,
         ttl = TTL, remaining_ttl = TTL, type = Type,
         last_active = Timestamp}.

reap_after(EtsIndex, Key, LifeTTL) ->
  receive
    {update_ttl, NewTTL} -> reap_after(EtsIndex, Key, NewTTL)
  after
    LifeTTL -> ets:delete(EtsIndex, Key),
               exit(self(), kill)
  end.

launch_datum_ttl_reaper(_, _, #datum{remaining_ttl = unlimited} = Datum) ->
  Datum;
launch_datum_ttl_reaper(EtsIndex, Key, #datum{remaining_ttl = TTL} = Datum) ->
  Datum#datum{ttl_reaper = spawn_link(fun() -> reap_after(EtsIndex, Key, TTL) end)}.


-compile({inline, [{datum_error, 2}]}).
datum_error(How, What) -> {ecache_datum_error, {How, What}}.

launch_datum(Key, EtsIndex, Module, Accessor, TTL, Policy) ->
    try Module:Accessor(Key) of
        CacheData ->
            UseKey = key(Key),
            Datum = create_datum(UseKey, CacheData, TTL, Policy),
            LaunchedDatum = launch_datum_ttl_reaper(EtsIndex, UseKey, Datum),
            ets:insert(EtsIndex, LaunchedDatum),
            CacheData
    catch
        How:What -> datum_error({How, What}, erlang:get_stacktrace())
    end.

launch_memoize_datum(Key, EtsIndex, Module, Accessor, TTL, Policy) ->
    try Module:Accessor(Key) of
        CacheData ->
            UseKey = key(Module, Accessor, Key),
            Datum = create_datum(UseKey, CacheData, TTL, Policy),
            LaunchedDatum = launch_datum_ttl_reaper(EtsIndex, UseKey, Datum),
            ets:insert(EtsIndex, LaunchedDatum),
            CacheData
    catch
        How:What -> datum_error({How, What}, erlang:get_stacktrace())
    end.

-compile({inline, [{data_from_datum, 1}]}).
data_from_datum(#datum{data = Data}) -> Data.

-compile({inline, [{ping_reaper, 2}]}).
ping_reaper(Reaper, NewTTL) when is_pid(Reaper) ->
  Reaper ! {update_ttl, NewTTL};
ping_reaper(_, _) -> ok.

update_ttl(DatumIndex, #datum{key = Key, ttl = unlimited}) ->
  ets:update_element(DatumIndex, Key, {#datum.last_active, os:timestamp()});
update_ttl(DatumIndex, #datum{key = Key, started = Started, ttl = TTL,
                  type = actual_time, ttl_reaper = Reaper}) ->
  Timestamp = os:timestamp(),
  % Get total time in seconds this datum has been running.  Convert to ms.
  % If we are less than the TTL, update with TTL-used (TTL in ms too)
  % else, we ran out of time.  expire on next loop.
  TTLRemaining = case timer:now_diff(Timestamp, Started) div 1000 of
                     StartedNowDiff when StartedNowDiff < TTL -> TTL - StartedNowDiff;
                     _ -> 0
                 end,
  ping_reaper(Reaper, TTLRemaining),
  ets:update_element(DatumIndex, Key, [{#datum.last_active, Timestamp}, {#datum.remaining_ttl, TTLRemaining}]);
update_ttl(DatumIndex, #datum{key = Key, ttl = TTL, ttl_reaper = Reaper}) ->
  ping_reaper(Reaper, TTL),
  ets:update_element(DatumIndex, Key, {#datum.last_active, os:timestamp()}).

fetch_data(Key, DatumIndex) when is_tuple(Key) ->
  case ets:lookup(DatumIndex, Key) of
    [Datum] -> update_ttl(DatumIndex, Datum),
               data_from_datum(Datum);
         [] -> {ecache, notfound}
  end.

replace_datum(Key, Data, DatumIndex) when is_tuple(Key) ->
  ets:update_element(DatumIndex, Key, [{#datum.data, Data}, {#datum.last_active, os:timestamp()}]),
  Data.

get_all_keys(EtsIndex) ->
  get_all_keys(EtsIndex, ets:first(EtsIndex), []).

get_all_keys(_, '$end_of_table', Accum) ->
  Accum;
get_all_keys(EtsIndex, NextKey, Accum) ->
  get_all_keys(EtsIndex, ets:next(EtsIndex, NextKey), [NextKey | Accum]).

%% ===================================================================
%% Data Abstraction
%% ===================================================================
