-module(ecache_server).

-behaviour(gen_server).

-export([start_link/3, start_link/4, start_link/5, start_link/6]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(cache, {name, datum_index, data_module, 
                reaper_pid, data_accessor, cache_size,
                cache_policy, default_ttl}).

-record(datum, {key, mgr, data, started, ttl_reaper = nil,
                last_active, ttl, type = mru, remaining_ttl}).

% make 8 MB cache
start_link(Name, Mod, Fun) ->
  start_link(Name, Mod, Fun, 8).

% make 5 minute expiry cache
start_link(Name, Mod, Fun, CacheSize) ->
  start_link(Name, Mod, Fun, CacheSize, 300000).

% make MRU policy cache
start_link(Name, Mod, Fun, CacheSize, CacheTime) ->
  start_link(Name, Mod, Fun, CacheSize, CacheTime, mru).

start_link(Name, Mod, Fun, CacheSize, CacheTime, CachePolicy) ->
  gen_server:start_link({local, Name}, 
    ?MODULE, [Name, Mod, Fun, CacheSize, CacheTime, CachePolicy], []).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_server
%%%----------------------------------------------------------------------

init([Name, Mod, Fun, CacheSize, CacheTime, CachePolicy]) ->
  DatumIndex = ets:new(Name, [set,
                              compressed,  % yay compression
                              public,      % public because we spawn writers
                              {keypos, 2}, % use Key stored in record
                              {read_concurrency, true}]),
  case CacheSize of
    unlimited -> ReaperPid = nil, CacheSizeBytes = unlimited;
            _ -> CacheSizeBytes = CacheSize*1024*1024, 
                 {ok, ReaperPid} = ecache_reaper:start(Name, CacheSizeBytes),
                 erlang:monitor(process, ReaperPid)
  end,

  State = #cache{name = Name,
                 datum_index = DatumIndex,
                 data_module = Mod,
                 data_accessor = Fun,
                 reaper_pid = ReaperPid,
                 default_ttl = CacheTime,
                 cache_policy = CachePolicy,
                 cache_size = CacheSizeBytes},
  {ok, State}.

locate(DatumKey, #cache{datum_index = DatumIndex, data_module = DataModule,
                  default_ttl = DefaultTTL, cache_policy = Policy,
                  data_accessor = DataAccessor} = State) ->
  case fetch_data(key(DatumKey), State) of 
    {ecache, notfound} -> Data = launch_datum(DatumKey, DatumIndex, DataModule,
                                              DataAccessor, DefaultTTL, Policy),
                          {launched, Data};
    Data -> {found, Data}
  end.

locate_memoize(DatumKey, DatumIndex, DataModule,
               DataAccessor, DefaultTTL, Policy, State) ->
  case fetch_data(key(DataModule, DataAccessor, DatumKey), State) of
    {ecache, notfound} -> Data = launch_memoize_datum(DatumKey,
                                   DatumIndex, DataModule,
                                   DataAccessor, DefaultTTL, Policy),
                          {launched, Data};
    Data -> {found, Data}
  end.

handle_call({generic_get, M, F, Key}, From, #cache{datum_index = DatumIndex,
    data_module = _DataModule,
    default_ttl = DefaultTTL,
    cache_policy = Policy,
    data_accessor = _DataAccessor} = State) ->
%    io:format("Requesting: ~p:~p(~p)~n", [M, F, Key]),
  spawn(fun() ->
          Reply = 
            case locate_memoize(Key, DatumIndex, M, F,
                                DefaultTTL, Policy, State) of
              {_, Data} -> Data
            end,
          gen_server:reply(From, Reply)
        end),
  {noreply, State};

handle_call({get, Key}, From, #cache{datum_index = _DatumIndex} = State) ->
%    io:format("Requesting: (~p)~n", [Key]),
  spawn(fun() ->
          Reply = 
          case locate(Key, State) of
            {_, Data} -> Data
          end,
          gen_server:reply(From, Reply)
        end),
  {noreply, State};

% NB: total_size using ETS includes ETS overhead.  An empty table still
% has a size.
handle_call(total_size, _From, #cache{datum_index = DatumIndex} = State) ->
  TableBytes = ets:info(DatumIndex, memory) * erlang:system_info(wordsize),
  {reply, TableBytes, State};

handle_call(stats, _From, #cache{datum_index = DatumIndex} = State) ->
  EtsInfo = ets:info(DatumIndex),
  CacheName = proplists:get_value(name, EtsInfo),
  DatumCount = proplists:get_value(size, EtsInfo),
  Bytes = proplists:get_value(memory, EtsInfo) * erlang:system_info(wordsize),
  Stats = [{cache_name, CacheName},
           {memory_size_bytes, Bytes},
           {datum_count, DatumCount}],
  {reply, Stats, State};

handle_call(empty, _From, #cache{datum_index = DatumIndex} = State) ->
  ets:delete_all_objects(DatumIndex),
  {reply, ok, State};

handle_call(reap_oldest, _From, #cache{datum_index = DatumIndex} = State) ->
  LeastActive =
    ets:foldl(fun(A, Acc) when A#datum.last_active < Acc -> A;
                 (_, Acc) -> Acc
              end,
              now(),
              DatumIndex),
  ets:delete(DatumIndex, LeastActive),
  {from, ok, State};

handle_call({rand, Type, Count}, From, 
  #cache{datum_index = DatumIndex} = State) ->
  spawn(fun() ->
          AllKeys = get_all_keys(DatumIndex),
          Length = length(AllKeys),
          FoundData = 
          case Length =< Count of
            true  -> case Type of
                       data -> [fetch_data(P, State) || P <- AllKeys];
                       keys -> [unkey(K) || K <- AllKeys]
                     end;
            false ->  RandomSet  = [crypto:rand_uniform(1, Length) || 
                                      _ <- lists:seq(1, Count)],
                      RandomKeys = [lists:nth(Q, AllKeys) || Q <- RandomSet],
                      case Type of
                        data -> [fetch_data(P, State) || P <- RandomKeys];
                        keys -> [unkey(K) || K <- RandomKeys]
                      end
          end,
          gen_server:reply(From, FoundData)
        end),
  {noreply, State};
  
handle_call(Arbitrary, _From, State) ->
  {reply, {arbitrary, Arbitrary}, State}.

handle_cast({dirty, Id, NewData}, State) ->
  replace_datum(key(Id), NewData, State),
  {noreply, State};

handle_cast({dirty, Id}, #cache{datum_index = DatumIndex} = State) ->
  ets:delete(DatumIndex, key(Id)),
  {noreply, State};

handle_cast({generic_dirty, M, F, A}, 
    #cache{datum_index = DatumIndex} = State) ->
  ets:delete(DatumIndex, key(M, F, A)),
  {noreply, State}.

terminate(_Reason, _State) ->
    ok.

handle_info({destroy,_DatumPid, ok}, State) ->
  {noreply, State};

handle_info({'DOWN', _Ref, process, ReaperPid, _Reason}, 
    #cache{reaper_pid = ReaperPid, name = Name, cache_size = Size} = State) ->
  {NewReaperPid, _Mon} = ecache_reaper:start_link(Name, Size),
  {noreply, State#cache{reaper_pid = NewReaperPid}};

handle_info({'DOWN', _Ref, process, _Pid, _Reason}, State) ->
  {noreply, State};

handle_info(Info, State) ->
  io:format("Other info of: ~p~n", [Info]),
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

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
unkey({ecache_multi, {M, F, A}}) -> {M, F, A}.

%% ===================================================================
%% Private
%% ===================================================================

-compile({inline, [{create_datum, 4}]}).
create_datum(DatumKey, Data, TTL, Type) ->
  #datum{key = DatumKey, data = Data, started = now(),
         ttl = TTL, remaining_ttl = TTL, type = Type,
         last_active = now()}.

reap_after(EtsIndex, Key, LifeTTL) ->
  receive
    {update_ttl, NewTTL} -> reap_after(EtsIndex, Key, NewTTL)
  after
    LifeTTL -> ets:delete(EtsIndex, Key)
  end.

launch_datum_ttl_reaper(_, _, #datum{remaining_ttl = unlimited} = Datum) ->
  Datum;
launch_datum_ttl_reaper(EtsIndex, Key, #datum{remaining_ttl = TTL} = Datum) ->
  Reaper = spawn_link(fun() -> reap_after(EtsIndex, Key, TTL) end),
  Datum#datum{ttl_reaper = Reaper}.


-compile({inline, [{datum_error, 2}]}).
datum_error(How, What) -> {ecache_datum_error, {How, What}}.

launch_datum(Key, EtsIndex, Module, Accessor, TTL, CachePolicy) ->
  try Module:Accessor(Key) of
    CacheData -> UseKey = key(Key),
                 Datum = create_datum(UseKey, CacheData, TTL, CachePolicy),
                 LaunchedDatum =
                   launch_datum_ttl_reaper(EtsIndex, UseKey, Datum),
                 ets:insert(EtsIndex, LaunchedDatum),
                 CacheData
  catch
    How:What -> datum_error(How, What)
  end.

launch_memoize_datum(Key, EtsIndex, Module, Accessor, TTL, CachePolicy) ->
  try Module:Accessor(Key) of
    CacheData -> UseKey = key(Module, Accessor, Key),
                 Datum = create_datum(UseKey, CacheData, TTL, CachePolicy),
                 LaunchedDatum =
                   launch_datum_ttl_reaper(EtsIndex, UseKey, Datum),
                 ets:insert(EtsIndex, LaunchedDatum),
                 CacheData
  catch
    How:What -> datum_error(How, What)
  end.


-compile({inline, [{data_from_datum, 1}]}).
data_from_datum(#datum{data = Data}) -> Data.

-compile({inline, [{ping_reaper, 2}]}).
ping_reaper(Reaper, NewTTL) when is_pid(Reaper) ->
  Reaper ! {update_ttl, NewTTL};
ping_reaper(_, _) -> ok.

update_ttl(DatumIndex, #datum{key = Key, ttl = unlimited}) ->
  NewNow = {#datum.last_active, now()},
  ets:update_element(DatumIndex, Key, NewNow);
update_ttl(DatumIndex, #datum{key = Key, started = Started, ttl = TTL,
                  type = actual_time, ttl_reaper = Reaper}) ->
  % Get total time in seconds this datum has been running.  Convert to ms.
  StartedNowDiff = (calendar:time_to_seconds(now()) - 
                    calendar:time_to_seconds(Started)) * 1000,
  % If we are less than the TTL, update with TTL-used (TTL in ms too)
  % else, we ran out of time.  expire on next loop.
  TTLRemaining = if
                   StartedNowDiff < TTL -> TTL - StartedNowDiff;
                                   true -> 0
                 end,

  ping_reaper(Reaper, TTLRemaining),
  NewNowTTL = [{#datum.last_active, now()},
               {#datum.remaining_ttl, TTLRemaining}],
  ets:update_element(DatumIndex, Key, NewNowTTL);
update_ttl(DatumIndex, #datum{key = Key, ttl = TTL, ttl_reaper = Reaper}) ->
  ping_reaper(Reaper, TTL),
  ets:update_element(DatumIndex, Key, {#datum.last_active, now()}).

fetch_data(Key, #cache{datum_index = DatumIndex}) when is_tuple(Key) ->
  case ets:lookup(DatumIndex, Key) of
    [Datum] -> update_ttl(DatumIndex, Datum),
               data_from_datum(Datum);
         [] -> {ecache, notfound}
  end.

replace_datum(Key, Data, #cache{datum_index = DatumIndex}) when is_tuple(Key) ->
  NewDataActive = [{#datum.data, Data}, {#datum.last_active, now()}],
  ets:update_element(DatumIndex, Key, NewDataActive),
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

