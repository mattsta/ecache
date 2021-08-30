-module(ecache).

-export([cache_sup/4, cache_ttl_sup/5]).
-export([dirty/2, dirty/3, dirty_memoize/4, empty/1, get/2, memoize/4]).
-export([stats/1, total_size/1]).
-export([rand/2, rand_keys/2]).

-define(TIMEOUT, infinity).

%% ===================================================================
%% Supervisory helpers
%% ===================================================================

cache_sup(Name, Mod, Fun, Opts) ->
    {Name, {ecache_server, start_link, [Name, Mod, Fun, Opts]}, permanent, brutal_kill, worker, [ecache_server]}.

cache_ttl_sup(Name, Mod, Fun, Size, TTL) ->
    {Name, {ecache_server, start_link, [Name, Mod, Fun, Size, TTL]}, permanent, brutal_kill, worker, [ecache_server]}.

%% ===================================================================
%% Calls into ecache_server
%% ===================================================================

get(Name, Key) -> get_value(Name, {get, Key}).

memoize(MemoizeCacheServer, Module, Fun, Key) -> get_value(MemoizeCacheServer, {generic_get, Module, Fun, Key}).

dirty_memoize(MemoizeCacheServer, Module, Fun, Key) ->
    gen_server:cast(MemoizeCacheServer, {generic_dirty, Module, Fun, Key}).

empty(Name) -> gen_server:call(Name, empty).

total_size(Name) -> gen_server:call(Name, total_size).

stats(Name) -> gen_server:call(Name, stats).

dirty(Name, Key, NewData) -> gen_server:cast(Name, {dirty, Key, NewData}).

dirty(Name, Key) -> gen_server:cast(Name, {dirty, Key}).

rand(Name, Count) -> gen_server:call(Name, {rand, data, Count}).

rand_keys(Name, Count) -> gen_server:call(Name, {rand, keys, Count}).

get_value(Name, Req) ->
    case gen_server:call(Name, Req, ?TIMEOUT) of
        {ok, Data} -> Data;
        {error, Error} -> Error;
        {ecache, Launcher} ->
            Ref = monitor(process, Launcher),
            receive
                {'DOWN', Ref, process, _, {ok, Data}} ->
                    gen_server:cast(Name, found),
                    Data;
                {'DOWN', Ref, process, _, {error, Error}} -> Error;
                {'DOWN', Ref, process, _, _noproc} -> get_value(Name, Req)
            end
    end.
