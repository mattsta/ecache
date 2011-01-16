ecache: Erlang ETS Based TTL Cache
==================================

ecache stores your cache items in ets.  Each cache item gets its own monitoring
process to auto-delete the cache item when the TTL expires.

ecache has the API of http://github.com/mattsta/pcache but stores data in ets
instead of storing data in individual processes.

Usage
-----
The cache server is designed to memoize a specific Module:Fun. The key in
a cache is the Argument passed to Module:Fun/1.

Start a cache:
        CacheName = my_cache,
        M = database,
        F = get_result,
        Size = 16,     % 16 MB cache
        Time = 300000, % 300k ms = 300s = 5 minute TTL
        Server = ecache_server:start_link(CacheName, M, F, Size, Time).

The TTL is an idle timer TTL.  Each entry resets to the TTL when accessed.
A cache with a five minute TTL will expire an entry when nothing touches it for five minutes.
The entry timer resets to the TTL every time an item is read.  You need to dirty a key when 
the result of M:F(Key) will change from what is in the cache.

Use a cache:
        Result = ecache:get(my_cache, <<"bob">>).
        ecache:dirty(my_cache, <<"bob">>, <<"newvalue">>).  % replace entry for <<"bob">>
        ecache:dirty(my_cache, <<"bob">>).  % remove entry from cache
        ecache:empty(my_cache).  % remove all entries from cache
        RandomValues = ecache:rand(my_cache, 12).
        RandomKeys = ecache:rand_keys(my_cache, 12).

Bonus feature: use arbitrary M:F/1 calls in a cache:
        Result = ecache:memoize(my_cache, OtherMod, OtherFun, Arg).
        ecache:dirty_memoize(my_cache, OtherMod, OtherFun, Arg).  % remove entry from cache

`ecache:memoize/4` helps us get around annoying issues of one-cache-per-mod-fun.
Your root cache Mod:Fun could be nonsense if you want to use `ecache:memoize/4` everywhere.

Short-hand to make a supervisor entry:
       SupervisorWorkerTuple = ecache:cache_sup(Name, M, F, Size).

For more examples, see https://github.com/mattsta/ecache/blob/master/test/ecache_tests.erl


Status
------
ecache is pcache but converted to use ets instead of processes.  It has not
been tested extensively in production environments yet.

Building
--------
        rebar compile

Testing
-------
        rebar eunit suite=ecache

TODO
----
### Add tests for

* Other TTL variation
* Reaper

### Future features

* Cache pools?  Cross-server awareness?
* Expose per-entry TTL to external setting/updating
* Expose ETS configuration to make compression and read/write optimizations
settable per-cache.
