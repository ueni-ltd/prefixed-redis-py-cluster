from redis.client import list_or_args

from rediscluster.client import RedisCluster
from rediscluster.client import StrictRedisCluster
from rediscluster.exceptions import RedisClusterException
from rediscluster.pipeline import StrictClusterPipeline


class PrefixedStrictRedisCluster(StrictRedisCluster):

    def _set_key_prefix_variables(self, key_prefix):
        self.key_prefix = key_prefix
        self.key_prefix_with_version = f'{key_prefix}:1:'

        self.key_prefix_len = len(self.key_prefix)
        self.key_prefix_with_version_len = len(self.key_prefix_with_version)

    def __init__(self, *args, key_prefix, **kwargs):
        self._set_key_prefix_variables(key_prefix)
        super().__init__(*args, **kwargs)

    @staticmethod
    def need_to_add_version(key):
        return str(key)[0] != ':'

    def make_key(self, key):
        if self.need_to_add_version(key):
            return f'{self.key_prefix_with_version}{key}'

        return f'{self.key_prefix}{key}'

    def make_keys(self, keys):
        return [self.make_key(key) for key in keys]

    def clean_key(self, key, clean_version=False):
        if clean_version:
            return key[self.key_prefix_with_version_len:]

        return key[self.key_prefix_len:]

    def clean_keys(self, keys, clean_version=False):
        return [self.clean_key(key, clean_version=clean_version) for key in keys]

    def flushall(self):
        raise RedisClusterException("method PrefixedRedisCluster.flushall() is not implemented")

    def flushdb(self):
        raise RedisClusterException("method PrefixedRedisCluster.flushall() is not implemented")

    # BASIC KEY COMMANDS
    def append(self, key, value):
        return super().append(self.make_key(key), value)

    def bitcount(self, key, start=None, end=None):
        return super().bitcount(self.make_key(key), start=start, end=end)

    def bitop(self, operation, dest, *keys):
        return super().bitop(operation, self.make_key(dest), *self.make_keys(keys))

    def bitpos(self, key, bit, start=None, end=None):
        return super().bitpos(self.make_key(key), bit, start=start, end=end)

    def decr(self, name, amount=1):
        return super().decr(self.make_key(name), amount=amount)

    def delete(self, *names):
        return super().delete(*self.make_keys(names))

    def dump(self, name):
        return super().dump(self.make_key(name))

    def exists(self, name):
        return super().exists(self.make_key(name))
    __contains__ = exists

    def expire(self, name, time):
        return super().expire(self.make_key(name), time)

    def expireat(self, name, when):
        return super().expireat(self.make_key(name), when)

    def get(self, name):
        return super().get(self.make_key(name))

    def getbit(self, name, offset):
        return super().getbit(self.make_key(name), offset)

    def getrange(self, key, start, end):
        return super().getrange(self.make_key(key), start, end)

    def getset(self, name, value):
        return super().getset(self.make_key(name), value)

    def incr(self, name, amount=1):
        return super().incr(self.make_key(name), amount=amount)

    def incrbyfloat(self, name, amount=1.0):
        return super().incrbyfloat(self.make_key(name), amount=amount)

    def keys(self, pattern='*'):
        clean_version = self.need_to_add_version(pattern)
        return self.clean_keys(super().keys(pattern=self.make_key(pattern)), clean_version=clean_version)

    def move(self, name, db):
        return super().move(self.make_key(name), db)

    def persist(self, name):
        return super().persist(self.make_key(name))

    def pexpire(self, name, time):
        return super().pexpire(self.make_key(name), time)

    def pexpireat(self, name, when):
        return super().pexpire(self.make_key(name), when)

    def psetex(self, name, time_ms, value):
        return super().psetex(self.make_key(name), time_ms, value)

    def pttl(self, name):
        return super().pttl(self.make_key(name))

    def restore(self, name, ttl, value, replace=False):
        return super().restore(self.make_key(name), ttl, value, replace=replace)

    def set(self, name, value, ex=None, px=None, nx=False, xx=False):
        return super().set(self.make_key(name), value, ex=ex, px=px, nx=nx, xx=xx)

    def setbit(self, name, offset, value):
        return super().setbit(self.make_key(name), offset, value)

    def setex(self, name, time, value):
        return super().setex(self.make_key(name), time, value)

    def setnx(self, name, value):
        return super().setnx(self.make_key(name), value)

    def setrange(self, name, offset, value):
        return super().setrange(self.make_key(name), offset, value)

    def strlen(self, name):
        return super().strlen(self.make_key(name))

    def substr(self, name, start, end=-1):
        return super().substr(self.make_key(name), start, end=-1)

    def touch(self, *args):
        return super().touch(*self.make_keys(args))

    def ttl(self, name):
        return super().ttl(self.make_key(name))

    def type(self, name):
        return super().type(self.make_key(name))

    # LIST COMMANDS
    def _bpop(self, command, keys, timeout=0):
        clean_version = self.need_to_add_version(keys[0])
        result = command(self.make_keys(keys), timeout=timeout)

        if result is not None:
            return self.clean_key(result[0], clean_version), result[1]

        return result

    def blpop(self, keys, timeout=0):
        return self._bpop(super().blpop, keys, timeout=timeout)

    def brpop(self, keys, timeout=0):
        return self._bpop(super().brpop, keys, timeout=timeout)

    def brpoplpush(self, src, dst, timeout=0):
        return super().brpoplpush(self.make_key(src), self.make_key(dst), timeout=timeout)

    def lindex(self, name, index):
        return super().lindex(self.make_key(name), index)

    def linsert(self, name, where, refvalue, value):
        return super().linsert(self.make_key(name), where, refvalue, value)

    def llen(self, name):
        return super().llen(self.make_key(name))

    def lpop(self, name):
        return super().lpop(self.make_key(name))

    def lpush(self, name, *values):
        return super().lpush(self.make_key(name), *values)

    def lpushx(self, name, value):
        return super().lpushx(self.make_key(name), value)

    def lrange(self, name, start, end):
        return super().lrange(self.make_key(name), start, end)

    def lrem(self, name, count, value):
        return super().lrem(self.make_key(name), count, value)

    def lset(self, name, index, value):
        return super().lset(self.make_key(name), index, value)

    def ltrim(self, name, start, end):
        return super().ltrim(self.make_key(name), start, end)

    def rpop(self, name):
        return super().rpop(self.make_key(name))

    def rpoplpush(self, src, dst):
        return super().rpoplpush(self.make_key(src), self.make_key(dst))

    def rpush(self, name, *values):
        return super().rpush(self.make_key(name), *values)

    def rpushx(self, name, value):
        return super().rpush(self.make_key(name), value)

    def sort(self, name, start=None, num=None, by=None, get=None, desc=False, alpha=False, store=None, groups=False):
        return super().sort(
            self.make_key(name), start=start, num=num, by=by, get=get,
            desc=desc, alpha=alpha, store=store, groups=groups
        )

    # SCAN COMMANDS
    def scan(self, cursor=0, match=None, count=None):
        match = match or '*'
        return super().scan(cursor=cursor, match=self.make_key(match), count=count)

    def scan_iter(self, match=None, count=None):
        match = match or '*'
        return super().scan_iter(match=self.make_key(match), count=count)

    def sscan(self, name, cursor=0, match=None, count=None):
        match = match or '*'
        return super().sscan(self.make_key(name), cursor=cursor, match=self.make_key(match), count=count)

    def hscan(self, name, cursor=0, match=None, count=None):
        match = match or '*'
        return super().hscan(self.make_key(name), cursor=cursor, match=self.make_key(match), count=count)

    def zscan(self, name, cursor=0, match=None, count=None, score_cast_func=float):
        match = match or '*'
        return super().zscan(
            self.make_key(name), cursor=cursor, match=self.make_key(match), count=count, score_cast_func=score_cast_func
        )

    # SET COMMANDS
    def sadd(self, name, *values):
        return super().sadd(self.make_key(name), *values)

    def scard(self, name):
        return super().scard(self.make_key(name))

    def sdiff(self, keys, *args):
        keys = list_or_args(keys, args)
        return super().sdiff(self.make_keys(keys))

    def sdiffstore(self, dest, keys, *args):
        keys = list_or_args(keys, args)
        return super().sdiffstore(self.make_key(dest), self.make_keys(keys))

    def sinter(self, keys, *args):
        keys = list_or_args(keys, args)
        return super().sinter(self.make_keys(keys))

    def sinterstore(self, dest, keys, *args):
        keys = list_or_args(keys, args)
        return super().sinterstore(self.make_key(dest), self.make_keys(keys))

    def sismember(self, name, value):
        return super().sismember(self.make_key(name), value)

    def smembers(self, name):
        return super().smembers(self.make_key(name))

    def smove(self, src, dst, value):
        return super().smove(self.make_key(src), self.make_key(dst), value)

    def spop(self, name):
        return super().spop(self.make_key(name))

    def srandmember(self, name, number=None):
        return super().srandmember(self.make_key(name), number=number)

    def srem(self, name, *values):
        return super().srem(self.make_key(name), *values)

    def sunion(self, keys, *args):
        keys = list_or_args(keys, args)
        return super().sunion(self.make_keys(keys))

    def sunionstore(self, dest, keys, *args):
        keys = list_or_args(keys, args)
        return super().sunion(self.make_key(dest), self.make_keys(keys))

    # SORTED SET COMMANDS
    def zadd(self, name, *args, **kwargs):
        return super().zadd(self.make_key(name), *args, **kwargs)

    def zcard(self, name):
        return super().zcard(self.make_key(name))

    def zcount(self, name, min, max):
        return super().zcount(self.make_key(name), min, max)

    def zincrby(self, name, value, amount=1):
        return super().zincrby(self.make_key(name), value, amount=amount)

    def zinterstore(self, dest, keys, aggregate=None):
        return super().zinterstore(self.make_key(dest), self.make_keys(keys), aggregate=aggregate)

    def zlexcount(self, name, min, max):
        return super().zlexcount(self.make_key(name), min, max)

    def zrange(self, name, start, end, desc=False, withscores=False, score_cast_func=float):
        return super().zrange(
            self.make_key(name), start, end, desc=desc, withscores=withscores, score_cast_func=score_cast_func
        )

    def zrangebylex(self, name, min, max, start=None, num=None):
        return super().zrangebylex(self.make_key(name), min, max, start=start, num=num)

    def zrevrangebylex(self, name, max, min, start=None, num=None):
        return super().zrevrangebylex(self.make_key(name), max, min, start=start, num=num)

    def zrangebyscore(self, name, min, max, start=None, num=None, withscores=False, score_cast_func=float):
        return super().zrangebyscore(
            self.make_key(name), min, max, start=start, num=num, withscores=withscores, score_cast_func=score_cast_func
        )

    def zrank(self, name, value):
        return super().zrank(self.make_key(name), value)

    def zrem(self, name, *values):
        return super().zrem(self.make_key(name), *values)

    def zremrangebylex(self, name, min, max):
        return super().zremrangebylex(self.make_key(name), min, max)

    def zremrangebyrank(self, name, min, max):
        return super().zremrangebyrank(self.make_key(name), min, max)

    def zremrangebyscore(self, name, min, max):
        return super().zremrangebyscore(self.make_key(name), min, max)

    def zrevrange(self, name, start, end, withscores=False, score_cast_func=float):
        return super().zrevrange(
            self.make_key(name), start, end, withscores=withscores, score_cast_func=score_cast_func
        )

    def zrevrangebyscore(self, name, max, min, start=None, num=None, withscores=False, score_cast_func=float):
        return super().zrevrangebyscore(
            self.make_key(name), max, min, start=start, num=num, withscores=withscores, score_cast_func=score_cast_func
        )

    def zrevrank(self, name, value):
        return super().zrevrank(self.make_key(name), value)

    def zscore(self, name, value):
        return super().zscore(self.make_key(name), value)

    def zunionstore(self, dest, keys, aggregate=None):
        return super().zunionstore(self.make_key(dest), self.make_keys(keys), aggregate=aggregate)

    # HYPERLOGLOG COMMANDS
    def pfadd(self, name, *values):
        return super().pfadd(self.make_key(name), *values)

    def pfcount(self, *sources):
        return super().pfcount(*self.make_keys(sources))

    def pfmerge(self, dest, *sources):
        return super().pfmerge(self.make_key(dest), *self.make_keys(sources))

    # HASH COMMANDS
    def hdel(self, name, *keys):
        return super().hdel(self.make_key(name), *keys)

    def hexists(self, name, key):
        return super().hexists(self.make_key(name), key)

    def hget(self, name, key):
        return super().hget(self.make_key(name), key)

    def hgetall(self, name):
        return super().hgetall(self.make_key(name))

    def hincrby(self, name, key, amount=1):
        return super().hincrby(self.make_key(name), key, amount=amount)

    def hincrbyfloat(self, name, key, amount=1.0):
        return super().hincrbyfloat(self.make_key(name), key, amount=amount)

    def hkeys(self, name):
        return super().hkeys(self.make_key(name))

    def hlen(self, name):
        return super().hlen(self.make_key(name))

    def hset(self, name, key, value):
        return super().hset(self.make_key(name), key, value)

    def hsetnx(self, name, key, value):
        return super().hsetnx(self.make_key(name), key, value)

    def hmset(self, name, mapping):
        return super().hmset(self.make_key(name), mapping)

    def hmget(self, name, keys, *args):
        keys = list_or_args(keys, args)
        return super().hmget(self.make_key(name), self.make_keys(keys))

    def hvals(self, name):
        return super().hvals(self.make_key(name))

    def hstrlen(self, name, key):
        return super().hstrlen(self.make_key(name), key)

    # GEO COMMANDS
    def geoadd(self, name, *values):
        return super().geoadd(self.make_key(name), *values)

    def geodist(self, name, place1, place2, unit=None):
        return super().geodist(self.make_key(name), place1, place2, unit=unit)

    def geohash(self, name, *values):
        return super().geohash(self.make_key(name), *values)

    def geopos(self, name, *values):
        return super().geopos(self.make_key(name), *values)

    def georadius(self, name, longitude, latitude, radius, unit=None,
                  withdist=False, withcoord=False, withhash=False, count=None,
                  sort=None, store=None, store_dist=None):
        return super().georadius(
            self.make_key(name), longitude, latitude, radius, unit=unit, withdist=withdist, withcoord=withcoord,
            withhash=withhash, count=count, sort=sort, store=store, store_dist=store_dist
        )

    def georadiusbymember(self, name, member, radius, unit=None,
                          withdist=False, withcoord=False, withhash=False,
                          count=None, sort=None, store=None, store_dist=None):
        return super().georadiusbymember(
            self.make_key(name), member, radius, unit=unit, withdist=withdist, withcoord=withcoord,
            withhash=withhash, count=count, sort=sort, store=store, store_dist=store_dist
        )


class PrefixedStrictClusterPipeline(StrictClusterPipeline, PrefixedStrictRedisCluster):

    def __init__(self, *args, key_prefix, **kwargs):
        self._set_key_prefix_variables(key_prefix)
        super().__init__(*args, **kwargs)


class PrefixedRedisCluster(PrefixedStrictRedisCluster, RedisCluster):

    def pipeline(self, transaction=None, shard_hint=None):
        if shard_hint:
            raise RedisClusterException("shard_hint is deprecated in cluster mode")

        if transaction:
            raise RedisClusterException("transaction is deprecated in cluster mode")

        return PrefixedStrictClusterPipeline(
            connection_pool=self.connection_pool,
            startup_nodes=self.connection_pool.nodes.startup_nodes,
            response_callbacks=self.response_callbacks,
            key_prefix=self.key_prefix,
        )
