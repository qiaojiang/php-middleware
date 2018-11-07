<?php
namespace app\common\lib;

use yii\db\Connection;

class CRedis extends Connection
{
    /**
     * @var Redis
     */
    private $redis;

    public $server;

    public function init()
    {
        $this->getRedis();
        $this->connect($this->server['host'], $this->server['port'], $this->server['timeout'], $this->server['password']);
	}

    public function setServer($server)
    {
        $this->server = $server;
    }

    public function getRedis()
    {
        if($this->redis!==null)
            return $this->redis;
        else
            return $this->redis = new \Redis();
    }

    public function connect($host, $port=6379, $timeout=false, $password=null)
    {
        try {
            if ($host{0} == '/') {//unix domain socket
                $this->redis->connect($host);
            }
            else {
                if ($timeout) {
                    $this->redis->connect($host, $port, $timeout);
                }
                else {
                    $this->redis->connect($host, $port);
                }
            }
            if(!empty($password)) {
                $this->auth($password);
            }
            return true;
        }
        catch(\Exception $e) {
            return false;
        }
    }

    public function close()
    {
        return $this->redis->close();
    }

    public function setOption($param1, $param2)
    {
        return $this->redis->setOption($param1, $param2);
    }

    public function getOption($param)
    {
        return $this->redis->getOption($param);
    }

    public function ping()
    {
        return $this->redis->ping();
    }

    public function get($key)
    {
        return $this->redis->get($key);
    }

    public function set($key, $value)
    {
        return $this->redis->set($key, $value);
    }

    public function setex($key, $ttl, $value)
    {
        return $this->redis->setex($key, $ttl, $value);
    }

    public function setnx($key, $value)
    {
        return $this->redis->setnx($key, $value);
    }

    public function delete($keys)
    {
        return $this->redis->delete($keys);
    }

    public function del($keys)
    {
        return $this->redis->del($keys);
    }

    public function multi()
    {
        return $this->redis->multi();
    }

    public function discard()
    {
        return $this->redis->discard();
    }

    public function watch($keys)
    {
        return $this->redis->watch($keys);
    }

    public function unwatch()
    {
        return $this->redis->unwatch();
    }

    public function subscribe($channels, $callback)
    {
        return $this->redis->subscribe($channels, $callback);
    }

    public function publish($channel, $message)
    {
        return $this->redis->publish($channel, $message);
    }

    public function exists($key)
    {
        return $this->redis->exists($key);
    }

    public function incr($key)
    {
        return $this->redis->incr($key);
    }

    public function incrBy($key, $value)
    {
        return $this->redis->incrBy($key, $value);
    }

    public function decr($key)
    {
        return $this->redis->decr($key);
    }

    public function decrBy($key, $value)
    {
        return $this->redis->decrBy($key, $value);
    }

    public function getMultiple($keys)
    {
        return $this->redis->getMultiple($keys);
    }

    public function lPush($key, $value)
    {
        return $this->redis->lPush($key, $value);
    }

    public function rPush($key, $value)
    {
        return $this->redis->rPush($key, $value);
    }

    public function lPushx($key, $value)
    {
        return $this->redis->lPushx($key, $value);
    }

    public function rPushx($key, $value)
    {
        return $this->redis->rPushx($key, $value);
    }

    public function lPop($key)
    {
        return $this->redis->lPop($key);
    }

    public function rPop($key)
    {
        return $this->redis->rPop($key);
    }

    public function blPop($keys, $timeout)
    {
        return $this->redis->blPop($keys, $timeout);
    }

    public function brPop($keys, $timeout)
    {
        return $this->redis->brPop($keys, $timeout);
    }

    public function lSize($key)
    {
        return $this->redis->lSize($key);
    }

    public function lIndex($key, $index)
    {
        return $this->redis->lIndex($key, $index);
    }

    public function lGet($key, $index)
    {
        return $this->redis->lGet($key, $index);
    }

    public function lSet($key, $index, $value)
    {
        return $this->redis->lSet($key, $index, $value);
    }

    public function lRange($key, $start, $end)
    {
        return $this->redis->lRange($key, $start, $end);
    }

    public function lGetRange($key, $start, $end)
    {
        return $this->redis->lGetRange($key, $start, $end);
    }

    public function lTrim($key, $start, $stop)
    {
        return $this->redis->lTrim($key, $start, $stop);
    }

    public function listTrim($key, $start, $stop)
    {
        return $this->redis->listTrim($key, $start, $stop);
    }

    public function lRem($key, $value, $count)
    {
        return $this->redis->lRem($key, $value, $count);
    }

    public function lRemove($key, $value, $count)
    {
        return $this->redis->lRemove($key, $value, $count);
    }

    public function lInsert($key, $position, $pivot, $value)
    {
        return $this->redis->lInsert($key, $position, $pivot, $value);
    }

    public function sAdd($key, $value)
    {
        return $this->redis->sAdd($key, $value);
    }

    public function sRem($key, $member)
    {
        return $this->redis->sRem($key, $member);
    }

    public function sRemove($key, $member)
    {
        return $this->redis->sRemove($key, $member);
    }

    public function sMove($srcKey, $dstKey, $member)
    {
        return $this->redis->sMove($srcKey, $dstKey, $member);
    }

    public function sIsMember($key, $value)
    {
        return $this->redis->sIsMember($key, $value);
    }

    public function sContains($key, $value)
    {
        return $this->redis->sContains($key, $value);
    }

    public function sCard($key)
    {
        return $this->redis->sCard($key);
    }

    public function sSize($key)
    {
        return $this->redis->sCard($key);
    }

    public function sPop($key)
    {
        return $this->redis->sPop($key);
    }

    public function sRandMember($key)
    {
        return $this->redis->sRandMember($key);
    }

    public function sInter($keys)
    {
        if (is_array($keys)) {
            return $this->redis->sInter(implode(',', $keys));
        }
        return $this->redis->sInter($keys);
    }

    public function sInterStore($dstKey, $srcKeys)
    {
        if (is_array($srcKeys)) {
            return $this->redis->sInterStore($dstKey, implode(',', $srcKeys));
        }
        return $this->redis->sInterStore($dstKey, $srcKeys);
    }

    public function sUnion($keys)
    {
        if (is_array($keys)) {
            return $this->redis->sUnion(implode(',', $keys));
        }
        return $this->redis->sUnion($keys);
    }

    public function sUnionStore($dstKey, $keys)
    {
        if (is_array($keys)) {
            return $this->redis->sUnionStore($dstKey, implode(',', $keys));
        }
        return $this->redis->sUnionStore($dstKey, $keys);
    }

    public function sDiff($keys)
    {
        if (is_array($keys)) {
            return $this->redis->sDiff(implode(',', $keys));
        }
        return $this->redis->sDiff($keys);
    }

    public function sDiffStore($dstKey, $keys)
    {
        if (is_array($keys)) {
            return $this->redis->sDiffStore($dstKey, implode(',', $keys));
        }
        return $this->redis->sDiffStore($dstKey, $keys);
    }

    public function sMembers($key)
    {
        return $this->redis->sMembers($key);
    }

    public function sGetMembers($key)
    {
        return $this->redis->sGetMembers($key);
    }

    public function getSet($key, $value)
    {
        return $this->redis->getSet($key, $value);
    }

    public function randomKey()
    {
        return $this->redis->randomKey();
    }

    public function select($index)
    {
        return $this->redis->select($index);
    }

    public function move($key, $dbindex)
    {
        return $this->redis->move($key, $dbindex);
    }

    public function rename($srcKey, $dstKey)
    {
        return $this->redis->rename($srcKey, $dstKey);
    }

    public function renameKey($srcKey, $dstKey)
    {
        return $this->redis->renameKey($srcKey, $dstKey);
    }

    public function renameNx($srcKey, $dstKey)
    {
        return $this->redis->renameNx($srcKey, $dstKey);
    }

    public function setTimeout($key, $ttl)
    {
        return $this->redis->setTimeout($key, $ttl);
    }

    public function expire($key, $ttl)
    {
        return $this->redis->expire($key, $ttl);
    }

    public function expireAt($key, $time)
    {
        return $this->redis->expireAt($key, $time);
    }

    public function keys($strKey)
    {
        return $this->redis->keys($strKey);
    }

    public function getKeys($strKey)
    {
        return $this->redis->getKeys($strKey);
    }

    public function dbSize()
    {
        return $this->redis->dbSize();
    }

    public function auth($pwd)
    {
        return $this->redis->auth($pwd);
    }

    public function bgrewriteaof()
    {
        return $this->redis->bgrewriteaof();
    }

    public function slaveof($host, $port=6379)
    {
        return $this->redis->slaveof($host, $port);
    }

    public function object($info, $str)
    {
        return $this->redis->object($info, $str);
    }

    public function save()
    {
        return $this->redis->save();
    }

    public function bgsave()
    {
        return $this->redis->bgsave();
    }

    public function lastSave()
    {
        return $this->redis->lastSave();
    }

    public function type($key)
    {
        return $this->redis->type($key);
    }

    public function append($key, $value)
    {
        return $this->redis->append($key, $value);
    }

    public function getRange($key, $start, $end)
    {
        return $this->redis->getRange($key, $start, $end);
    }

    public function setRange($key, $offset, $value)
    {
        return $this->redis->setRange($key, $offset, $value);
    }

    public function strlen($key)
    {
        return $this->redis->strlen($key);
    }

    public function getBit($key, $offset)
    {
        return $this->redis->getBit($key, $offset);
    }

    public function setBit($key, $offset, $value)
    {
        return $this->redis->setBit($key, $offset, $value);
    }

    public function flushDB()
    {
        return $this->redis->flushDB();
    }

    public function flushAll()
    {
        return $this->redis->flushAll();
    }

    public function sort($key, $options=NULL)
    {
        if (is_array($options)) {
            return $this->redis->sort($key, $options);
        }
        return $this->redis->sort($key);
    }

    public function info()
    {
        return $this->redis->info();
    }

    public function resetStat()
    {
        return $this->redis->resetStat();
    }

    public function ttl($key)
    {
        return $this->redis->ttl($key);
    }

    public function persist($key)
    {
        return $this->redis->persist($key);
    }

    public function mset($arr)
    {
        return $this->redis->mset($arr);
    }

    public function msetnx($arr)
    {
        return $this->redis->msetnx($arr);
    }

    public function rpoplpush($srcKey, $dstKey)
    {
        return $this->redis->rpoplpush($srcKey, $dstKey);
    }

    public function brpoplpush($srcKey, $dstKey)
    {
        return $this->redis->brpoplpush($srcKey, $dstKey);
    }

    public function zAdd($key, $score, $value)
    {
        return $this->redis->zAdd($key, $score, $value);
    }

    public function zRange($key, $start, $end)
    {
        return $this->redis->zRange($key, $start, $end);
    }

    public function zDelete($key, $member)
    {
        return $this->redis->zDelete($key, $member);
    }

    public function zRem($key, $member)
    {
        return $this->redis->zRem($key, $member);
    }

    public function zRevRange($key, $start, $end, $withScores = false)
    {
        return $this->redis->zRevRange($key, $start, $end, $withScores);
    }

    public function zRangeByScore($key, $start, $end, $options)
    {
        return $this->redis->zRangeByScore($key, $start, $end, $options);
    }

    public function zRevRangeByScore($key, $start, $end, $options)
    {
        return $this->redis->zRevRangeByScore($key, $start, $end, $options);
    }

    public function zCount($key, $start, $end)
    {
        return $this->redis->zCount($key, $start, $end);
    }

    public function zRemRangeByScore($key, $start, $end)
    {
        return $this->redis->zRemRangeByScore($key, $start, $end);
    }

    public function zDeleteRangeByScore($key, $start, $end)
    {
        return $this->redis->zDeleteRangeByScore($key, $start, $end);
    }

    public function zRemRangeByRank($key, $start, $end)
    {
        return $this->redis->zRemRangeByRank($key, $start, $end);
    }

    public function zDeleteRangeByRank($key, $start, $end)
    {
        return $this->redis->zDeleteRangeByRank($key, $start, $end);
    }

    public function zSize($key)
    {
        return $this->redis->zSize($key);
    }

    public function zCard($key)
    {
        return $this->redis->zCard($key);
    }

    public function zScore($key, $member)
    {
        return $this->redis->zScore($key, $member);
    }

    public function zRank($key, $member)
    {
        return $this->redis->zRank($key, $member);
    }

    public function zRevRank($key, $member)
    {
        return $this->redis->zRevRank($key, $member);
    }

    public function zIncrBy($key, $value, $member)
    {
        return $this->redis->zIncrBy($key, $value, $member);
    }

    public function zUnion($key, $setKeys, $weights = NULL, $fun = NULL)
    {
        if ($weights != NULL && $fun == NULL) {;
            return $this->redis->zUnion($key, $setKeys, $weights);
        }
        else if ($weights != NULL && $fun != NULL) {
            return $this->redis->zUnion($key, $setKeys, $weights, $fun);
        }
        else if ($weights == NULL && $fun != NULL){
            return $this->redis->zUnion($key, $setKeys, NULL, $fun);
        }
        else {
            return $this->redis->zUnion($key, $setKeys);
        }
    }

    public function zInter($key, $setKeys, $weights = NULL, $fun = NULL)
    {
        if ($weights != NULL && $fun == NULL) {;
            return $this->redis->zInter($key, $setKeys, $weights);
        }
        else if ($weights != NULL && $fun != NULL) {
            return $this->redis->zInter($key, $setKeys, $weights, $fun);
        }
        else if ($weights == NULL && $fun != NULL){
            return $this->redis->zInter($key, $setKeys, NULL, $fun);
        }
        else {
            return $this->redis->zInter($key, $setKeys);
        }
    }

    public function hSet($key, $hash, $value)
    {
        return $this->redis->hSet($key, $hash, $value);
    }

    public function hSetNx($key, $hash, $value)
    {
        return $this->redis->hSetNx($key, $hash, $value);
    }

    public function hGet($key, $hashKey)
    {
        return $this->redis->hGet($key, $hashKey);
    }

    public function hLen($key)
    {
        return $this->redis->hLen($key);
    }

    public function hDel($key, $hashKey)
    {
        return $this->redis->hDel($key, $hashKey);
    }

    public function hKeys($key)
    {
        return $this->redis->hKeys($key);
    }

    public function hVals($key)
    {
        return $this->redis->hVals($key);
    }

    public function hGetAll($key)
    {
        return $this->redis->hGetAll($key);
    }

    public function hExists($key, $memberKey)
    {
        return $this->redis->hExists($key, $memberKey);
    }

    public function hIncrBy($key, $memberKey, $value)
    {
        return $this->redis->hIncrBy($key, $memberKey, $value);
    }

    public function hMset($key, $arr)
    {
        return $this->redis->hMset($key, $arr);
    }

    public function hMGet($key, $memberKeys)
    {
        return $this->redis->hMGet($key, $memberKeys);
    }
}