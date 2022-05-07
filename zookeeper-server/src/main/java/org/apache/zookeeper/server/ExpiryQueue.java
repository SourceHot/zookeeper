/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import org.apache.zookeeper.common.Time;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ExpiryQueue tracks elements in time sorted fixed duration buckets.
 * It's used by SessionTrackerImpl to expire sessions and NIOServerCnxnFactory
 * to expire connections.
 */
public class ExpiryQueue<E> {
    /**
     * 过期容器
     * key: 过期对象
     * value: 过期时间
     */
    private final ConcurrentHashMap<E, Long> elemMap =
            new ConcurrentHashMap<E, Long>();
    /**
     * The maximum number of buckets is equal to max timeout/expirationInterval,
     * so the expirationInterval should not be too small compared to the
     * max timeout that this expiry queue needs to maintain.
     *
     * 过期容器
     * key: 过期时间
     * value: 过期对象集合
     */
    private final ConcurrentHashMap<Long, Set<E>> expiryMap =
            new ConcurrentHashMap<Long, Set<E>>();

    /**
     * 下一个过期时间
     */
    private final AtomicLong nextExpirationTime = new AtomicLong();
    /**
     * 间隔期间
     */
    private final int expirationInterval;

    public ExpiryQueue(int expirationInterval) {
        this.expirationInterval = expirationInterval;
        nextExpirationTime.set(roundToNextInterval(Time.currentElapsedTime()));
    }

    /**
     * 计算下一个区间
     * @param time
     * @return
     */
    private long roundToNextInterval(long time) {
        return (time / expirationInterval + 1) * expirationInterval;
    }

    /**
     * Removes element from the queue.
     * @param elem  element to remove
     * @return time at which the element was set to expire, or null if
     *              it wasn't present
     */
    public Long remove(E elem) {
        Long expiryTime = elemMap.remove(elem);
        if (expiryTime != null) {
            Set<E> set = expiryMap.get(expiryTime);
            if (set != null) {
                set.remove(elem);
                // We don't need to worry about removing empty sets,
                // they'll eventually be removed when they expire.
            }
        }
        return expiryTime;
    }

    /**
     * Adds or updates expiration time for element in queue, rounding the
     * timeout to the expiry interval bucketed used by this queue.
     *
     * @param elem    element to add/update
     * @param timeout timout in milliseconds
     * @return time at which the element is now set to expire if
     * changed, or null if unchanged
     */
    public Long update(E elem, int timeout) {
        // 从elemMap容器中根据元素获取过期时间，含义为上一个过期时间
        Long prevExpiryTime = elemMap.get(elem);
        // 获取当前时间
        long now = Time.currentElapsedTime();
        // 计算下一个过期时间
        Long newExpiryTime = roundToNextInterval(now + timeout);

        // 判断新的过期时间和上一个过期时间是否相同，如果相同则返回null
        if (newExpiryTime.equals(prevExpiryTime)) {
            // No change, so nothing to update
            return null;
        }

        // First add the elem to the new expiry time bucket in expiryMap.
        // 获取过期时间对应的数据
        Set<E> set = expiryMap.get(newExpiryTime);
        if (set == null) {
            // Construct a ConcurrentHashSet using a ConcurrentHashMap
            // 创建set对象
            set = Collections.newSetFromMap(
                    new ConcurrentHashMap<E, Boolean>());
            // Put the new set in the map, but only if another thread
            // hasn't beaten us to it
            // 向expiryMap容器中加入过期数据
            Set<E> existingSet = expiryMap.putIfAbsent(newExpiryTime, set);
            // 存在历史过期数据将其赋值到set变量
            if (existingSet != null) {
                set = existingSet;
            }
        }
        // 将参数数据加入到set集合中
        set.add(elem);

        // Map the elem to the new expiry time. If a different previous
        // mapping was present, clean up the previous expiry bucket.
        // 向elemMap容器加入elem数据，并获取历史过期时间
        prevExpiryTime = elemMap.put(elem, newExpiryTime);
        // 历史过期时间不为空
        // 新的过期时间和历史过期时间不相同
        if (prevExpiryTime != null && !newExpiryTime.equals(prevExpiryTime)) {
            // 根据历史过期时间获取过期元素集合
            Set<E> prevSet = expiryMap.get(prevExpiryTime);
            // 过期元素集合不为空的情况下移除当前元素
            if (prevSet != null) {
                prevSet.remove(elem);
            }
        }
        return newExpiryTime;
    }

    /**
     * @return milliseconds until next expiration time, or 0 if has already past
     */
    public long getWaitTime() {
        // 当前时间
        long now = Time.currentElapsedTime();
        // 过期时间
        long expirationTime = nextExpirationTime.get();
        // 当前时间小于过期时间则将时间差返回，反之则返回0
        return now < expirationTime ? (expirationTime - now) : 0L;
    }

    /**
     * Remove the next expired set of elements from expireMap. This method needs
     * to be called frequently enough by checking getWaitTime(), otherwise there
     * will be a backlog of empty sets queued up in expiryMap.
     *
     * @return next set of expired elements, or an empty set if none are
     *         ready
     */
    public Set<E> poll() {
        // 获取当前时间
        long now = Time.currentElapsedTime();
        // 获取过期时间
        long expirationTime = nextExpirationTime.get();
        // 如果当前时间小于过期时间，返回空集合
        if (now < expirationTime) {
            return Collections.emptySet();
        }

        // 创建存储结果的容器
        Set<E> set = null;
        // 计算新的过期时间
        long newExpirationTime = expirationTime + expirationInterval;
        // 将新的过期时间赋值到变量nextExpirationTime中
        if (nextExpirationTime.compareAndSet(
                expirationTime, newExpirationTime)) {
            // 从expiryMap容器中移除过期时间对应的数据，并将数据保存到结果容器中
            set = expiryMap.remove(expirationTime);
        }
        // 如果结果存储容器为空创建空集合
        if (set == null) {
            return Collections.emptySet();
        }
        return set;
    }

    /**
     * 输出相关信息
     */
    public void dump(PrintWriter pwriter) {
        pwriter.print("Sets (");
        pwriter.print(expiryMap.size());
        pwriter.print(")/(");
        pwriter.print(elemMap.size());
        pwriter.println("):");
        ArrayList<Long> keys = new ArrayList<Long>(expiryMap.keySet());
        Collections.sort(keys);
        for (long time : keys) {
            Set<E> set = expiryMap.get(time);
            if (set != null) {
                pwriter.print(set.size());
                pwriter.print(" expire at ");
                pwriter.print(Time.elapsedTimeToDate(time));
                pwriter.println(":");
                for (E elem : set) {
                    pwriter.print("\t");
                    pwriter.println(elem.toString());
                }
            }
        }
    }

    /**
     * Returns an unmodifiable view of the expiration time -> elements mapping.
     */
    public Map<Long, Set<E>> getExpiryMap() {
        return Collections.unmodifiableMap(expiryMap);
    }
}

