/*
 * Copyright 2015 Ben Manes. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.benmanes.caffeine.cache.simulator.policy.linked;

import static java.util.Locale.US;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.StringUtils;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.Admission;
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.google.common.base.MoreObjects;
import com.typesafe.config.Config;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

/**
 * Inverse Weight Policy in O(log(n)) time. Elements hit rates act as weight where the probability of being evicted is
 * based on the inverse weight.
 *
 * @author l.f.d.versluis@vu.nl (Laurens Versluis)
 */
public final class InverseWeightPolicy implements Policy {
    final PolicyStats policyStats;
    final Long2ObjectMap<Node> data;
    final EvictionPolicy policy;
    final FrequencyNode freq0;
    final Admittor admittor;
    final int maximumSize;

    public InverseWeightPolicy(Admission admission, EvictionPolicy policy, Config config) {
        this.policyStats = new PolicyStats(admission.format("linked." + policy.label()));
        this.admittor = admission.from(config, policyStats);
        BasicSettings settings = new BasicSettings(config);
        this.data = new Long2ObjectOpenHashMap<>();
        this.maximumSize = settings.maximumSize();
        this.policy = requireNonNull(policy);
        this.freq0 = new FrequencyNode();
    }

    /**
     * Returns all variations of this policy based on the configuration parameters.
     */
    public static Set<Policy> policies(Config config, EvictionPolicy policy) {
        BasicSettings settings = new BasicSettings(config);
        return settings.admission().stream().map(admission ->
                new InverseWeightPolicy(admission, policy, config)
        ).collect(toSet());
    }

    @Override
    public PolicyStats stats() {
        return policyStats;
    }

    @Override
    public void record(long key) {
        policyStats.recordOperation();
        Node node = data.get(key);
        admittor.record(key);
        if (node == null) {
            onMiss(key);
        } else {
            onHit(node);
        }
    }

    /**
     * Moves the entry to the nextFreqNode higher frequency list, creating it if necessary.
     */
    private void onHit(Node node) {
        policyStats.recordHit();

        int newHits = node.freqNode.frequency + 1;
        FrequencyNode freqN = (node.freqNode.nextFreqNode.frequency == newHits)
                ? node.freqNode.nextFreqNode
                : new FrequencyNode(newHits, node.freqNode);
        node.remove();

        if (node.freqNode.isEmpty()) {
            node.freqNode.remove();
        }
        node.freqNode = freqN;
        node.append();
    }

    /**
     * Adds the entry, creating an initial frequency list of 1 if necessary, and evicts if needed.
     */
    private void onMiss(long key) {
        FrequencyNode freq1 = (freq0.nextFreqNode.frequency == 1)
                ? freq0.nextFreqNode
                : new FrequencyNode(1, freq0);
        Node node = new Node(key, freq1);
        policyStats.recordMiss();
        data.put(key, node);
        node.append();
        evict(node);
    }

    /**
     * Evicts while the map exceeds the maximum capacity.
     */
    private void evict(Node candidate) {
        if (data.size() > maximumSize) {
            Node victim = nextVictim();
            boolean admit = admittor.admit(candidate.key, victim.key);
            if (admit) {
                evictEntry(victim);
            } else {
                evictEntry(candidate);
            }
            policyStats.recordEviction();
        }
    }

    /**
     * Returns the nextFreqNode victim, excluding the newly added candidate. This exclusion is required so
     * that a candidate has a fair chance to be used, rather than always rejected due to existing
     * entries having a high frequency from the distant past.
     */
    Node nextVictim() {
        // A treemap keeps the keys sorted.
        TreeMap<Long, Integer> cumsumToFreq = new TreeMap<>();
        HashMap<Integer, FrequencyNode> freqToFreqNode = new HashMap<>(100);
        FrequencyNode f = freq0.nextFreqNode;
        long currentSum = 0;
        while (f != freq0) {
            cumsumToFreq.put(currentSum, f.frequency);
            freqToFreqNode.put(f.frequency, f);

            Long w = (long) Math.ceil(f.nodeCount * (Math.pow(10, 8) / f.frequency));
            currentSum += w;
            f = f.nextFreqNode;
        }

        Long randomNum = ThreadLocalRandom.current().nextLong(0, currentSum);

        long prevKey = 0;
        for(Long key : cumsumToFreq.keySet()) {
            if(key < randomNum) {
                prevKey = key;
            } else {
                break;
            }
        }

        FrequencyNode target = freqToFreqNode.get(cumsumToFreq.get(prevKey));

        if(policy == EvictionPolicy.IWLRU) {
            return target.nextNode.nextNode;
        }

        // Evict a random node from this frequency
        int indexToEvict = ThreadLocalRandom.current().nextInt(0, target.nodeCount);
        Node n = target.nextNode.nextNode;
        while(indexToEvict > 0) {
            n = n.nextNode;
            indexToEvict--;
        }
        return n;
    }

    /**
     * Removes the entry.
     */
    private void evictEntry(Node node) {
        data.remove(node.key);
        node.remove();
        if (node.freqNode.isEmpty()) {
            node.freqNode.remove();
        }
    }

    public enum EvictionPolicy {
        IW, IWLRU;  // Inverse Weight. Idea for the next one: Inverse Weight based on both size AND/OR frequency. Or an Inverse weight where we do not evict from the list at random but the oldest one (LRU style).

        public String label() {
            return StringUtils.capitalize(name().toLowerCase(US));
        }
    }

    /**
     * A frequency frequency and associated chain of cache entries.
     */
    static final class FrequencyNode {
        final int frequency;
        final Node nextNode;
        int nodeCount;
        ArrayList<Long> keylist;

        FrequencyNode prevFreqNode;
        FrequencyNode nextFreqNode;

        public FrequencyNode() {
            nextNode = new Node(this);
            this.prevFreqNode = this;
            this.nextFreqNode = this;
            this.frequency = 0;
            this.nodeCount = 0;
            keylist = new ArrayList<>(10000);
        }

        public FrequencyNode(int frequency, FrequencyNode prevFreqNode) {
            nextNode = new Node(this);
            this.prevFreqNode = prevFreqNode;
            this.nextFreqNode = prevFreqNode.nextFreqNode;
            prevFreqNode.nextFreqNode = this;
            nextFreqNode.prevFreqNode = this;
            this.frequency = frequency;
            this.nodeCount = 0;
            keylist = new ArrayList<>(10000);
        }

        public boolean isEmpty() {
            return (nextNode == nextNode.nextNode);
        }

        /**
         * Removes the node from the list.
         */
        public void remove() {
            prevFreqNode.nextFreqNode = nextFreqNode;
            nextFreqNode.prevFreqNode = prevFreqNode;
            nextFreqNode = prevFreqNode = null;
            nodeCount = 0;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("frequency", frequency)
                    .toString();
        }
    }

    /**
     * A cache entry on the frequency node's chain.
     */
    static final class Node {
        final long key;

        FrequencyNode freqNode;
        Node prevNode;
        Node nextNode;

        public Node(FrequencyNode freqNode) {
            this.key = Long.MIN_VALUE;
            this.freqNode = freqNode;
            this.prevNode = this;
            this.nextNode = this;
        }

        public Node(long key, FrequencyNode freqNode) {
            this.nextNode = null;
            this.prevNode = null;
            this.freqNode = freqNode;
            this.key = key;
        }

        /**
         * Appends the node to the tail of the list.
         */
        public void append() {
            freqNode.nodeCount++;
            prevNode = freqNode.nextNode.prevNode;
            nextNode = freqNode.nextNode;
            prevNode.nextNode = this;
            nextNode.prevNode = this;
            freqNode.keylist.add(this.key);
        }

        /**
         * Removes the node from the list.
         */
        public void remove() {
            freqNode.nodeCount--;
            prevNode.nextNode = nextNode;
            nextNode.prevNode = prevNode;
            nextNode = prevNode = null;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("key", key)
                    .add("freqNode", freqNode)
                    .toString();
        }
    }
}
