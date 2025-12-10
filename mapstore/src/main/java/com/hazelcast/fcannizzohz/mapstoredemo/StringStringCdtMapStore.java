package com.hazelcast.fcannizzohz.mapstoredemo;

import com.aerospike.client.Value;

public class StringStringCdtMapStore
        extends AbstractAerospikeCdtMapStore<String, String> {

    @Override
    protected Value toAerospikeMapKey(String key) {
        return Value.get(key);
    }

    @Override
    protected Value toAerospikeMapValue(String value) {
        return Value.get(value);
    }

    @Override
    protected String fromAerospikeMapValue(String key, Value value) {
        Object raw = value.getObject();
        if (raw == null) {
            return null;
        }
        return raw.toString();
    }
}
