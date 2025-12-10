package com.hazelcast.fcannizzohz.mapstoredemo;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.client.test.TestHazelcastFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.fcannizzohz.mapstoredemo.AerospikeTestClient.AS_HOST;
import static com.hazelcast.fcannizzohz.mapstoredemo.AerospikeTestClient.AS_PORT;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static org.assertj.core.api.Assertions.assertThat;

public class StringStringCdtMapStoreITest {

    private static TestHazelcastFactory hazelcastFactory;
    private static AerospikeTestClient aerospikeClient;

    private HazelcastInstance hazelcast;


    // Shared Aerospike metadata – MUST match MapStore properties


    @BeforeClass
    public static void setUpClass() {

        aerospikeClient = new AerospikeTestClient();// ---- Pre-populate CDT map in Aerospike ----
        aerospikeClient.prepopulateCdtAerospike();
        // ---- Start Hazelcast member with MapStore ----
        hazelcastFactory = new TestHazelcastFactory();
    }

    @AfterClass
    public static void tearDownClass() {
        if (hazelcastFactory != null) {
            hazelcastFactory.terminateAll();
        }
        if (aerospikeClient != null) {
            aerospikeClient.close();
        }
    }

    @Before
    public void setUp() {
        hazelcast = hazelcastFactory.newHazelcastInstance(newHzConfig());
    }

    @After
    public void tearDown() {
        if (hazelcastFactory != null) {
            hazelcastFactory.shutdownAll();
        }
    }

    private Config newHzConfig() {
        Config config = new Config()
                .setJetConfig(new JetConfig().setEnabled(true))
                .setClusterName(randomName())
                .setLicenseKey(System.getenv("HZ_LICENSEKEY"));

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setImplementation(new StringStringCdtMapStore())
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY);

        Properties props = new Properties();
        props.setProperty("aerospike.host", AS_HOST);
        props.setProperty("aerospike.port", String.valueOf(AS_PORT));
        props.setProperty("aerospike.namespace", aerospikeClient.namespace);
        props.setProperty("aerospike.set", aerospikeClient.setName);
        props.setProperty("aerospike.mapBinName",aerospikeClient.mapBinName);
        props.setProperty("aerospike.recordKeyType", "string");
        props.setProperty("aerospike.recordKey", aerospikeClient.recordKeyString);
        mapStoreConfig.setProperties(props);

        config.getMapConfig("samples")
              .setMapStoreConfig(mapStoreConfig);

        return config;
    }

    @Test
    public void testLazyLoadFromAerospike() {
        IMap<String, String> map = hazelcast.getMap("samples");

        // Trigger lazy load via getAll
        Set<String> keys = Set.of("k1", "k2", "k3", "k4", "k5");
        Map<String, String> loaded = map.getAll(keys);

        assertThat(loaded).hasSize(5);
        assertThat(loaded.get("k1")).isEqualTo("v1");
        assertThat(loaded.get("k5")).isEqualTo("v5");
    }

    @Test
    public void testStoreWritesBackToAerospike() {
        IMap<String, String> map = hazelcast.getMap("samples");

        // Put a new entry via Hazelcast
        map.put("k6", "v6");

        // Verify directly in Aerospike
        Object raw = aerospikeClient.get("k6");
        assertThat(raw).isNotNull();
        assertThat(raw.toString()).isEqualTo("v6");
    }
}
