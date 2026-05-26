package com.jiangtao2545.kafkautils.config;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kafka")
public class KafkaClusterProperties {

    private String activeCluster = "default";

    private Map<String, ClusterConfig> clusters = new HashMap<String, ClusterConfig>();

    public String getActiveCluster() {
        return activeCluster;
    }

    public void setActiveCluster(String activeCluster) {
        this.activeCluster = activeCluster;
    }

    public Map<String, ClusterConfig> getClusters() {
        return clusters;
    }

    public void setClusters(Map<String, ClusterConfig> clusters) {
        this.clusters = clusters;
    }

    public static class ClusterConfig {
        private String bootstrapServers;
        private String securityProtocol = "PLAINTEXT";
        private Integer requestTimeoutMs = 5000;
        private String saslMechanism;
        private String saslJaasConfig;

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getSecurityProtocol() {
            return securityProtocol;
        }

        public void setSecurityProtocol(String securityProtocol) {
            this.securityProtocol = securityProtocol;
        }

        public Integer getRequestTimeoutMs() {
            return requestTimeoutMs;
        }

        public void setRequestTimeoutMs(Integer requestTimeoutMs) {
            this.requestTimeoutMs = requestTimeoutMs;
        }

        public String getSaslMechanism() {
            return saslMechanism;
        }

        public void setSaslMechanism(String saslMechanism) {
            this.saslMechanism = saslMechanism;
        }

        public String getSaslJaasConfig() {
            return saslJaasConfig;
        }

        public void setSaslJaasConfig(String saslJaasConfig) {
            this.saslJaasConfig = saslJaasConfig;
        }
    }
}
