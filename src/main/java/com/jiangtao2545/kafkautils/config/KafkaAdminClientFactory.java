package com.jiangtao2545.kafkautils.config;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PreDestroy;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.jiangtao2545.kafkautils.config.KafkaClusterProperties.ClusterConfig;
import com.jiangtao2545.kafkautils.exception.BusinessException;

@Component
public class KafkaAdminClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAdminClientFactory.class);

    private final KafkaClusterProperties kafkaClusterProperties;

    private final Map<String, AdminClient> adminClientCache = new ConcurrentHashMap<String, AdminClient>();

    public KafkaAdminClientFactory(KafkaClusterProperties kafkaClusterProperties) {
        this.kafkaClusterProperties = kafkaClusterProperties;
    }

    public AdminClient getActiveAdminClient() {
        return getAdminClient(kafkaClusterProperties.getActiveCluster());
    }

    public AdminClient getAdminClient(String clusterName) {
        ClusterConfig clusterConfig = kafkaClusterProperties.getClusters().get(clusterName);
        if (clusterConfig == null) {
            throw BusinessException.badRequest("未找到Kafka集群配置: " + clusterName);
        }
        if (!StringUtils.hasText(clusterConfig.getBootstrapServers())) {
            throw BusinessException.badRequest("Kafka bootstrap-servers 未配置: " + clusterName);
        }

        return adminClientCache.computeIfAbsent(clusterName, name -> {
            Properties properties = new Properties();
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clusterConfig.getBootstrapServers());
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, clusterConfig.getSecurityProtocol());
            properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, clusterConfig.getRequestTimeoutMs());
            if (StringUtils.hasText(clusterConfig.getSaslMechanism())) {
                properties.put(SaslConfigs.SASL_MECHANISM, clusterConfig.getSaslMechanism());
            }
            if (StringUtils.hasText(clusterConfig.getSaslJaasConfig())) {
                properties.put(SaslConfigs.SASL_JAAS_CONFIG, clusterConfig.getSaslJaasConfig());
            }
            LOGGER.info("Create Kafka AdminClient for cluster={}", name);
            return AdminClient.create(properties);
        });
    }

    @PreDestroy
    public void closeAll() {
        for (Map.Entry<String, AdminClient> entry : adminClientCache.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception ex) {
                LOGGER.warn("Failed to close AdminClient for cluster={}", entry.getKey(), ex);
            }
        }
        adminClientCache.clear();
    }
}
