package com.jiangtao2545.kafkautils.dto.response;

import java.util.Map;

public class TopicSummaryResponse {

    private String topicName;
    private Integer partitions;
    private Short replicationFactor;
    private Map<String, String> configSummary;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public Integer getPartitions() {
        return partitions;
    }

    public void setPartitions(Integer partitions) {
        this.partitions = partitions;
    }

    public Short getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(Short replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public Map<String, String> getConfigSummary() {
        return configSummary;
    }

    public void setConfigSummary(Map<String, String> configSummary) {
        this.configSummary = configSummary;
    }
}
