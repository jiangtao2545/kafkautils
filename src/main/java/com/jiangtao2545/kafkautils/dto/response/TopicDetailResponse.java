package com.jiangtao2545.kafkautils.dto.response;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TopicDetailResponse {

    private String topicName;
    private Integer partitionCount;
    private Short replicationFactor;
    private List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();
    private Map<String, String> configs;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public Integer getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(Integer partitionCount) {
        this.partitionCount = partitionCount;
    }

    public Short getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(Short replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public List<PartitionInfo> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<PartitionInfo> partitions) {
        this.partitions = partitions;
    }

    public Map<String, String> getConfigs() {
        return configs;
    }

    public void setConfigs(Map<String, String> configs) {
        this.configs = configs;
    }

    public static class PartitionInfo {
        private Integer partition;
        private Integer leaderId;
        private List<Integer> replicas;
        private List<Integer> inSyncReplicas;

        public Integer getPartition() {
            return partition;
        }

        public void setPartition(Integer partition) {
            this.partition = partition;
        }

        public Integer getLeaderId() {
            return leaderId;
        }

        public void setLeaderId(Integer leaderId) {
            this.leaderId = leaderId;
        }

        public List<Integer> getReplicas() {
            return replicas;
        }

        public void setReplicas(List<Integer> replicas) {
            this.replicas = replicas;
        }

        public List<Integer> getInSyncReplicas() {
            return inSyncReplicas;
        }

        public void setInSyncReplicas(List<Integer> inSyncReplicas) {
            this.inSyncReplicas = inSyncReplicas;
        }
    }
}
