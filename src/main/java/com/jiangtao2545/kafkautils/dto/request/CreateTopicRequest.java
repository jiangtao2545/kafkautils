package com.jiangtao2545.kafkautils.dto.request;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;

public class CreateTopicRequest {

    @NotBlank(message = "topicName不能为空")
    @Pattern(regexp = "^[a-zA-Z0-9._-]{1,249}$", message = "topicName格式不合法")
    private String topicName;

    @Min(value = 1, message = "partitions必须大于0")
    @Max(value = 1000, message = "partitions过大")
    private Integer partitions = 1;

    @Min(value = 1, message = "replicationFactor必须大于0")
    @Max(value = 10, message = "replicationFactor过大")
    private Short replicationFactor = 1;

    @Min(value = 1, message = "retentionMs必须大于0")
    private Long retentionMs;

    @Pattern(regexp = "^(delete|compact)$", message = "cleanupPolicy仅支持delete或compact")
    private String cleanupPolicy;

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

    public Long getRetentionMs() {
        return retentionMs;
    }

    public void setRetentionMs(Long retentionMs) {
        this.retentionMs = retentionMs;
    }

    public String getCleanupPolicy() {
        return cleanupPolicy;
    }

    public void setCleanupPolicy(String cleanupPolicy) {
        this.cleanupPolicy = cleanupPolicy;
    }
}
