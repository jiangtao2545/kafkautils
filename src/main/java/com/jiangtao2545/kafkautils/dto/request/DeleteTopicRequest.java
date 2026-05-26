package com.jiangtao2545.kafkautils.dto.request;

import javax.validation.constraints.NotBlank;

public class DeleteTopicRequest {

    @NotBlank(message = "topicNames不能为空")
    private String topicNames;

    private Boolean forceDelete = Boolean.FALSE;

    public String getTopicNames() {
        return topicNames;
    }

    public void setTopicNames(String topicNames) {
        this.topicNames = topicNames;
    }

    public Boolean getForceDelete() {
        return forceDelete;
    }

    public void setForceDelete(Boolean forceDelete) {
        this.forceDelete = forceDelete;
    }
}
