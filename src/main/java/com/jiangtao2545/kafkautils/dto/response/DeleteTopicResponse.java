package com.jiangtao2545.kafkautils.dto.response;

import java.util.ArrayList;
import java.util.List;

public class DeleteTopicResponse {

    private List<String> successList = new ArrayList<String>();
    private List<FailedTopic> failList = new ArrayList<FailedTopic>();

    public List<String> getSuccessList() {
        return successList;
    }

    public void setSuccessList(List<String> successList) {
        this.successList = successList;
    }

    public List<FailedTopic> getFailList() {
        return failList;
    }

    public void setFailList(List<FailedTopic> failList) {
        this.failList = failList;
    }

    public void addSuccess(String topicName) {
        successList.add(topicName);
    }

    public void addFail(String topicName, String reason) {
        failList.add(new FailedTopic(topicName, reason));
    }

    public static class FailedTopic {
        private String topicName;
        private String reason;

        public FailedTopic() {
        }

        public FailedTopic(String topicName, String reason) {
            this.topicName = topicName;
            this.reason = reason;
        }

        public String getTopicName() {
            return topicName;
        }

        public void setTopicName(String topicName) {
            this.topicName = topicName;
        }

        public String getReason() {
            return reason;
        }

        public void setReason(String reason) {
            this.reason = reason;
        }
    }
}
