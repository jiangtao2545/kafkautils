package com.jiangtao2545.kafkautils.service;

import java.util.List;

import com.jiangtao2545.kafkautils.dto.request.CreateTopicRequest;
import com.jiangtao2545.kafkautils.dto.request.DeleteTopicRequest;
import com.jiangtao2545.kafkautils.dto.response.ClusterHealthResponse;
import com.jiangtao2545.kafkautils.dto.response.DeleteTopicResponse;
import com.jiangtao2545.kafkautils.dto.response.TopicDetailResponse;
import com.jiangtao2545.kafkautils.dto.response.TopicSummaryResponse;

public interface KafkaTopicService {

    List<TopicSummaryResponse> listTopics();

    TopicDetailResponse getTopicDetail(String topicName);

    void createTopic(CreateTopicRequest request);

    DeleteTopicResponse deleteTopics(DeleteTopicRequest request);

    ClusterHealthResponse checkClusterHealth();
}
