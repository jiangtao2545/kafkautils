package com.jiangtao2545.kafkautils.controller;

import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.jiangtao2545.kafkautils.dto.request.CreateTopicRequest;
import com.jiangtao2545.kafkautils.dto.request.DeleteTopicRequest;
import com.jiangtao2545.kafkautils.dto.response.ApiResponse;
import com.jiangtao2545.kafkautils.dto.response.ClusterHealthResponse;
import com.jiangtao2545.kafkautils.dto.response.DeleteTopicResponse;
import com.jiangtao2545.kafkautils.dto.response.TopicDetailResponse;
import com.jiangtao2545.kafkautils.dto.response.TopicSummaryResponse;
import com.jiangtao2545.kafkautils.service.KafkaTopicService;

@RestController
@Validated
@RequestMapping("/kafka")
public class KafkaTopicController {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopicController.class);

    private final KafkaTopicService kafkaTopicService;

    public KafkaTopicController(KafkaTopicService kafkaTopicService) {
        this.kafkaTopicService = kafkaTopicService;
    }

    @GetMapping("/topic/list")
    public ApiResponse<List<TopicSummaryResponse>> listTopics() {
        LOGGER.info("Request to list Kafka topics");
        return ApiResponse.success(kafkaTopicService.listTopics());
    }

    @GetMapping("/topic/detail")
    public ApiResponse<TopicDetailResponse> topicDetail(
            @RequestParam("topicName") @NotBlank @Pattern(regexp = "^[a-zA-Z0-9._-]{1,249}$", message = "topicName格式不合法") String topicName) {
        LOGGER.info("Request to get topic detail, topicName={}", topicName);
        return ApiResponse.success(kafkaTopicService.getTopicDetail(topicName));
    }

    @PostMapping("/topic/create")
    public ApiResponse<Object> createTopic(@Valid @RequestBody CreateTopicRequest request) {
        LOGGER.info("Request to create topic, topicName={}", request.getTopicName());
        kafkaTopicService.createTopic(request);
        return ApiResponse.success("Topic创建成功", null);
    }

    @PostMapping("/topic/delete")
    public ApiResponse<DeleteTopicResponse> deleteTopic(@Valid @RequestBody DeleteTopicRequest request) {
        LOGGER.info("Request to delete topic(s), topicNames={}", request.getTopicNames());
        return ApiResponse.success("操作完成", kafkaTopicService.deleteTopics(request));
    }

    @GetMapping("/cluster/health")
    public ApiResponse<ClusterHealthResponse> clusterHealth() {
        LOGGER.info("Request to check cluster health");
        return ApiResponse.success(kafkaTopicService.checkClusterHealth());
    }
}
