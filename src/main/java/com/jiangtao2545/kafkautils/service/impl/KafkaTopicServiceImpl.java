package com.jiangtao2545.kafkautils.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.jiangtao2545.kafkautils.config.KafkaAdminClientFactory;
import com.jiangtao2545.kafkautils.dto.request.CreateTopicRequest;
import com.jiangtao2545.kafkautils.dto.request.DeleteTopicRequest;
import com.jiangtao2545.kafkautils.dto.response.ClusterHealthResponse;
import com.jiangtao2545.kafkautils.dto.response.DeleteTopicResponse;
import com.jiangtao2545.kafkautils.dto.response.TopicDetailResponse;
import com.jiangtao2545.kafkautils.dto.response.TopicDetailResponse.PartitionInfo;
import com.jiangtao2545.kafkautils.dto.response.TopicSummaryResponse;
import com.jiangtao2545.kafkautils.exception.BusinessException;
import com.jiangtao2545.kafkautils.exception.KafkaOperationException;
import com.jiangtao2545.kafkautils.service.KafkaTopicService;

@Service
public class KafkaTopicServiceImpl implements KafkaTopicService {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopicServiceImpl.class);

    private static final String RETENTION_MS = "retention.ms";
    private static final String CLEANUP_POLICY = "cleanup.policy";
    private static final String MAX_MESSAGE_BYTES = "max.message.bytes";

    private static final Set<String> SYSTEM_TOPICS = new HashSet<String>(
            Arrays.asList("__consumer_offsets", "__transaction_state", "__schema_history"));

    private final KafkaAdminClientFactory kafkaAdminClientFactory;

    public KafkaTopicServiceImpl(KafkaAdminClientFactory kafkaAdminClientFactory) {
        this.kafkaAdminClientFactory = kafkaAdminClientFactory;
    }

    @Override
    public List<TopicSummaryResponse> listTopics() {
        AdminClient adminClient = kafkaAdminClientFactory.getActiveAdminClient();
        try {
            Set<String> topicNames = adminClient.listTopics().names().get();
            List<String> targetTopics = topicNames.stream().filter(topic -> !SYSTEM_TOPICS.contains(topic)).sorted()
                    .collect(Collectors.toList());
            if (targetTopics.isEmpty()) {
                return Collections.emptyList();
            }
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(targetTopics);
            Map<String, TopicDescription> descriptionMap = describeTopicsResult.all().get();

            List<ConfigResource> resources = targetTopics.stream().map(topic -> new ConfigResource(Type.TOPIC, topic))
                    .collect(Collectors.toList());
            DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(resources);
            Map<ConfigResource, Config> configMap = describeConfigsResult.all().get();

            List<TopicSummaryResponse> responses = new ArrayList<TopicSummaryResponse>();
            for (String topicName : targetTopics) {
                TopicDescription topicDescription = descriptionMap.get(topicName);
                TopicSummaryResponse response = new TopicSummaryResponse();
                response.setTopicName(topicName);
                response.setPartitions(topicDescription.partitions().size());
                short replicationFactor = topicDescription.partitions().isEmpty() ? 0
                        : (short) topicDescription.partitions().get(0).replicas().size();
                response.setReplicationFactor(replicationFactor);
                response.setConfigSummary(extractCommonConfigs(configMap.get(new ConfigResource(Type.TOPIC, topicName))));
                responses.add(response);
            }
            return responses;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaOperationException("查询Topic列表被中断", ex);
        } catch (ExecutionException ex) {
            throw new KafkaOperationException("查询Topic列表失败", ex);
        }
    }

    @Override
    public TopicDetailResponse getTopicDetail(String topicName) {
        AdminClient adminClient = kafkaAdminClientFactory.getActiveAdminClient();
        if (!StringUtils.hasText(topicName)) {
            throw BusinessException.badRequest("topicName不能为空");
        }
        try {
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topicName));
            TopicDescription description = describeTopicsResult.all().get().get(topicName);
            if (description == null) {
                throw BusinessException.notFound("Topic不存在: " + topicName);
            }
            DescribeConfigsResult describeConfigsResult = adminClient
                    .describeConfigs(Collections.singletonList(new ConfigResource(Type.TOPIC, topicName)));
            Config config = describeConfigsResult.all().get().get(new ConfigResource(Type.TOPIC, topicName));

            TopicDetailResponse response = new TopicDetailResponse();
            response.setTopicName(topicName);
            response.setPartitionCount(description.partitions().size());
            short replicationFactor = description.partitions().isEmpty() ? 0
                    : (short) description.partitions().get(0).replicas().size();
            response.setReplicationFactor(replicationFactor);

            List<PartitionInfo> partitionInfos = description.partitions().stream().map(partition -> {
                PartitionInfo partitionInfo = new PartitionInfo();
                partitionInfo.setPartition(partition.partition());
                partitionInfo.setLeaderId(partition.leader() == null ? null : partition.leader().id());
                partitionInfo.setReplicas(
                        partition.replicas().stream().map(Node::id).collect(Collectors.toList()));
                partitionInfo.setInSyncReplicas(partition.isr().stream().map(Node::id).collect(Collectors.toList()));
                return partitionInfo;
            }).collect(Collectors.toList());
            response.setPartitions(partitionInfos);
            response.setConfigs(extractCommonConfigs(config));
            return response;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaOperationException("查询Topic详情被中断", ex);
        } catch (ExecutionException ex) {
            if (ex.getCause() != null && ex.getCause().getClass().getSimpleName().contains("UnknownTopicOrPartition")) {
                throw BusinessException.notFound("Topic不存在: " + topicName);
            }
            throw new KafkaOperationException("查询Topic详情失败", ex);
        }
    }

    @Override
    public void createTopic(CreateTopicRequest request) {
        AdminClient adminClient = kafkaAdminClientFactory.getActiveAdminClient();
        if (SYSTEM_TOPICS.contains(request.getTopicName())) {
            throw BusinessException.forbidden("不允许创建系统保留Topic");
        }
        NewTopic newTopic = new NewTopic(request.getTopicName(), request.getPartitions(), request.getReplicationFactor());
        Map<String, String> configs = new HashMap<String, String>();
        if (request.getRetentionMs() != null) {
            configs.put(RETENTION_MS, String.valueOf(request.getRetentionMs()));
        }
        if (StringUtils.hasText(request.getCleanupPolicy())) {
            configs.put(CLEANUP_POLICY, request.getCleanupPolicy());
        }
        if (!configs.isEmpty()) {
            newTopic.configs(configs);
        }
        LOGGER.info("Creating topic: name={}, partitions={}, replicationFactor={}", request.getTopicName(),
                request.getPartitions(), request.getReplicationFactor());
        try {
            CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singletonList(newTopic));
            createTopicsResult.all().get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaOperationException("创建Topic被中断", ex);
        } catch (ExecutionException ex) {
            throw new KafkaOperationException("创建Topic失败: " + request.getTopicName(), ex);
        }
    }

    @Override
    public DeleteTopicResponse deleteTopics(DeleteTopicRequest request) {
        AdminClient adminClient = kafkaAdminClientFactory.getActiveAdminClient();
        DeleteTopicResponse response = new DeleteTopicResponse();

        Set<String> requestedTopics = Arrays.stream(request.getTopicNames().split(",")).map(String::trim)
                .filter(StringUtils::hasText).collect(Collectors.toCollection(LinkedHashSet::new));
        if (requestedTopics.isEmpty()) {
            throw BusinessException.badRequest("topicNames不能为空");
        }

        for (String topic : requestedTopics) {
            if (!topic.matches("^[a-zA-Z0-9._-]{1,249}$")) {
                response.addFail(topic, "Topic名称不合法");
            }
        }

        Set<String> validTopics = requestedTopics.stream().filter(topic -> topic.matches("^[a-zA-Z0-9._-]{1,249}$"))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        for (String topic : validTopics) {
            if (SYSTEM_TOPICS.contains(topic)) {
                response.addFail(topic, "系统Topic禁止删除");
            }
        }

        Set<String> candidateTopics = validTopics.stream().filter(topic -> !SYSTEM_TOPICS.contains(topic))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        try {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            List<String> deletableTopics = candidateTopics.stream().filter(existingTopics::contains)
                    .collect(Collectors.toList());

            for (String topic : candidateTopics) {
                if (!existingTopics.contains(topic)) {
                    response.addFail(topic, "Topic不存在");
                }
            }

            if (!deletableTopics.isEmpty()) {
                LOGGER.info("Deleting topics: {}", deletableTopics);
                DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(deletableTopics);
                for (Map.Entry<String, org.apache.kafka.common.KafkaFuture<Void>> future : deleteTopicsResult.values()
                        .entrySet()) {
                    try {
                        future.getValue().get();
                        response.addSuccess(future.getKey());
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        String reason = "删除被中断";
                        if (Boolean.TRUE.equals(request.getForceDelete())) {
                            LOGGER.warn("Force delete ignored interruption for topic={}", future.getKey());
                            response.addFail(future.getKey(), reason + "（forceDelete已忽略）");
                        } else {
                            response.addFail(future.getKey(), reason);
                        }
                    } catch (ExecutionException ex) {
                        String reason = ex.getCause() == null ? ex.getMessage() : ex.getCause().getMessage();
                        if (Boolean.TRUE.equals(request.getForceDelete())) {
                            LOGGER.warn("Force delete ignored failure for topic={}, reason={}", future.getKey(), reason);
                            response.addFail(future.getKey(), reason + "（forceDelete已忽略）");
                        } else {
                            response.addFail(future.getKey(), reason);
                        }
                    }
                }
            }
            return response;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaOperationException("删除Topic被中断", ex);
        } catch (ExecutionException ex) {
            throw new KafkaOperationException("删除Topic失败", ex);
        }
    }

    @Override
    public ClusterHealthResponse checkClusterHealth() {
        AdminClient adminClient = kafkaAdminClientFactory.getActiveAdminClient();
        try {
            DescribeClusterResult clusterResult = adminClient.describeCluster();
            String clusterId = clusterResult.clusterId().get();
            List<Node> nodes = new ArrayList<Node>(clusterResult.nodes().get());
            Node controller = clusterResult.controller().get();
            ClusterHealthResponse response = new ClusterHealthResponse();
            response.setClusterId(clusterId);
            response.setController(controller == null ? null : String.valueOf(controller.id()));
            response.setNodeCount(nodes.size());
            response.setNodes(nodes.stream().map(node -> node.id() + "@" + node.host() + ":" + node.port())
                    .collect(Collectors.toList()));
            return response;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaOperationException("集群检查被中断", ex);
        } catch (ExecutionException ex) {
            throw new KafkaOperationException("Kafka集群不可用，请检查连接配置", ex);
        }
    }

    private Map<String, String> extractCommonConfigs(Config config) {
        Map<String, String> result = new HashMap<String, String>();
        if (config == null) {
            return result;
        }
        putIfPresent(config, result, CLEANUP_POLICY);
        putIfPresent(config, result, RETENTION_MS);
        putIfPresent(config, result, MAX_MESSAGE_BYTES);
        return result;
    }

    private void putIfPresent(Config config, Map<String, String> result, String key) {
        ConfigEntry entry = config.get(key);
        if (entry != null && entry.value() != null) {
            result.put(key, entry.value());
        }
    }
}
