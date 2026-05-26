package com.jiangtao2545.kafkautils.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.jiangtao2545.kafkautils.config.KafkaAdminClientFactory;
import com.jiangtao2545.kafkautils.dto.request.DeleteTopicRequest;
import com.jiangtao2545.kafkautils.dto.response.DeleteTopicResponse;
import com.jiangtao2545.kafkautils.service.impl.KafkaTopicServiceImpl;

class KafkaTopicServiceImplTest {

    @Test
    void shouldRejectSystemTopicDeletion() throws Exception {
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        KafkaAdminClientFactory factory = Mockito.mock(KafkaAdminClientFactory.class);
        Mockito.when(factory.getActiveAdminClient()).thenReturn(adminClient);

        org.apache.kafka.clients.admin.ListTopicsResult listTopicsResult = Mockito
                .mock(org.apache.kafka.clients.admin.ListTopicsResult.class);
        Mockito.when(adminClient.listTopics()).thenReturn(listTopicsResult);
        Mockito.when(listTopicsResult.names()).thenReturn(KafkaFuture.completedFuture(Collections.<String>emptySet()));

        DeleteTopicsResult deleteTopicsResult = Mockito.mock(DeleteTopicsResult.class);
        Mockito.when(adminClient.deleteTopics(Mockito.anyCollection())).thenReturn(deleteTopicsResult);
        Map<String, KafkaFuture<Void>> deleteFutures = new HashMap<String, KafkaFuture<Void>>();
        deleteFutures.put("normal_topic", KafkaFuture.completedFuture(null));
        Mockito.when(deleteTopicsResult.values()).thenReturn(deleteFutures);

        KafkaTopicServiceImpl service = new KafkaTopicServiceImpl(factory);
        DeleteTopicRequest request = new DeleteTopicRequest();
        request.setTopicNames("__consumer_offsets,normal_topic");

        DeleteTopicResponse response = service.deleteTopics(request);
        assertEquals(0, response.getSuccessList().size());
        assertEquals(2, response.getFailList().size());
    }
}
