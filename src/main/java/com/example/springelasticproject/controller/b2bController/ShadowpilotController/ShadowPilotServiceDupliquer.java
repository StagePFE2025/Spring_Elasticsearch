package com.example.springelasticproject.controller.b2bController.ShadowpilotController;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;

import com.example.springelasticproject.model.b2bModel.ShadowPilot.ShadowPilot;
import com.example.springelasticproject.repository.ShadowPilotRepository.ShadowPilotRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

@Service
public class ShadowPilotServiceDupliquer {

    @Autowired
    private ElasticsearchOperations elasticsearchOperations;

    @Autowired
    private ShadowPilotRepository shadowPilotRepository;
    @Async
    public void removeDuplicateCompanies() {
        int pageSize =10000;
        int page = 0;
        boolean hasMore = true;

        Map<String, String> seenKeysToKeepId = new HashMap<>();
        List<String> duplicateIds = new ArrayList<>();

        while (hasMore) {
            Pageable pageable = PageRequest.of(page, pageSize);
            Page<ShadowPilot> companyPage = shadowPilotRepository.findAll(pageable);

            for (ShadowPilot company : companyPage) {
                String key = (company.getName() ).toLowerCase().trim();
                if (seenKeysToKeepId.containsKey(key)) {
                    duplicateIds.add(company.getId());
                } else {
                    seenKeysToKeepId.put(key, company.getId());
                }
            }

            hasMore = companyPage.hasNext();
            page++;
        }

        // Suppression des doublons
        if (!duplicateIds.isEmpty()) {
            shadowPilotRepository.deleteAllById(duplicateIds);
            System.out.println("✔ Supprimé " + duplicateIds.size() + " doublons.");
        } else {
            System.out.println("Aucun doublon trouvé.");
        }
    }
}