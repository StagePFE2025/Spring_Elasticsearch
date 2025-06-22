package com.example.springelasticproject.Services.b2bService;

import com.example.springelasticproject.model.b2bModel.B2B;
import com.example.springelasticproject.model.b2bModel.ScoreResult;
import com.example.springelasticproject.repository.B2BRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Service
public class B2BScoreServiceFast {

    private final B2BRepository b2bRepository;
    private final ElasticsearchOperations elasticsearchOperations;
    private final Executor executor = Executors.newFixedThreadPool(4); // Adjust thread count based on your system

    @Autowired
    public B2BScoreServiceFast(B2BRepository b2bRepository,
                           ElasticsearchOperations elasticsearchOperations) {
        this.b2bRepository = b2bRepository;
        this.elasticsearchOperations = elasticsearchOperations;
    }

    public ScoreResult calculateBusinessScore(int n1, int n2, int n3, int n4, int n5,
                                              double k1, double k2, int m, double neutralRating) {
        int totalReviews = n1 + n2 + n3 + n4 + n5;

        if (totalReviews == 0) {
            return new ScoreResult(neutralRating * 20, "Average");
        }

        double weightedNumerator = (k1 * n1 + k2 * n2 + 3.0 * n3 + 4.0 * n4 + 5.0 * n5);
        double weightedDenominator = (k1 * n1 + k2 * n2 + n3 + n4 + n5);
        double weightedRating = weightedNumerator / weightedDenominator;

        double w = (double) totalReviews / (totalReviews + m);
        double score = (w * weightedRating + (1 - w) * neutralRating) * 100 / 5;

        String category = determineCategory(score);

        return new ScoreResult(Math.round(score * 100.0) / 100.0, category);
    }

    private String determineCategory(double score) {
        if (score >= 80) return "Excellent";
        if (score >= 60) return "Good";
        if (score >= 40) return "Average";
        if (score >= 20) return "Poor";
        return "Failing";
    }

    // OPTION 1: Batch Processing with saveAll()
    @Transactional
    public void calculateAndStoreScoresBatch() {
        final int PAGE_SIZE = 1000; // Ajustez selon vos besoins
        final int BATCH_SIZE = 1000;

        int page = 0;
        boolean hasMore = true;

        while (hasMore) {
            try {
                Pageable pageable = PageRequest.of(page, PAGE_SIZE);
                Page<B2B> b2bPage = b2bRepository.findAll(pageable);

                List<B2B> batch = new ArrayList<>();

                for (B2B b2b : b2bPage.getContent()) {
                    processB2BScore(b2b);
                    batch.add(b2b);

                    if (batch.size() >= BATCH_SIZE) {
                        b2bRepository.saveAll(batch);
                        batch.clear();
                    }
                }

                // Sauvegarder les éléments restants
                if (!batch.isEmpty()) {
                    b2bRepository.saveAll(batch);
                }

                hasMore = b2bPage.hasNext();
                page++;

                System.out.println("Page " + page + " traitée - " + b2bPage.getContent().size() + " éléments");

            } catch (Exception e) {
                System.err.println("Erreur lors du traitement de la page " + page + ": " + e.getMessage());
                throw e;
            }
        }
    }

    // OPTION 2: Bulk Update using Elasticsearch Operations (Fastest)
    public void calculateAndStoreScoresBulkUpdate() {
        final int BATCH_SIZE = 1000;

        List<UpdateQuery> updateQueries = new ArrayList<>();
        Iterable<B2B> allB2B = b2bRepository.findAll();

        for (B2B b2b : allB2B) {
            ScoreResult result = processB2BScoreCalculation(b2b);
            if (result != null) {
                Document document = Document.create()
                        .append("score", (int) result.getScore())
                        .append("scoreCategory", result.getCategory());

                UpdateQuery updateQuery = UpdateQuery.builder(b2b.getPlaceId())
                        .withDocument(document)
                        .build();

                updateQueries.add(updateQuery);

                if (updateQueries.size() >= BATCH_SIZE) {
                    elasticsearchOperations.bulkUpdate(updateQueries, B2B.class);
                    updateQueries.clear();
                }
            }
        }

        // Process remaining updates
        if (!updateQueries.isEmpty()) {
            elasticsearchOperations.bulkUpdate(updateQueries, B2B.class);
        }
    }

    // OPTION 3: Parallel Processing with CompletableFuture
    public void calculateAndStoreScoresParallel() {
        final int BATCH_SIZE = 500;

        List<B2B> allB2B = new ArrayList<>();
        b2bRepository.findAll().forEach(allB2B::add);

        // Split into batches and process in parallel
        List<List<B2B>> batches = partitionList(allB2B, BATCH_SIZE);

        List<CompletableFuture<Void>> futures = batches.stream()
                .map(batch -> CompletableFuture.runAsync(() -> processBatch(batch), executor))
                .collect(Collectors.toList());

        // Wait for all batches to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    private void processBatch(List<B2B> batch) {
        List<B2B> processedBatch = batch.stream()
                .peek(this::processB2BScore)
                .collect(Collectors.toList());

        b2bRepository.saveAll(processedBatch);
    }

    // OPTION 4: Stream Processing with Pagination (Memory Efficient)
    public void calculateAndStoreScoresWithPagination() {
        final int PAGE_SIZE = 1000;
        int page = 0;
        boolean hasMore = true;

        while (hasMore) {
            org.springframework.data.domain.Pageable pageable =
                    org.springframework.data.domain.PageRequest.of(page, PAGE_SIZE);

            org.springframework.data.domain.Page<B2B> b2bPage = b2bRepository.findAll(pageable);

            List<B2B> processedB2Bs = b2bPage.getContent().stream()
                    .peek(this::processB2BScore)
                    .collect(Collectors.toList());

            b2bRepository.saveAll(processedB2Bs);

            hasMore = b2bPage.hasNext();
            page++;
        }
    }

    private void processB2BScore(B2B b2b) {
        ScoreResult result = processB2BScoreCalculation(b2b);
        if (result != null) {
            b2b.setScore((int) result.getScore());
            b2b.setScoreCategory(result.getCategory());
        }
    }

    private ScoreResult processB2BScoreCalculation(B2B b2b) {
        Map<String, Integer> reviewMap = b2b.getReviewsPerRating();
        if (reviewMap == null) return null;

        int n1 = reviewMap.getOrDefault("1", 0);
        int n2 = reviewMap.getOrDefault("2", 0);
        int n3 = reviewMap.getOrDefault("3", 0);
        int n4 = reviewMap.getOrDefault("4", 0);
        int n5 = reviewMap.getOrDefault("5", 0);

        return calculateBusinessScore(n1, n2, n3, n4, n5, 4.0, 1.5, 10, 3.0);
    }

    private <T> List<List<T>> partitionList(List<T> list, int batchSize) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            partitions.add(list.subList(i, Math.min(i + batchSize, list.size())));
        }
        return partitions;
    }

    // Keep your original method for backward compatibility
    public void calculateAndStoreScores() {
        calculateAndStoreScoresBatch(); // Use optimized version by default
    }
}