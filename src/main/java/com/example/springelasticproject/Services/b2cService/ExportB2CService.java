package com.example.springelasticproject.Services.b2cService;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import com.example.springelasticproject.model.b2cModel.B2C;
import com.example.springelasticproject.repository.B2CRepository;
import com.opencsv.CSVWriter;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

@Service
public class ExportB2CService {
    private final B2CRepository b2CRepository;
    @Autowired
    private ElasticsearchOperations elasticsearchOperations;
    @Autowired
    public ExportB2CService(B2CRepository b2CRepository) {
        this.b2CRepository = b2CRepository;
    }

    @Async
    public void ExportSeachDataPS(Map<String, String> attributes, HttpServletResponse response) throws IOException {
        BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

        // Liste des champs à traiter automatiquement
        List<String> simpleFields = List.of("firstName", "lastName", "phoneNumber", "gender", "relationshipStatus",
                "currentCity", "currentCountry", "currentRegion", "hometownCity", "hometownCountry");

        // Champs avec traitement spécifique
        attributes.forEach((key, value) -> {
            if (value != null && !value.isEmpty()) {
                switch (key) {
                    case "email" -> {
                        boolQueryBuilder.must(q -> q.prefix(p -> p.field("email").value(value.toLowerCase())));
                    }
                    case "workplace" -> {
                        boolQueryBuilder.must(q -> q.match(m -> m.field("workplace").query(value).operator(Operator.And).fuzziness("1")));
                    }
                    case "currentDepartment" -> {
                        BoolQuery.Builder deptBoolBuilder = new BoolQuery.Builder();
                        if (!value.contains("-")) {
                            deptBoolBuilder.must(q -> q.matchPhrase(m -> m.field("currentDepartment.keyword").query(value)));
                            deptBoolBuilder.mustNot(q -> q.wildcard(w -> w.field("currentDepartment.keyword").wildcard("*-*")));
                        } else {
                            deptBoolBuilder.must(q -> q.matchPhrase(m -> m.field("currentDepartment.keyword").query(value)));
                        }
                        boolQueryBuilder.must(q -> q.bool(deptBoolBuilder.build()));
                    }
                    default -> {
                        if (simpleFields.contains(key)) {
                            boolQueryBuilder.must(q -> q.match(m -> m.field(key).query(value).operator(Operator.And)));
                        } else {
                            boolQueryBuilder.must(q -> q.match(m -> m.field(key).query(value)));
                        }
                    }
                }
            }
        });

        // Configuration HTTP
        response.setContentType("text/csv");
        response.setHeader("Content-Disposition", "attachment; filename=\"search-results.csv\"");

        try (CSVWriter csvWriter = new CSVWriter(response.getWriter())) {
            String[] header = {
                    "First Name", "Last Name", "Phone Number", "Email", "Workplace",
                    "Gender", "Relationship Status", "Current City", "Current Department",
                    "Current Region", "Current Country", "Hometown City", "Hometown Country"
            };
            csvWriter.writeNext(header);

            BoolQuery query = boolQueryBuilder.build();
            final int BATCH_SIZE = 500_000;
            int from = 0;
            boolean hasMoreResults = true;

            while (hasMoreResults) {
                NativeQuery searchQuery = NativeQuery.builder()
                        .withQuery(q -> q.bool(query))
                        .withSort(Sort.by(Sort.Direction.DESC, "_score"))
                        .withPageable(PageRequest.of(from / BATCH_SIZE, BATCH_SIZE))
                        .withTrackTotalHits(true)
                        .build();

                List<SearchHit<B2C>> hits = elasticsearchOperations.search(searchQuery, B2C.class).getSearchHits();

                if (hits.isEmpty()) {
                    hasMoreResults = false;
                    break;
                }

                for (SearchHit<B2C> hit : hits) {
                    B2C b2C = hit.getContent();
                    String[] row = {
                            nullSafe(b2C.getFirstName()), nullSafe(b2C.getLastName()),
                            nullSafe(b2C.getPhoneNumber()), nullSafe(b2C.getEmail()),
                            nullSafe(b2C.getWorkplace()), nullSafe(b2C.getGender()),
                            nullSafe(b2C.getRelationshipStatus()), nullSafe(b2C.getCurrentCity()),
                            nullSafe(b2C.getCurrentDepartment()), nullSafe(b2C.getCurrentRegion()),
                            nullSafe(b2C.getCurrentCountry()), nullSafe(b2C.getHometownCity()),
                            nullSafe(b2C.getHometownCountry())
                    };
                    csvWriter.writeNext(row);
                }

                if (hits.size() < BATCH_SIZE) {
                    hasMoreResults = false;
                } else {
                    from += BATCH_SIZE;
                }
            }

            System.out.println("Export terminé.");

        } catch (Exception e) {
            System.err.println("Erreur lors de l'export CSV : " + e.getMessage());
            e.printStackTrace();
        }
    }


    @Async
    public void ExportSeachDataMS(
            String gender,
            List<String> departments,
            List<String> regions,
            List<String> cities,
            Map<String, String> additionalAttributes,
            HttpServletResponse response) throws IOException {

        BoolQuery.Builder boolQuery = new BoolQuery.Builder();

        // Filtre simple sur le genre
        if (gender != null && !gender.isEmpty()) {
            boolQuery.must(q -> q.match(m -> m.field("gender").query(gender).operator(Operator.And)));
        }

        // Helper pour should query (regions, cities)
        BiConsumer<List<String>, String> applyShouldMatch = (values, field) -> {
            if (values != null && !values.isEmpty()) {
                BoolQuery.Builder shouldQuery = new BoolQuery.Builder();
                values.forEach(val -> shouldQuery.should(q -> q.match(m -> m.field(field).query(val).operator(Operator.And))));
                shouldQuery.minimumShouldMatch("1");
                boolQuery.must(q -> q.bool(shouldQuery.build()));
            }
        };

        applyShouldMatch.accept(regions, "currentRegion");
        applyShouldMatch.accept(cities, "currentCity");

        // Filtrage des départements
        if (departments != null && !departments.isEmpty()) {
            BoolQuery.Builder deptBool = new BoolQuery.Builder();
            for (String dep : departments) {
                BoolQuery.Builder sub = new BoolQuery.Builder();
                sub.must(q -> q.matchPhrase(mp -> mp.field("currentDepartment.keyword").query(dep)));
                if (!dep.contains("-")) {
                    sub.mustNot(q -> q.wildcard(wc -> wc.field("currentDepartment.keyword").wildcard("*-*")));
                }
                deptBool.should(q -> q.bool(sub.build()));
            }
            deptBool.minimumShouldMatch("1");
            boolQuery.must(q -> q.bool(deptBool.build()));
        }

        // Attributs additionnels
        if (additionalAttributes != null) {
            for (Map.Entry<String, String> entry : additionalAttributes.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (value == null || value.isEmpty()) continue;

                switch (key) {
                    case "firstName":
                    case "lastName":
                    case "currentCity":
                    case "currentCountry":
                    case "hometownCity":
                    case "hometownCountry":
                    case "relationshipStatus":
                    case "phoneNumber":
                        boolQuery.must(q -> q.match(m -> m.field(key).query(value).operator(Operator.And)));
                        break;
                    case "email":
                        boolQuery.must(q -> q.prefix(p -> p.field("email").value(value.toLowerCase())));
                        break;
                    case "workplace":
                        boolQuery.must(q -> q.match(m -> m.field("workplace").query(value).operator(Operator.And).fuzziness("1")));
                        break;
                }
            }
        }

        // Configuration de la réponse CSV
        response.setContentType("text/csv");
        response.setHeader("Content-Disposition", "attachment; filename=\"search-results.csv\"");

        try (CSVWriter csvWriter = new CSVWriter(response.getWriter())) {
            csvWriter.writeNext(new String[]{
                    "First Name", "Last Name", "Phone Number", "Email", "Workplace", "Gender",
                    "Relationship Status", "Current City", "Current Department", "Current Region",
                    "Current Country", "Hometown City", "Hometown Country"
            });

            final int BATCH_SIZE = 1_000_000;
            int page = 0;
            BoolQuery finalQuery = boolQuery.build();

            while (true) {
                NativeQuery searchQuery = NativeQuery.builder()
                        .withQuery(q -> q.bool(finalQuery))
                        .withSort(Sort.by(Sort.Direction.DESC, "_score"))
                        .withPageable(PageRequest.of(page, BATCH_SIZE))
                        .withTrackTotalHits(true)
                        .build();

                SearchHits<B2C> searchHits = elasticsearchOperations.search(searchQuery, B2C.class);
                List<SearchHit<B2C>> hits = searchHits.getSearchHits();
                if (hits.isEmpty()) break;

                for (SearchHit<B2C> hit : hits) {
                    B2C u = hit.getContent();
                    csvWriter.writeNext(new String[]{
                            nullSafe(u.getFirstName()), nullSafe(u.getLastName()), nullSafe(u.getPhoneNumber()),
                            nullSafe(u.getEmail()), nullSafe(u.getWorkplace()), nullSafe(u.getGender()),
                            nullSafe(u.getRelationshipStatus()), nullSafe(u.getCurrentCity()),
                            nullSafe(u.getCurrentDepartment()), nullSafe(u.getCurrentRegion()),
                            nullSafe(u.getCurrentCountry()), nullSafe(u.getHometownCity()),
                            nullSafe(u.getHometownCountry())
                    });
                }

                if (hits.size() < BATCH_SIZE) break;
                page++;
            }
        }
    }

    private String nullSafe(String value) {
        return value != null ? value : "";
    }
}


