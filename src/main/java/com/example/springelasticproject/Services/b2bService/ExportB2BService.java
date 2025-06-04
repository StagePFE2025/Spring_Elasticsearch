package com.example.springelasticproject.Services.b2bService;

import com.example.springelasticproject.model.b2bModel.B2B;
import com.example.springelasticproject.repository.B2BRepository;
import com.opencsv.CSVWriter;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;


import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Operator;

import java.util.List;
import java.util.Map;

@Service
public class ExportB2BService {
    private final B2BRepository b2BRepository;
    @Autowired
    private ElasticsearchOperations elasticsearchOperations;

    @Autowired
    public ExportB2BService(B2BRepository b2BRepository) {
        this.b2BRepository = b2BRepository;
    }



    @Async
    public void ExportSearchDataB2B(Map<String, String> attributes, HttpServletResponse response) throws IOException {

        // Configuration HTTP d'abord
        response.setContentType("text/csv");
        response.setHeader("Content-Disposition", "attachment; filename=\"b2b-search-results.csv\"");

        try (CSVWriter csvWriter = new CSVWriter(response.getWriter())) {
            String[] header = {
                    "Name", "Description", "Main Category", "Address", "City",
                    "State", "Postal Code", "Country Code", "Phone", "Website",
                    "Rating", "Reviews", "Price Range", "Status", "Latitude", "Longitude"
            };
            csvWriter.writeNext(header);

            try {
                // Construire la requête avec une approche plus simple
                BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();
                boolean hasFilters = false;

                // Traitement pour le champ name
                if (attributes.containsKey("name") && attributes.get("name") != null && !attributes.get("name").trim().isEmpty()) {
                    String nameValue = attributes.get("name").trim();
                    boolQueryBuilder.must(q -> q.match(m -> m
                            .field("name")
                            .query(nameValue)
                            .operator(Operator.And)
                            .fuzziness("1")
                    ));
                    hasFilters = true;
                }

                // Traitement pour le champ city
                if (attributes.containsKey("city") && attributes.get("city") != null && !attributes.get("city").trim().isEmpty()) {
                    String cityValue = attributes.get("city").trim();
                    boolQueryBuilder.must(q -> q.match(m -> m
                            .field("city")
                            .query(cityValue)
                            .operator(Operator.And)
                            .fuzziness("1")
                    ));
                    hasFilters = true;
                }

                // Traitement pour le champ query
                if (attributes.containsKey("query") && attributes.get("query") != null && !attributes.get("query").trim().isEmpty()) {
                    String queryValue = attributes.get("query").trim();
                    boolQueryBuilder.must(q -> q.match(m -> m
                            .field("query")
                            .query(queryValue)
                            .operator(Operator.And)
                            .fuzziness("2")
                    ));
                    hasFilters = true;
                }

                // Si aucun filtre n'est appliqué, utiliser match_all
                if (!hasFilters) {
                    boolQueryBuilder.must(q -> q.matchAll(ma -> ma));
                }

                BoolQuery query = boolQueryBuilder.build();
                final int BATCH_SIZE = 1000; // Commencer avec une taille plus petite
                int from = 0;
                boolean hasMoreResults = true;
                int totalProcessed = 0;

                System.out.println("Début de l'export B2B...");

                while (hasMoreResults) {
                    try {
                        // Créer la requête native avec des paramètres sûrs
                        NativeQuery searchQuery = NativeQuery.builder()
                                .withQuery(q -> q.bool(query))
                                .withPageable(PageRequest.of(from / BATCH_SIZE, BATCH_SIZE))
                                .withTrackTotalHits(false) // Désactiver le comptage total pour éviter les erreurs
                                .build();

                        System.out.println("Recherche batch " + (from / BATCH_SIZE + 1) + "...");

                        List<SearchHit<B2B>> hits = elasticsearchOperations.search(searchQuery, B2B.class).getSearchHits();

                        if (hits.isEmpty()) {
                            hasMoreResults = false;
                            break;
                        }

                        for (SearchHit<B2B> hit : hits) {
                            B2B b2b = hit.getContent();
                            if (b2b != null) { // Vérification de sécurité
                                String[] row = {
                                        nullSafe(b2b.getName()),
                                        nullSafe(b2b.getDescription()),
                                        nullSafe(b2b.getMainCategory()),
                                        nullSafe(b2b.getAddress()),
                                        nullSafe(b2b.getCity()),
                                        nullSafe(b2b.getState()),
                                        nullSafe(b2b.getPostalCode()),
                                        nullSafe(b2b.getCountryCode()),
                                        nullSafe(b2b.getPhone()),
                                        nullSafe(b2b.getWebsite()),
                                        b2b.getRating() != null ? b2b.getRating().toString() : "",
                                        b2b.getReviews() != null ? b2b.getReviews().toString() : "",
                                        nullSafe(b2b.getPriceRange()),
                                        nullSafe(b2b.getStatus()),
                                        b2b.getLatitude() != null ? b2b.getLatitude().toString() : "",
                                        b2b.getLongitude() != null ? b2b.getLongitude().toString() : ""
                                };
                                csvWriter.writeNext(row);
                                totalProcessed++;
                            }
                        }

                        // Vérifier s'il y a plus de résultats
                        if (hits.size() < BATCH_SIZE) {
                            hasMoreResults = false;
                        } else {
                            from += BATCH_SIZE;
                        }

                        // Limiter le nombre total d'enregistrements pour éviter les timeouts
                        if (totalProcessed >= 100000) { // Limite de sécurité
                            System.out.println("Limite de 100,000 enregistrements atteinte, arrêt de l'export.");
                            break;
                        }

                    } catch (Exception batchException) {
                        System.err.println("Erreur lors du traitement du batch " + (from / BATCH_SIZE + 1) + ": " + batchException.getMessage());
                        // Continuer avec le batch suivant ou arrêter selon le type d'erreur
                        if (batchException.getMessage().contains("search_phase_execution_exception")) {
                            System.err.println("Erreur critique, arrêt de l'export.");
                            break;
                        }
                        from += BATCH_SIZE; // Passer au batch suivant
                    }
                }

                System.out.println("Export B2B terminé avec succès. Total: " + totalProcessed + " enregistrements.");

                // Si aucun enregistrement n'a été trouvé, ajouter une ligne d'information
                if (totalProcessed == 0) {
                    String[] noResultRow = {"Aucun résultat trouvé", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""};
                    csvWriter.writeNext(noResultRow);
                }

            } catch (Exception e) {
                System.err.println("Erreur lors de l'export B2B : " + e.getMessage());
                e.printStackTrace();

                // Écrire l'erreur dans le CSV
                String[] errorRow = {"Erreur: " + e.getMessage(), "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""};
                csvWriter.writeNext(errorRow);
            }

        } catch (IOException ioException) {
            System.err.println("Erreur d'écriture CSV : " + ioException.getMessage());
            throw ioException;
        }
    }

    // Helper method
    private String nullSafe(String value) {
        return value != null ? value.trim() : "";
    }
}


