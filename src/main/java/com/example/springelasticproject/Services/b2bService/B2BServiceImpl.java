package com.example.springelasticproject.Services.b2bService;

import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.json.JsonData;
import com.example.springelasticproject.model.b2bModel.B2B;
import com.example.springelasticproject.repository.B2BRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.IndexOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class B2BServiceImpl implements B2BService {

    private final B2BRepository B2BRepository;
    private final ElasticsearchOperations elasticsearchOperations;

    @Autowired
    public B2BServiceImpl(B2BRepository B2BRepository,
                                 ElasticsearchOperations elasticsearchOperations) {
        this.B2BRepository = B2BRepository;
        this.elasticsearchOperations = elasticsearchOperations;
    }

    @Override
    public B2B save(B2B b2B) {
        return B2BRepository.save(b2B);
    }

    @Override
    public Optional<B2B> findById(String id) {
        return B2BRepository.findById(id);
    }

    @Override
    public List<B2B> findAll() {
        List<B2B> shops = new ArrayList<>();
        B2BRepository.findAll().forEach(shops::add);
        return shops;
    }

    @Override
    public Page<B2B> findAll(Pageable pageable) {
        return B2BRepository.findAll(pageable);
    }

    @Override
    public void deleteById(String id) {
        B2BRepository.deleteById(id);
    }
    @Override
    public void deleteAll() {
        B2BRepository.deleteAll();
    }

    @Override
    public boolean existsById(String id) {
        return B2BRepository.existsById(id);
    }

    @Override
    public long count() {
        return B2BRepository.count();
    }

    @Override
    public List<B2B> findByName(String name) {
        return B2BRepository.findByNameContainingIgnoreCase(name);
    }

    @Override
    public List<B2B> findByMainCategory(String mainCategory) {
        return B2BRepository.findByMainCategory(mainCategory);
    }

    @Override
    public List<B2B> findByCity(String city) {
        return B2BRepository.findByCity(city);
    }

    @Override
    public List<B2B> findByPostalCode(String postalCode) {
        return B2BRepository.findByPostalCode(postalCode);
    }

    @Override
    public List<B2B> findByMinimumRating(Float rating) {
        return B2BRepository.findByRatingGreaterThanEqual(rating);
    }

    @Override
    public List<B2B> findByRatingRange(Float minRating, Float maxRating) {
        return B2BRepository.findByRatingBetween(minRating, maxRating);
    }

    @Override
    public List<B2B> findByMinimumReviews(Integer minReviews) {
        return B2BRepository.findByReviewsGreaterThanEqual(minReviews);
    }

    @Override
    public List<B2B> findOpenOn(String day) {
        return B2BRepository.findByClosedOnNotContaining(day);
    }

    @Override
    public List<B2B> findByPhone(String phone) {
        return B2BRepository.findByPhoneContaining(phone);
    }

    @Override
    public List<B2B> findByStatus(String status) {
        return B2BRepository.findByStatus(status);
    }

    @Override
    public List<B2B> searchByText(String searchText) {
        return B2BRepository.searchAcrossFields(searchText);
    }

    @Override
    public List<B2B> findNearbyShops(Double latitude, Double longitude, Double distanceInKm) {
        return B2BRepository.findByGeoDistance(distanceInKm, latitude, longitude);
    }

    @Override
    public List<B2B> findByCategory(String category) {
        return B2BRepository.findByCategoriesContaining(category);
    }

    @Override
    public void indexAllShops(List<B2B> shops) {
        B2BRepository.saveAll(shops);
    }

    @Override
    public void reindexAll() {
        deleteIndex();
        createIndex();
    }

    @Override
    public void deleteIndex() {
        IndexOperations indexOps = elasticsearchOperations.indexOps(B2B.class);
        if (indexOps.exists()) {
            indexOps.delete();
        }
    }

    @Override
    public void createIndex() {
        IndexOperations indexOps = elasticsearchOperations.indexOps(B2B.class);
        if (!indexOps.exists()) {
            indexOps.create();
            indexOps.putMapping(indexOps.createMapping());
        }
    }
    /*@Override
    public Map<String, Object> searchByAttributes04NoFuss(Map<String, String> attributes, Pageable pageable) {
        BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

        // Traitement spécial pour les champs courants
        if (attributes.containsKey("name") && !attributes.get("name").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("name")
                    .query(attributes.get("name"))
                    .operator(Operator.And)
                    .fuzziness("1") // Légère fuzziness pour flexibilité
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("query") && !attributes.get("query").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("query")
                    .query(attributes.get("query"))
                    .operator(Operator.And)
                    .fuzziness("2")
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Pour city (workplace)
        if (attributes.containsKey("city") && !attributes.get("city").isEmpty()) {
            String workplace = attributes.get("city");

            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("city")
                    .query(workplace)
                    .operator(Operator.And)
                    .fuzziness("1")
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // NOUVEAU: Recherche par rating (approximative)
        if (attributes.containsKey("rating") && !attributes.get("rating").isEmpty()) {
            try {
                double targetRating = Double.parseDouble(attributes.get("rating"));
                double tolerance = 0.5; // Tolérance de ±0.5 pour la recherche approximative

                RangeQuery rangeQuery = new RangeQuery.Builder()
                        .field("rating")
                        .gte(JsonData.of(targetRating - tolerance))
                        .lte(JsonData.of(targetRating + tolerance))
                        .build();

                boolQueryBuilder.must(new Query.Builder().range(rangeQuery).build());
            } catch (NumberFormatException e) {
                System.err.println("Format de rating invalide: " + attributes.get("rating"));
            }
        }

        // NOUVEAU: Recherche par score (approximative)
        if (attributes.containsKey("score") && !attributes.get("score").isEmpty()) {
            try {
                double targetScore = Double.parseDouble(attributes.get("score"));
                double tolerance = 5.0; // Tolérance de ±5 pour la recherche approximative

                RangeQuery rangeQuery = new RangeQuery.Builder()
                        .field("score")
                        .gte(JsonData.of(targetScore - tolerance))
                        .lte(JsonData.of(targetScore + tolerance))
                        .build();

                boolQueryBuilder.must(new Query.Builder().range(rangeQuery).build());
            } catch (NumberFormatException e) {
                System.err.println("Format de score invalide: " + attributes.get("score"));
            }
        }

        // NOUVEAU: Recherche par scoreCategory (exacte)
        if (attributes.containsKey("scoreCategory") && !attributes.get("scoreCategory").isEmpty()) {
            MatchQuery termQuery = new MatchQuery.Builder()
                    .field("scoreCategory") // Utiliser .keyword pour recherche exacte
                    .query(attributes.get("scoreCategory"))
                    .operator(Operator.And) // Assurer que la correspondance est exacte
                    .build();



            boolQueryBuilder.must(new Query.Builder().match(termQuery).build());
        }

// NOUVEAU: Recherche dans about - Version simplifiée sans nested
        if (attributes.containsKey("about") && !attributes.get("about").isEmpty()) {
            String aboutQuery = attributes.get("about");

            // Approche 1: Recherche simple dans tous les champs about
            BoolQuery.Builder aboutBoolQuery = new BoolQuery.Builder();

            // Recherche dans about.name
            MatchQuery aboutNameQuery = new MatchQuery.Builder()
                    .field("about.name")
                    .query(aboutQuery)
                    .fuzziness("1")
                    .build();

            // Recherche dans about.options.name
            MatchQuery aboutOptionsQuery = new MatchQuery.Builder()
                    .field("about.options.name")
                    .query(aboutQuery)
                    .fuzziness("1")
                    .build();

            // Recherche générale dans about (si c'est un champ texte)
            MatchQuery aboutGeneralQuery = new MatchQuery.Builder()
                    .field("about")
                    .query(aboutQuery)
                    .fuzziness("1")
                    .build();

            aboutBoolQuery.should(new Query.Builder().match(aboutNameQuery).build());
            aboutBoolQuery.should(new Query.Builder().match(aboutOptionsQuery).build());
            aboutBoolQuery.should(new Query.Builder().match(aboutGeneralQuery).build());

            boolQueryBuilder.must(new Query.Builder().bool(aboutBoolQuery.build()).build());
        }

        // NOUVEAU: Recherche par service spécifique (version simplifiée)
        if (attributes.containsKey("serviceName") && !attributes.get("serviceName").isEmpty()) {
            MatchQuery serviceQuery = new MatchQuery.Builder()
                    .field("about.name")
                    .query(attributes.get("serviceName"))
                    .operator(Operator.And)
                    .fuzziness("1")
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(serviceQuery).build());
        }

        // NOUVEAU: Recherche par option de service (version simplifiée)
        if (attributes.containsKey("serviceOption") && !attributes.get("serviceOption").isEmpty()) {
            MatchQuery optionQuery = new MatchQuery.Builder()
                    .field("about.options.name")
                    .query(attributes.get("serviceOption"))
                    .operator(Operator.And)
                    .fuzziness("1")
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(optionQuery).build());
        }


        // Traitement des autres attributs (inchangé)
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            // Exclure les champs déjà traités
            if (!key.equals("name") && !key.equals("city") && !key.equals("query")
                    && !key.equals("rating") && !key.equals("score") && !key.equals("scoreCategory")
                    && !key.equals("about") && !key.equals("serviceName") && !key.equals("serviceOption")
                    && value != null && !value.isEmpty()) {
                MatchQuery matchQuery = new MatchQuery.Builder()
                        .field(key)
                        .query(value)
                        .build();

                boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
            }
        }

        // Création de la requête finale
        BoolQuery builtBoolQuery = boolQueryBuilder.build();

        try {
            // Création de la requête avec pagination fournie par le client
            NativeQuery searchQuery = NativeQuery.builder()
                    .withQuery(q -> q.bool(builtBoolQuery))
                    .withPageable(pageable)
                    .withSort(Sort.by(Sort.Direction.DESC, "_score"))
                    .withTrackTotalHits(true)
                    .build();

            // Pour débogage - Affiche la requête générée
            // System.out.println("DEBUG - Requête générée: " + searchQuery.toString());

            // Exécuter la recherche
            SearchHits<B2B> searchHits = elasticsearchOperations.search(searchQuery, B2B.class);

            // Récupérer le nombre total de résultats
            long totalHits = searchHits.getTotalHits();

            // Récupérer les résultats
            List<B2B> usersOnPage = searchHits.getSearchHits().stream()
                    .map(SearchHit::getContent)
                    .collect(Collectors.toList());

            // Log pour débogage
            System.out.println("Page " + pageable.getPageNumber() + ": " + usersOnPage.size() + " résultats");
            System.out.println("Total résultats trouvés: " + totalHits);

            // Créer un Map contenant la page de résultats et le nombre total
            Map<String, Object> result = new HashMap<>();
            result.put("page", new PageImpl<>(usersOnPage, pageable, totalHits));
            result.put("totalResults", totalHits);

            return result;
        } catch (Exception e) {
            // Log détaillé de l'erreur
            System.err.println("Erreur lors de l'exécution de la requête Elasticsearch: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

     */
    @Override
    public Map<String, Object> searchByAttributes04Fuss(Map<String, String> attributes, Pageable pageable) {
        BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

        // ===================== CHAMPS TEXTE (avec analyseur français) =====================

        // Nom de l'entreprise
        if (attributes.containsKey("name") && !attributes.get("name").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("name")
                    .query(attributes.get("name"))
                    .operator(Operator.And)
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Description
        if (attributes.containsKey("description") && !attributes.get("description").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("description")
                    .query(attributes.get("description"))
                    .operator(Operator.Or) // Plus flexible pour description
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Catégorie principale
        if (attributes.containsKey("mainCategory") && !attributes.get("mainCategory").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("mainCategory")
                    .query(attributes.get("mainCategory"))
                    .operator(Operator.And)
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Categories (liste)
        if (attributes.containsKey("categories") && !attributes.get("categories").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("categories")
                    .query(attributes.get("categories"))
                    .operator(Operator.Or)
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Query (requête de recherche)
        if (attributes.containsKey("query") && !attributes.get("query").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("query")
                    .query(attributes.get("query"))
                    .operator(Operator.And)
                    .fuzziness("2")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // ===================== CHAMPS DE LOCALISATION =====================

        // Ville
        if (attributes.containsKey("city") && !attributes.get("city").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("city")
                    .query(attributes.get("city"))
                    .operator(Operator.And)
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Adresse complète
        if (attributes.containsKey("address") && !attributes.get("address").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("address")
                    .query(attributes.get("address"))
                    .operator(Operator.Or) // Plus flexible pour adresse
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Quartier
        if (attributes.containsKey("ward") && !attributes.get("ward").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("ward")
                    .query(attributes.get("ward"))
                    .operator(Operator.And)
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Rue
        if (attributes.containsKey("street") && !attributes.get("street").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("street")
                    .query(attributes.get("street"))
                    .operator(Operator.And)
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Code postal
        if (attributes.containsKey("postalCode") && !attributes.get("postalCode").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("postalCode") // Recherche exacte pour code postal
                    .query(attributes.get("postalCode"))
                    .operator(Operator.And) // Assurer que la correspondance est exacte
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // État/Région
        if (attributes.containsKey("state") && !attributes.get("state").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("state")
                    .query(attributes.get("state"))
                    .operator(Operator.And)
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Code pays
        if (attributes.containsKey("countryCode") && !attributes.get("countryCode").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("countryCode") // Recherche exacte pour code pays
                    .query(attributes.get("countryCode"))
                    .operator(Operator.And) // Assurer que la correspondance est exacte
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Fuseau horaire
        if (attributes.containsKey("timeZone") && !attributes.get("timeZone").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("timeZone")
                    .query(attributes.get("timeZone"))
                    .operator(Operator.And)
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Plus Code
        if (attributes.containsKey("plusCode") && !attributes.get("plusCode").isEmpty()) {
            MatchQuery termQuery = new MatchQuery.Builder()
                    .field("plusCode")
                    .query(attributes.get("plusCode"))
                    .operator(Operator.And) // Assurer que la correspondance est exacte
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(termQuery).build());
        }

        // ===================== RECHERCHE PAR INTERVALLE (RANGE) =====================

        // Recherche par rating avec intervalle
        if ((attributes.containsKey("ratingMin") || attributes.containsKey("ratingMax")) &&
                (attributes.get("ratingMin") != null && !attributes.get("ratingMin").isEmpty() ||
                        attributes.get("ratingMax") != null && !attributes.get("ratingMax").isEmpty())) {

            try {
                RangeQuery.Builder rangeBuilder = new RangeQuery.Builder().field("rating");

                // Rating minimum
                if (attributes.containsKey("ratingMin") && !attributes.get("ratingMin").isEmpty()) {
                    double minRating = Double.parseDouble(attributes.get("ratingMin"));
                    rangeBuilder.gte(JsonData.of(minRating));
                }

                // Rating maximum
                if (attributes.containsKey("ratingMax") && !attributes.get("ratingMax").isEmpty()) {
                    double maxRating = Double.parseDouble(attributes.get("ratingMax"));
                    rangeBuilder.lte(JsonData.of(maxRating));
                }

                boolQueryBuilder.must(new Query.Builder().range(rangeBuilder.build()).build());

            } catch (NumberFormatException e) {
                System.err.println("Format de rating invalide dans l'intervalle: " + e.getMessage());
            }
        }

        // Recherche par rating unique (pour compatibilité arrière)
        else if (attributes.containsKey("rating") && !attributes.get("rating").isEmpty()) {
            try {
                double targetRating = Double.parseDouble(attributes.get("rating"));
                double tolerance = 0.2; // Tolérance réduite de ±0.2

                RangeQuery rangeQuery = new RangeQuery.Builder()
                        .field("rating")
                        .gte(JsonData.of(targetRating - tolerance))
                        .lte(JsonData.of(targetRating + tolerance))
                        .build();

                boolQueryBuilder.must(new Query.Builder().range(rangeQuery).build());
            } catch (NumberFormatException e) {
                System.err.println("Format de rating invalide: " + attributes.get("rating"));
            }
        }

        // Recherche par score avec intervalle
        if ((attributes.containsKey("scoreMin") || attributes.containsKey("scoreMax")) &&
                (attributes.get("scoreMin") != null && !attributes.get("scoreMin").isEmpty() ||
                        attributes.get("scoreMax") != null && !attributes.get("scoreMax").isEmpty())) {

            try {
                RangeQuery.Builder rangeBuilder = new RangeQuery.Builder().field("score");

                // Score minimum
                if (attributes.containsKey("scoreMin") && !attributes.get("scoreMin").isEmpty()) {
                    int minScore = Integer.parseInt(attributes.get("scoreMin"));
                    rangeBuilder.gte(JsonData.of(minScore));
                }

                // Score maximum
                if (attributes.containsKey("scoreMax") && !attributes.get("scoreMax").isEmpty()) {
                    int maxScore = Integer.parseInt(attributes.get("scoreMax"));
                    rangeBuilder.lte(JsonData.of(maxScore));
                }

                boolQueryBuilder.must(new Query.Builder().range(rangeBuilder.build()).build());

            } catch (NumberFormatException e) {
                System.err.println("Format de score invalide dans l'intervalle: " + e.getMessage());
            }
        }

        // Recherche par score unique (pour compatibilité arrière)
        else if (attributes.containsKey("score") && !attributes.get("score").isEmpty()) {
            try {
                int targetScore = Integer.parseInt(attributes.get("score"));
                int tolerance = 3; // Tolérance réduite de ±3

                RangeQuery rangeQuery = new RangeQuery.Builder()
                        .field("score")
                        .gte(JsonData.of(targetScore - tolerance))
                        .lte(JsonData.of(targetScore + tolerance))
                        .build();

                boolQueryBuilder.must(new Query.Builder().range(rangeQuery).build());
            } catch (NumberFormatException e) {
                System.err.println("Format de score invalide: " + attributes.get("score"));
            }
        }

        // NOUVEAU: Recherche par scoreCategory (exacte)
        if (attributes.containsKey("scoreCategory") && !attributes.get("scoreCategory").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("scoreCategory")
                    .query(attributes.get("scoreCategory"))
                    .operator(Operator.And) // Assurer que la correspondance est exacte
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // NOUVEAU: Recherche dans about - Version simplifiée sans nested
        if (attributes.containsKey("about") && !attributes.get("about").isEmpty()) {
            String aboutQuery = attributes.get("about");

            // Approche 1: Recherche simple dans tous les champs about
            BoolQuery.Builder aboutBoolQuery = new BoolQuery.Builder();

            // Recherche dans about.name
            MatchQuery aboutNameQuery = new MatchQuery.Builder()
                    .field("about.name")
                    .query(aboutQuery)
                    .fuzziness("1")
                    .build();

            // Recherche dans about.options.name
            MatchQuery aboutOptionsQuery = new MatchQuery.Builder()
                    .field("about.options.name")
                    .query(aboutQuery)
                    .fuzziness("1")
                    .build();

            // Recherche générale dans about (si c'est un champ texte)
            MatchQuery aboutGeneralQuery = new MatchQuery.Builder()
                    .field("about")
                    .query(aboutQuery)
                    .fuzziness("1")
                    .build();

            aboutBoolQuery.should(new Query.Builder().match(aboutNameQuery).build());
            aboutBoolQuery.should(new Query.Builder().match(aboutOptionsQuery).build());
            aboutBoolQuery.should(new Query.Builder().match(aboutGeneralQuery).build());

            boolQueryBuilder.must(new Query.Builder().bool(aboutBoolQuery.build()).build());
        }

        // NOUVEAU: Recherche par service spécifique (version simplifiée)
        if (attributes.containsKey("serviceName") && !attributes.get("serviceName").isEmpty()) {
            MatchQuery serviceQuery = new MatchQuery.Builder()
                    .field("about.name")
                    .query(attributes.get("serviceName"))
                    .operator(Operator.And)
                    .fuzziness("1")
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(serviceQuery).build());
        }

        // NOUVEAU: Recherche par option de service (version simplifiée)
        if (attributes.containsKey("serviceOption") && !attributes.get("serviceOption").isEmpty()) {
            MatchQuery optionQuery = new MatchQuery.Builder()
                    .field("about.options.name")
                    .query(attributes.get("serviceOption"))
                    .operator(Operator.And)
                    .fuzziness("1")
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(optionQuery).build());
        }

        // Traitement des autres attributs (inchangé)
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            // Exclure tous les champs déjà traités
            if (!key.equals("name") && !key.equals("description") && !key.equals("mainCategory")
                    && !key.equals("categories") && !key.equals("query") && !key.equals("city")
                    && !key.equals("address") && !key.equals("ward") && !key.equals("street")
                    && !key.equals("postalCode") && !key.equals("state") && !key.equals("countryCode")
                    && !key.equals("timeZone") && !key.equals("plusCode")
                    && !key.equals("rating") && !key.equals("ratingMin") && !key.equals("ratingMax")
                    && !key.equals("score") && !key.equals("scoreMin") && !key.equals("scoreMax")
                    && !key.equals("scoreCategory")
                    && !key.equals("about") && !key.equals("serviceName") && !key.equals("serviceOption")
                    && value != null && !value.isEmpty()) {
                MatchQuery matchQuery = new MatchQuery.Builder()
                        .field(key)
                        .query(value)
                        .build();

                boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
            }
        }

        // Création de la requête finale
        BoolQuery builtBoolQuery = boolQueryBuilder.build();

        try {
            // Création de la requête avec pagination fournie par le client
            NativeQuery searchQuery = NativeQuery.builder()
                    .withQuery(q -> q.bool(builtBoolQuery))
                    .withPageable(pageable)
                    .withSort(Sort.by(Sort.Direction.DESC, "_score"))
                    .withTrackTotalHits(true)
                    .build();

            // Pour débogage - Affiche la requête générée
            // System.out.println("DEBUG - Requête générée: " + searchQuery.toString());

            // Exécuter la recherche
            SearchHits<B2B> searchHits = elasticsearchOperations.search(searchQuery, B2B.class);

            // Récupérer le nombre total de résultats
            long totalHits = searchHits.getTotalHits();

            // Récupérer les résultats
            List<B2B> usersOnPage = searchHits.getSearchHits().stream()
                    .map(SearchHit::getContent)
                    .collect(Collectors.toList());

            // Log pour débogage
            System.out.println("Page " + pageable.getPageNumber() + ": " + usersOnPage.size() + " résultats");
            System.out.println("Total résultats trouvés: " + totalHits);

            // Créer un Map contenant la page de résultats et le nombre total
            Map<String, Object> result = new HashMap<>();
            result.put("page", new PageImpl<>(usersOnPage, pageable, totalHits));
            result.put("totalResults", totalHits);

            return result;
        } catch (Exception e) {
            // Log détaillé de l'erreur
            System.err.println("Erreur lors de l'exécution de la requête Elasticsearch: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }
    @Override
    public Map<String, Object> searchByAttributes04Exact(Map<String, String> attributes, Pageable pageable) {
        BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

        // ===================== CHAMPS TEXTE (recherche exacte) =====================

        // Nom de l'entreprise
        if (attributes.containsKey("name") && !attributes.get("name").isEmpty()) {
            MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery.Builder()
                    .field("name")
                    .query(attributes.get("name"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().matchPhrase(matchPhraseQuery).build());
        }

        // Description
        if (attributes.containsKey("description") && !attributes.get("description").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("description")
                    .query(attributes.get("description"))
                    .operator(Operator.And) // Tous les mots doivent être présents
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Catégorie principale
        if (attributes.containsKey("mainCategory") && !attributes.get("mainCategory").isEmpty()) {
            MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery.Builder()
                    .field("mainCategory")
                    .query(attributes.get("mainCategory"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().matchPhrase(matchPhraseQuery).build());
        }

        // Categories (liste)
        if (attributes.containsKey("categories") && !attributes.get("categories").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("categories")
                    .query(attributes.get("categories"))
                    .operator(Operator.And) // Tous les mots doivent être présents
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Query (requête de recherche)
        if (attributes.containsKey("query") && !attributes.get("query").isEmpty()) {
            MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery.Builder()
                    .field("query")
                    .query(attributes.get("query"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().matchPhrase(matchPhraseQuery).build());
        }

        // ===================== CHAMPS DE LOCALISATION =====================

        // Ville
        if (attributes.containsKey("city") && !attributes.get("city").isEmpty()) {
            MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery.Builder()
                    .field("city")
                    .query(attributes.get("city"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().matchPhrase(matchPhraseQuery).build());
        }

        // Adresse complète
        if (attributes.containsKey("address") && !attributes.get("address").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("address")
                    .query(attributes.get("address"))
                    .operator(Operator.And) // Tous les mots doivent être présents
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Quartier
        if (attributes.containsKey("ward") && !attributes.get("ward").isEmpty()) {
            MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery.Builder()
                    .field("ward")
                    .query(attributes.get("ward"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().matchPhrase(matchPhraseQuery).build());
        }

        // Rue
        if (attributes.containsKey("street") && !attributes.get("street").isEmpty()) {
            MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery.Builder()
                    .field("street")
                    .query(attributes.get("street"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().matchPhrase(matchPhraseQuery).build());
        }

        // Code postal (recherche exacte)
        if (attributes.containsKey("postalCode") && !attributes.get("postalCode").isEmpty()) {
            TermQuery termQuery = new TermQuery.Builder()
                    .field("postalCode")
                    .value(attributes.get("postalCode"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().term(termQuery).build());
        }

        // État/Région
        if (attributes.containsKey("state") && !attributes.get("state").isEmpty()) {
            MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery.Builder()
                    .field("state")
                    .query(attributes.get("state"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().matchPhrase(matchPhraseQuery).build());
        }

        // Code pays (recherche exacte)
        if (attributes.containsKey("countryCode") && !attributes.get("countryCode").isEmpty()) {
            TermQuery termQuery = new TermQuery.Builder()
                    .field("countryCode")
                    .value(attributes.get("countryCode"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().term(termQuery).build());
        }

        // Fuseau horaire
        if (attributes.containsKey("timeZone") && !attributes.get("timeZone").isEmpty()) {
            MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery.Builder()
                    .field("timeZone")
                    .query(attributes.get("timeZone"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().matchPhrase(matchPhraseQuery).build());
        }

        // Plus Code (recherche exacte)
        if (attributes.containsKey("plusCode") && !attributes.get("plusCode").isEmpty()) {
            TermQuery termQuery = new TermQuery.Builder()
                    .field("plusCode")
                    .value(attributes.get("plusCode"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().term(termQuery).build());
        }

        // ===================== RECHERCHE PAR INTERVALLE (RANGE) =====================

        // Recherche par rating avec intervalle
        if ((attributes.containsKey("ratingMin") || attributes.containsKey("ratingMax")) &&
                (attributes.get("ratingMin") != null && !attributes.get("ratingMin").isEmpty() ||
                        attributes.get("ratingMax") != null && !attributes.get("ratingMax").isEmpty())) {

            try {
                RangeQuery.Builder rangeBuilder = new RangeQuery.Builder().field("rating");

                // Rating minimum
                if (attributes.containsKey("ratingMin") && !attributes.get("ratingMin").isEmpty()) {
                    double minRating = Double.parseDouble(attributes.get("ratingMin"));
                    rangeBuilder.gte(JsonData.of(minRating));
                }

                // Rating maximum
                if (attributes.containsKey("ratingMax") && !attributes.get("ratingMax").isEmpty()) {
                    double maxRating = Double.parseDouble(attributes.get("ratingMax"));
                    rangeBuilder.lte(JsonData.of(maxRating));
                }

                boolQueryBuilder.must(new Query.Builder().range(rangeBuilder.build()).build());

            } catch (NumberFormatException e) {
                System.err.println("Format de rating invalide dans l'intervalle: " + e.getMessage());
            }
        }

        // Recherche par rating unique (pour compatibilité arrière)
        else if (attributes.containsKey("rating") && !attributes.get("rating").isEmpty()) {
            try {
                double targetRating = Double.parseDouble(attributes.get("rating"));
                // Pour recherche exacte, pas de tolérance
                TermQuery termQuery = new TermQuery.Builder()
                        .field("rating")
                        .value(targetRating)
                        .build();
                boolQueryBuilder.must(new Query.Builder().term(termQuery).build());
            } catch (NumberFormatException e) {
                System.err.println("Format de rating invalide: " + attributes.get("rating"));
            }
        }

        // Recherche par score avec intervalle
        if ((attributes.containsKey("scoreMin") || attributes.containsKey("scoreMax")) &&
                (attributes.get("scoreMin") != null && !attributes.get("scoreMin").isEmpty() ||
                        attributes.get("scoreMax") != null && !attributes.get("scoreMax").isEmpty())) {

            try {
                RangeQuery.Builder rangeBuilder = new RangeQuery.Builder().field("score");

                // Score minimum
                if (attributes.containsKey("scoreMin") && !attributes.get("scoreMin").isEmpty()) {
                    int minScore = Integer.parseInt(attributes.get("scoreMin"));
                    rangeBuilder.gte(JsonData.of(minScore));
                }

                // Score maximum
                if (attributes.containsKey("scoreMax") && !attributes.get("scoreMax").isEmpty()) {
                    int maxScore = Integer.parseInt(attributes.get("scoreMax"));
                    rangeBuilder.lte(JsonData.of(maxScore));
                }

                boolQueryBuilder.must(new Query.Builder().range(rangeBuilder.build()).build());

            } catch (NumberFormatException e) {
                System.err.println("Format de score invalide dans l'intervalle: " + e.getMessage());
            }
        }

        // Recherche par score unique (pour compatibilité arrière)
        else if (attributes.containsKey("score") && !attributes.get("score").isEmpty()) {
            try {
                int targetScore = Integer.parseInt(attributes.get("score"));
                // Pour recherche exacte, pas de tolérance
                TermQuery termQuery = new TermQuery.Builder()
                        .field("score")
                        .value(targetScore)
                        .build();
                boolQueryBuilder.must(new Query.Builder().term(termQuery).build());
            } catch (NumberFormatException e) {
                System.err.println("Format de score invalide: " + attributes.get("score"));
            }
        }

        // Recherche par scoreCategory (exacte)
        // NOUVEAU: Recherche par scoreCategory (exacte)
        if (attributes.containsKey("scoreCategory") && !attributes.get("scoreCategory").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("scoreCategory")
                    .query(attributes.get("scoreCategory"))
                    .operator(Operator.And) // Assurer que la correspondance est exacte
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Recherche dans about - Version exacte
        if (attributes.containsKey("about") && !attributes.get("about").isEmpty()) {
            String aboutQuery = attributes.get("about");

            BoolQuery.Builder aboutBoolQuery = new BoolQuery.Builder();

            // Recherche exacte dans about.name
            MatchPhraseQuery aboutNameQuery = new MatchPhraseQuery.Builder()
                    .field("about.name")
                    .query(aboutQuery)
                    .build();

            // Recherche exacte dans about.options.name
            MatchPhraseQuery aboutOptionsQuery = new MatchPhraseQuery.Builder()
                    .field("about.options.name")
                    .query(aboutQuery)
                    .build();

            // Recherche générale dans about avec tous les mots
            MatchQuery aboutGeneralQuery = new MatchQuery.Builder()
                    .field("about")
                    .query(aboutQuery)
                    .operator(Operator.And)
                    .build();

            aboutBoolQuery.should(new Query.Builder().matchPhrase(aboutNameQuery).build());
            aboutBoolQuery.should(new Query.Builder().matchPhrase(aboutOptionsQuery).build());
            aboutBoolQuery.should(new Query.Builder().match(aboutGeneralQuery).build());

            boolQueryBuilder.must(new Query.Builder().bool(aboutBoolQuery.build()).build());
        }

        // Recherche par service spécifique (exacte)
        if (attributes.containsKey("serviceName") && !attributes.get("serviceName").isEmpty()) {
            MatchPhraseQuery serviceQuery = new MatchPhraseQuery.Builder()
                    .field("about.name")
                    .query(attributes.get("serviceName"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().matchPhrase(serviceQuery).build());
        }

        // Recherche par option de service (exacte)
        if (attributes.containsKey("serviceOption") && !attributes.get("serviceOption").isEmpty()) {
            MatchPhraseQuery optionQuery = new MatchPhraseQuery.Builder()
                    .field("about.options.name")
                    .query(attributes.get("serviceOption"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().matchPhrase(optionQuery).build());
        }

        // Traitement des autres attributs avec recherche exacte
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            // Exclure tous les champs déjà traités
            if (!key.equals("name") && !key.equals("description") && !key.equals("mainCategory")
                    && !key.equals("categories") && !key.equals("query") && !key.equals("city")
                    && !key.equals("address") && !key.equals("ward") && !key.equals("street")
                    && !key.equals("postalCode") && !key.equals("state") && !key.equals("countryCode")
                    && !key.equals("timeZone") && !key.equals("plusCode")
                    && !key.equals("rating") && !key.equals("ratingMin") && !key.equals("ratingMax")
                    && !key.equals("score") && !key.equals("scoreMin") && !key.equals("scoreMax")
                    && !key.equals("scoreCategory")
                    && !key.equals("about") && !key.equals("serviceName") && !key.equals("serviceOption")
                    && value != null && !value.isEmpty()) {

                // Utiliser match_phrase pour une recherche plus exacte
                MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery.Builder()
                        .field(key)
                        .query(value)
                        .build();
                boolQueryBuilder.must(new Query.Builder().matchPhrase(matchPhraseQuery).build());
            }
        }

        // Création de la requête finale
        BoolQuery builtBoolQuery = boolQueryBuilder.build();

        try {
            // Création de la requête avec pagination fournie par le client
            NativeQuery searchQuery = NativeQuery.builder()
                    .withQuery(q -> q.bool(builtBoolQuery))
                    .withPageable(pageable)
                    .withSort(Sort.by(Sort.Direction.DESC, "_score"))
                    .withTrackTotalHits(true)
                    .build();

            // Pour débogage - Affiche la requête générée
            // System.out.println("DEBUG - Requête exacte générée: " + searchQuery.toString());

            // Exécuter la recherche
            SearchHits<B2B> searchHits = elasticsearchOperations.search(searchQuery, B2B.class);

            // Récupérer le nombre total de résultats
            long totalHits = searchHits.getTotalHits();

            // Récupérer les résultats
            List<B2B> usersOnPage = searchHits.getSearchHits().stream()
                    .map(SearchHit::getContent)
                    .collect(Collectors.toList());

            // Log pour débogage
            System.out.println("Page " + pageable.getPageNumber() + ": " + usersOnPage.size() + " résultats (recherche exacte)");
            System.out.println("Total résultats trouvés: " + totalHits);

            // Créer un Map contenant la page de résultats et le nombre total
            Map<String, Object> result = new HashMap<>();
            result.put("page", new PageImpl<>(usersOnPage, pageable, totalHits));
            result.put("totalResults", totalHits);

            return result;
        } catch (Exception e) {
            // Log détaillé de l'erreur
            System.err.println("Erreur lors de l'exécution de la requête Elasticsearch exacte: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

}