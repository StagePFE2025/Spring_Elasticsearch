package com.example.springelasticproject.Services.b2cService;

import co.elastic.clients.elasticsearch._types.query_dsl.*;
import com.example.springelasticproject.model.b2cModel.B2C;
import com.example.springelasticproject.repository.B2CRepository;
import com.opencsv.CSVReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.*;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Service
public class B2CService {

    private final B2CRepository b2CRepository;

    @Autowired
    private ElasticsearchOperations elasticsearchOperations;

    @Autowired
    public B2CService(B2CRepository b2CRepository) {
        this.b2CRepository = b2CRepository;
    }

    // CRUD operations

    // Créer ou mettre à jour un utilisateur
    public B2C saveUser(B2C b2C) {
        return b2CRepository.save(b2C);
    }

    // Trouver un utilisateur par ID
    public Optional<B2C> findUserById(Long idS) {
        return b2CRepository.findById(idS);
    }

    // Trouver tous les utilisateurs
    public Iterable<B2C> findAllUsers() {
        return b2CRepository.findAll();
    }

    // Supprimer un utilisateur par ID
    public void deleteUser(Long idS) {
        b2CRepository.deleteById(idS);
    }

    // Recherche par prénom et/ou nom
    public List<B2C> searchUsersByFirstNameOrLastName(String firstName, String lastName) {
        if (firstName != null && lastName != null) {
            return b2CRepository.findByFirstNameContainingOrLastNameContaining(firstName, lastName);
        } else if (firstName != null) {
            return b2CRepository.findByFirstNameContaining(firstName);
        } else if (lastName != null) {
            return b2CRepository.findByLastNameContaining(lastName);
        }
        return List.of(); // Si aucun paramètre de recherche n'est fourni
    }

    @Async
    public void deleteAllUsers() {
        b2CRepository.deleteAll();
    }

    public Integer countUsers() {
        return (int) b2CRepository.count();
    }

    public List<B2C> searchByAttributes(Map<String, String> attributes) {
        BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

        // Traitement spécial pour les champs courants
        if (attributes.containsKey("firstName") && !attributes.get("firstName").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("firstName")
                    .query(attributes.get("firstName"))
                    .fuzziness("AUTO")
                    .prefixLength(2)
                    .maxExpansions(10)
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("lastName") && !attributes.get("lastName").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("lastName")
                    .query(attributes.get("lastName"))
                    .fuzziness("AUTO")
                    .prefixLength(2)
                    .maxExpansions(10)
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("email") && !attributes.get("email").isEmpty()) {
            String email = attributes.get("email").toLowerCase();

            // Term query for exact match
            TermQuery termQuery = new TermQuery.Builder()
                    .field("email.keyword")
                    .value(email)
                    .build();

            // Prefix query for partial match
            PrefixQuery prefixQuery = new PrefixQuery.Builder()
                    .field("email")
                    .value(email)
                    .build();

            // Combine with should
            BoolQuery.Builder emailBoolBuilder = new BoolQuery.Builder()
                    .should(new Query.Builder().term(termQuery).build())
                    .should(new Query.Builder().prefix(prefixQuery).build())
                    .minimumShouldMatch("1");

            boolQueryBuilder.must(new Query.Builder().bool(emailBoolBuilder.build()).build());
        }

        if (attributes.containsKey("currentCity") && !attributes.get("currentCity").isEmpty()) {
            String city = attributes.get("currentCity").toLowerCase();

            // Match query with fuzziness
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("currentCity")
                    .query(city)
                    .fuzziness("AUTO")
                    .build();

            // Wildcard query
            WildcardQuery wildcardQuery = new WildcardQuery.Builder()
                    .field("currentCity")
                    .wildcard("*" + city + "*")
                    .build();

            // Combine with should
            BoolQuery.Builder cityBoolBuilder = new BoolQuery.Builder()
                    .should(new Query.Builder().match(matchQuery).build())
                    .should(new Query.Builder().wildcard(wildcardQuery).build())
                    .minimumShouldMatch("1");

            boolQueryBuilder.must(new Query.Builder().bool(cityBoolBuilder.build()).build());
        }

        if (attributes.containsKey("workplace") && !attributes.get("workplace").isEmpty()) {
            String workplace = attributes.get("workplace").toLowerCase();

            // Match query with fuzziness
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("workplace")
                    .query(workplace)
                    .fuzziness("AUTO")
                    .build();

            // Wildcard query
            WildcardQuery wildcardQuery = new WildcardQuery.Builder()
                    .field("workplace")
                    .wildcard("*" + workplace + "*")
                    .build();

            // Combine with should
            BoolQuery.Builder workplaceBoolBuilder = new BoolQuery.Builder()
                    .should(new Query.Builder().match(matchQuery).build())
                    .should(new Query.Builder().wildcard(wildcardQuery).build())
                    .minimumShouldMatch("1");

            boolQueryBuilder.must(new Query.Builder().bool(workplaceBoolBuilder.build()).build());
        }

        if (attributes.containsKey("gender") && !attributes.get("gender").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("gender")
                    .query(attributes.get("gender"))
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Traitement des autres attributs non spécifiquement optimisés
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            // Ne traiter que les attributs qui n'ont pas déjà été traités ci-dessus
            if (!key.equals("firstName") && !key.equals("lastName") && !key.equals("email")
                    && !key.equals("currentCity") && !key.equals("workplace") && !key.equals("gender")
                    && value != null && !value.isEmpty()) {

                MatchQuery matchQuery = new MatchQuery.Builder()
                        .field(key)
                        .query(value)
                        .build();

                boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
            }
        }

        // Création de la requête finale
        Query query = new Query.Builder().bool(boolQueryBuilder.build()).build();
        System.out.println("Query: " + query.toString());

        // Création de la requête avec pagination (limité à 100 résultats par défaut)
        NativeQuery searchQuery = NativeQuery.builder()
                .withQuery(q -> q.bool(boolQueryBuilder.build()))
                .withPageable(PageRequest.of(0, 100))
                .withSort(Sort.by(Sort.Direction.DESC, "_score"))
                .build();

        SearchHits<B2C> searchHits = elasticsearchOperations.search(searchQuery, B2C.class);

        // Afficher le nombre de résultats pour le débogage
        System.out.println("Nombre de résultats: " + searchHits.getTotalHits());

        return searchHits.stream()
                .map(SearchHit::getContent)
                .collect(Collectors.toList());
    }

    public List<B2C> searchByAttributes01(Map<String, String> attributes) {
        BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

        // Traitement spécial pour les champs courants
        if (attributes.containsKey("firstName") && !attributes.get("firstName").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("firstName")
                    .query(attributes.get("firstName"))
                    .fuzziness("AUTO")
                    .prefixLength(2)
                    .maxExpansions(10)
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("lastName") && !attributes.get("lastName").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("lastName")
                    .query(attributes.get("lastName"))
                    .fuzziness("AUTO")
                    .prefixLength(2)
                    .maxExpansions(10)
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("email") && !attributes.get("email").isEmpty()) {
            String email = attributes.get("email").toLowerCase();

            // Term query for exact match
            TermQuery termQuery = new TermQuery.Builder()
                    .field("email.keyword")
                    .value(email)
                    .build();

            // Prefix query for partial match
            PrefixQuery prefixQuery = new PrefixQuery.Builder()
                    .field("email")
                    .value(email)
                    .build();

            // Combine with should
            BoolQuery.Builder emailBoolBuilder = new BoolQuery.Builder()
                    .should(new Query.Builder().term(termQuery).build())
                    .should(new Query.Builder().prefix(prefixQuery).build())
                    .minimumShouldMatch("1");

            boolQueryBuilder.must(new Query.Builder().bool(emailBoolBuilder.build()).build());
        }

        if (attributes.containsKey("currentCity") && !attributes.get("currentCity").isEmpty()) {
            String city = attributes.get("currentCity").toLowerCase();

            // Match query with fuzziness
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("currentCity")
                    .query(city)
                    .fuzziness("AUTO")
                    .build();

            // Wildcard query
            WildcardQuery wildcardQuery = new WildcardQuery.Builder()
                    .field("currentCity")
                    .wildcard("*" + city + "*")
                    .build();

            // Combine with should
            BoolQuery.Builder cityBoolBuilder = new BoolQuery.Builder()
                    .should(new Query.Builder().match(matchQuery).build())
                    .should(new Query.Builder().wildcard(wildcardQuery).build())
                    .minimumShouldMatch("1");

            boolQueryBuilder.must(new Query.Builder().bool(cityBoolBuilder.build()).build());
        }

        if (attributes.containsKey("workplace") && !attributes.get("workplace").isEmpty()) {
            String workplace = attributes.get("workplace").toLowerCase();

            // Match query with fuzziness
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("workplace")
                    .query(workplace)
                    .fuzziness("AUTO")
                    .build();

            // Wildcard query
            WildcardQuery wildcardQuery = new WildcardQuery.Builder()
                    .field("workplace")
                    .wildcard("*" + workplace + "*")
                    .build();

            // Combine with should
            BoolQuery.Builder workplaceBoolBuilder = new BoolQuery.Builder()
                    .should(new Query.Builder().match(matchQuery).build())
                    .should(new Query.Builder().wildcard(wildcardQuery).build())
                    .minimumShouldMatch("1");

            boolQueryBuilder.must(new Query.Builder().bool(workplaceBoolBuilder.build()).build());
        }

        if (attributes.containsKey("gender") && !attributes.get("gender").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("gender")
                    .query(attributes.get("gender"))
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Traitement des autres attributs
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (!key.equals("firstName") && !key.equals("lastName") && !key.equals("email")
                    && !key.equals("currentCity") && !key.equals("workplace") && !key.equals("gender")
                    && value != null && !value.isEmpty()) {

                MatchQuery matchQuery = new MatchQuery.Builder()
                        .field(key)
                        .query(value)
                        .build();

                boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
            }
        }

        // Création de la requête finale avec un lambda pour éviter d'utiliser directement la classe Query.Builder
        BoolQuery builtBoolQuery = boolQueryBuilder.build();

        // Configuration pour l'approche par pagination pour récupérer tous les résultats
        final int pageSize = 1000;
        int pageNumber = 0;
        boolean hasMoreData = true;
        List<B2C> allB2CS = new ArrayList<>();

        while (hasMoreData) {
            // Création de la requête avec pagination
            NativeQuery searchQuery = NativeQuery.builder()
                    .withQuery(q -> q.bool(builtBoolQuery))
                    .withPageable(PageRequest.of(pageNumber, pageSize))
                    .withSort(Sort.by(Sort.Direction.DESC, "_score"))
                    .build();

            // Exécuter la recherche pour la page actuelle
            SearchHits<B2C> searchHits = elasticsearchOperations.search(searchQuery, B2C.class);

            // Récupérer les résultats de la page actuelle
            List<B2C> usersOnPage = searchHits.getSearchHits().stream()
                    .map(SearchHit::getContent)
                    .collect(Collectors.toList());

            // Ajouter les résultats à notre liste complète
            allB2CS.addAll(usersOnPage);

            // Log pour débogage
            System.out.println("Page " + pageNumber + ": " + usersOnPage.size() + " résultats");

            // Vérifier s'il y a plus de données à récupérer
            // Si nous avons récupéré moins de résultats que la taille de la page, nous avons atteint la fin
            if (usersOnPage.size() < pageSize) {
                hasMoreData = false;
            } else {
                // Passer à la page suivante
                pageNumber++;
            }
        }

        // Afficher le nombre total de résultats pour le débogage
        System.out.println("Nombre total de résultats: " + allB2CS.size());

        return allB2CS;
    }

    public Page<B2C> searchByAttributes02(Map<String, String> attributes, Pageable pageable) {
        BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

        // Process common fields first
        processCommonField(boolQueryBuilder, attributes, "firstName", true);
        processCommonField(boolQueryBuilder, attributes, "lastName", true);
        processEmailField(boolQueryBuilder, attributes);
        processTextField(boolQueryBuilder, attributes, "currentCity");
        processTextField(boolQueryBuilder, attributes, "workplace");
        processCommonField(boolQueryBuilder, attributes, "gender", false);

        // Process other attributes
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            // Skip already processed fields and empty values
            if (isCommonField(key) || value == null || value.isEmpty()) {
                continue;
            }

            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field(key)
                    .query(value)
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Create final bool query
        BoolQuery builtBoolQuery = boolQueryBuilder.build();

        try {
            // Créer une requête paginée
            NativeQuery searchQuery = NativeQuery.builder()
                    .withQuery(q -> q.bool(builtBoolQuery))
                    .withPageable(pageable)
                    .withSort(Sort.by(Sort.Direction.DESC, "_score"))
                    .build();

            // Exécuter la recherche avec pagination
            SearchHits<B2C> searchHits = elasticsearchOperations.search(searchQuery, B2C.class);

            // Convertir les résultats en Page<B2C>
            List<B2C> b2CS = searchHits.getSearchHits().stream()
                    .map(SearchHit::getContent)
                    .collect(Collectors.toList());

            // Créer un objet Page avec les résultats, la pagination et le nombre total
            return new PageImpl<>(b2CS, pageable, searchHits.getTotalHits());

        } catch (Exception e) {
            // Log the error with detailed information
            System.err.println("Error executing Elasticsearch query: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    // Helper methods restent les mêmes
    private boolean isCommonField(String field) {
        return field.equals("firstName") || field.equals("lastName") || field.equals("email") ||
                field.equals("currentCity") || field.equals("workplace") || field.equals("gender");
    }

    private void processCommonField(BoolQuery.Builder builder, Map<String, String> attributes,
                                    String fieldName, boolean useFuzziness) {
        if (attributes.containsKey(fieldName) && !attributes.get(fieldName).isEmpty()) {
            MatchQuery.Builder matchQueryBuilder = new MatchQuery.Builder()
                    .field(fieldName)
                    .query(attributes.get(fieldName));

            if (useFuzziness) {
                matchQueryBuilder.fuzziness("AUTO")
                        .prefixLength(2)
                        .maxExpansions(10);
            }

            builder.must(new Query.Builder().match(matchQueryBuilder.build()).build());
        }
    }
    private void processEmailField(BoolQuery.Builder builder, Map<String, String> attributes) {
        if (attributes.containsKey("email") && !attributes.get("email").isEmpty()) {
            String email = attributes.get("email");

            // Use match query instead of term for less strict matching
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("email")
                    .query(email)
                    .build();

            builder.must(new Query.Builder().match(matchQuery).build());
        }
    }

    private void processTextField(BoolQuery.Builder builder, Map<String, String> attributes, String fieldName) {
        if (attributes.containsKey(fieldName) && !attributes.get(fieldName).isEmpty()) {
            String value = attributes.get(fieldName);

            // Use match query with fuzziness for better results
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field(fieldName)
                    .query(value)
                    .fuzziness("AUTO")
                    .build();

            builder.must(new Query.Builder().match(matchQuery).build());
        }
    }

    public Page<B2C> searchByAttributes03(Map<String, String> attributes, Pageable pageable) {
        BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

        // Traitement spécial pour les champs courants
        if (attributes.containsKey("firstName") && !attributes.get("firstName").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("firstName")
                    .query(attributes.get("firstName"))
                    .fuzziness("AUTO")
                    .prefixLength(2)
                    .maxExpansions(10)
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("lastName") && !attributes.get("lastName").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("lastName")
                    .query(attributes.get("lastName"))
                    .fuzziness("AUTO")
                    .prefixLength(2)
                    .maxExpansions(10)
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("email") && !attributes.get("email").isEmpty()) {
            String email = attributes.get("email").toLowerCase();

            // Term query for exact match
            TermQuery termQuery = new TermQuery.Builder()
                    .field("email.keyword")
                    .value(email)
                    .build();

            // Prefix query for partial match
            PrefixQuery prefixQuery = new PrefixQuery.Builder()
                    .field("email")
                    .value(email)
                    .build();

            // Combine with should
            BoolQuery.Builder emailBoolBuilder = new BoolQuery.Builder()
                    .should(new Query.Builder().term(termQuery).build())
                    .should(new Query.Builder().prefix(prefixQuery).build())
                    .minimumShouldMatch("1");

            boolQueryBuilder.must(new Query.Builder().bool(emailBoolBuilder.build()).build());
        }

        if (attributes.containsKey("currentCity") && !attributes.get("currentCity").isEmpty()) {
            String city = attributes.get("currentCity").toLowerCase();

            // Match query with fuzziness
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("currentCity")
                    .query(city)
                    .fuzziness("AUTO")
                    .build();

            // Wildcard query
            WildcardQuery wildcardQuery = new WildcardQuery.Builder()
                    .field("currentCity")
                    .wildcard("*" + city + "*")
                    .build();

            // Combine with should
            BoolQuery.Builder cityBoolBuilder = new BoolQuery.Builder()
                    .should(new Query.Builder().match(matchQuery).build())
                    .should(new Query.Builder().wildcard(wildcardQuery).build())
                    .minimumShouldMatch("1");

            boolQueryBuilder.must(new Query.Builder().bool(cityBoolBuilder.build()).build());
        }

        if (attributes.containsKey("workplace") && !attributes.get("workplace").isEmpty()) {
            String workplace = attributes.get("workplace").toLowerCase();

            // Match query with fuzziness
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("workplace")
                    .query(workplace)
                    .fuzziness("AUTO")
                    .build();

            // Wildcard query
            WildcardQuery wildcardQuery = new WildcardQuery.Builder()
                    .field("workplace")
                    .wildcard("*" + workplace + "*")
                    .build();

            // Combine with should
            BoolQuery.Builder workplaceBoolBuilder = new BoolQuery.Builder()
                    .should(new Query.Builder().match(matchQuery).build())
                    .should(new Query.Builder().wildcard(wildcardQuery).build())
                    .minimumShouldMatch("1");

            boolQueryBuilder.must(new Query.Builder().bool(workplaceBoolBuilder.build()).build());
        }

        if (attributes.containsKey("gender") && !attributes.get("gender").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("gender")
                    .query(attributes.get("gender"))
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Traitement des autres attributs
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (!key.equals("firstName") && !key.equals("lastName") && !key.equals("email")
                    && !key.equals("currentCity") && !key.equals("workplace") && !key.equals("gender")
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
                    .withPageable(pageable)  // Utilisation du Pageable fourni par le client
                    .withSort(Sort.by(Sort.Direction.DESC, "_score"))
                    .build();

            // Exécuter la recherche
            SearchHits<B2C> searchHits = elasticsearchOperations.search(searchQuery, B2C.class);

            // Récupérer les résultats
            List<B2C> usersOnPage = searchHits.getSearchHits().stream()
                    .map(SearchHit::getContent)
                    .collect(Collectors.toList());

            // Log pour débogage
            System.out.println("Page " + pageable.getPageNumber() + ": " + usersOnPage.size() + " résultats");
            System.out.println("Total résultats trouvés: " + searchHits.getTotalHits());

            // Retourner un objet Page avec les résultats, la pagination et le nombre total
            return new PageImpl<>(usersOnPage, pageable, searchHits.getTotalHits());
        } catch (Exception e) {
            // Log détaillé de l'erreur
            System.err.println("Erreur lors de l'exécution de la requête Elasticsearch: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    // Recherche par attributs avec pagination
    public Map<String, Object> searchByAttributes04(Map<String, String> attributes, Pageable pageable) {
        BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

        // Traitement spécial pour les champs courants
        if (attributes.containsKey("firstName") && !attributes.get("firstName").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("firstName")
                    .query(attributes.get("firstName"))
                    .fuzziness("2")
                    .prefixLength(2)
                    .maxExpansions(10)
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("lastName") && !attributes.get("lastName").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("lastName")
                    .query(attributes.get("lastName"))
                    .fuzziness("2")
                    .prefixLength(2)
                    .maxExpansions(10)
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("email") && !attributes.get("email").isEmpty()) {
            String email = attributes.get("email").toLowerCase();

            // Term query for exact match
            TermQuery termQuery = new TermQuery.Builder()
                    .field("email.keyword")
                    .value(email)
                    .build();

            // Prefix query for partial match
            PrefixQuery prefixQuery = new PrefixQuery.Builder()
                    .field("email")
                    .value(email)
                    .build();

            // Combine with should
            BoolQuery.Builder emailBoolBuilder = new BoolQuery.Builder()
                    .should(new Query.Builder().term(termQuery).build())
                    .should(new Query.Builder().prefix(prefixQuery).build())
                    .minimumShouldMatch("1");

            boolQueryBuilder.must(new Query.Builder().bool(emailBoolBuilder.build()).build());
        }

        if (attributes.containsKey("currentCity") && !attributes.get("currentCity").isEmpty()) {
            String city = attributes.get("currentCity").toLowerCase();

            // Match query with fuzziness
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("currentCity")
                    .query(city)
                    .fuzziness("2")
                    .build();

            // Wildcard query
            WildcardQuery wildcardQuery = new WildcardQuery.Builder()
                    .field("currentCity")
                    .wildcard("*" + city + "*")
                    .build();

            // Combine with should
            BoolQuery.Builder cityBoolBuilder = new BoolQuery.Builder()
                    .should(new Query.Builder().match(matchQuery).build())
                    .should(new Query.Builder().wildcard(wildcardQuery).build())
                    .minimumShouldMatch("1");

            boolQueryBuilder.must(new Query.Builder().bool(cityBoolBuilder.build()).build());
        }

        if (attributes.containsKey("workplace") && !attributes.get("workplace").isEmpty()) {
            String workplace = attributes.get("workplace").toLowerCase();

            // Match query with fuzziness
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("workplace")
                    .query(workplace)
                    .fuzziness("2")
                    .build();

            // Wildcard query
            WildcardQuery wildcardQuery = new WildcardQuery.Builder()
                    .field("workplace")
                    .wildcard("*" + workplace + "*")
                    .build();

            // Combine with should
            BoolQuery.Builder workplaceBoolBuilder = new BoolQuery.Builder()
                    .should(new Query.Builder().match(matchQuery).build())
                    .should(new Query.Builder().wildcard(wildcardQuery).build())
                    .minimumShouldMatch("1");

            boolQueryBuilder.must(new Query.Builder().bool(workplaceBoolBuilder.build()).build());
        }

        if (attributes.containsKey("gender") && !attributes.get("gender").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("gender")
                    .query(attributes.get("gender"))
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Nouveaux attributs ajoutés
        if (attributes.containsKey("phoneNumber") && !attributes.get("phoneNumber").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("phoneNumber")
                    .query(attributes.get("phoneNumber"))
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("currentCountry") && !attributes.get("currentCountry").isEmpty()) {
            String country = attributes.get("currentCountry").toLowerCase();

            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("currentCountry")
                    .query(country)
                    .fuzziness("2")
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("hometownCity") && !attributes.get("hometownCity").isEmpty()) {
            String hometownCity = attributes.get("hometownCity").toLowerCase();

            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("hometownCity")
                    .query(hometownCity)
                    .fuzziness("2")
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("hometownCountry") && !attributes.get("hometownCountry").isEmpty()) {
            String hometownCountry = attributes.get("hometownCountry").toLowerCase();

            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("hometownCountry")
                    .query(hometownCountry)
                    .fuzziness("2")
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("relationshipStatus") && !attributes.get("relationshipStatus").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("relationshipStatus")
                    .query(attributes.get("relationshipStatus"))
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }
        if (attributes.containsKey("currentDepartment") && !attributes.get("currentDepartment").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("currentDepartment")
                    .query(attributes.get("currentDepartment"))
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Traitement des autres attributs (pour tous attributs non spécifiquement traités ci-dessus)
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (!key.equals("firstName") && !key.equals("lastName") && !key.equals("email")
                    && !key.equals("currentCity") && !key.equals("workplace") && !key.equals("gender")
                    && !key.equals("phoneNumber") && !key.equals("currentCountry")
                    && !key.equals("hometownCity") && !key.equals("hometownCountry")
                    && !key.equals("relationshipStatus")
                    && !key.equals("currentDepartment")
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
                    .withPageable(pageable)  // Utilisation du Pageable fourni par le client
                    .withSort(Sort.by(Sort.Direction.DESC, "_score"))
                    .withTrackTotalHits(true) // 💥 Cette ligne est ESSENTIELLE pour obtenir le vrai total
                    .build();

            // Exécuter la recherche
            SearchHits<B2C> searchHits = elasticsearchOperations.search(searchQuery, B2C.class);

            // Récupérer le nombre total de résultats
            long totalHits = searchHits.getTotalHits();

            // Récupérer les résultats
            List<B2C> usersOnPage = searchHits.getSearchHits().stream()
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

    public Map<String, Object> searchByAttributes04NoFus(Map<String, String> attributes, Pageable pageable) {
    BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

    // Traitement spécial pour les champs courants
    if (attributes.containsKey("firstName") && !attributes.get("firstName").isEmpty()) {
        MatchQuery matchQuery = new MatchQuery.Builder()
                .field("firstName")
                .query(attributes.get("firstName"))
                .operator(Operator.And) // Tous les termes doivent correspondre
                .build();

        boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
    }

    if (attributes.containsKey("lastName") && !attributes.get("lastName").isEmpty()) {
        MatchQuery matchQuery = new MatchQuery.Builder()
                .field("lastName")
                .query(attributes.get("lastName"))
                .operator(Operator.And) // Tous les termes doivent correspondre
                .build();

        boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
    }

    if (attributes.containsKey("email") && !attributes.get("email").isEmpty()) {
        String email = attributes.get("email").toLowerCase();

        // Prefix query for partial match at start
        PrefixQuery prefixQuery = new PrefixQuery.Builder()
                .field("email")
                .value(email)
                .build();

        boolQueryBuilder.must(new Query.Builder().prefix(prefixQuery).build());
    }

    // Traitement pour les champs exacts
    // Pour currentCity
    if (attributes.containsKey("currentCity") && !attributes.get("currentCity").isEmpty()) {
        String city = attributes.get("currentCity");

        // Version simple mais efficace pour correspondance exacte
        MatchQuery matchQuery = new MatchQuery.Builder()
                .field("currentCity")
                .query(city)
                .operator(Operator.And) // Tous les termes doivent correspondre
                .build();

        boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
    }

    // Pour workplace (un peu plus flexible)
    if (attributes.containsKey("workplace") && !attributes.get("workplace").isEmpty()) {
        String workplace = attributes.get("workplace");

        MatchQuery matchQuery = new MatchQuery.Builder()
                .field("workplace")
                .query(workplace)
                .operator(Operator.And) // Tous les termes doivent correspondre
                .fuzziness("1") // Légère fuzziness pour flexibilité
                .build();

        boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
    }

    // Pour gender (exact)
    if (attributes.containsKey("gender") && !attributes.get("gender").isEmpty()) {
        String gender = attributes.get("gender");

        MatchQuery matchQuery = new MatchQuery.Builder()
                .field("gender")
                .query(gender)
                .operator(Operator.And)
                .build();

        boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
    }

    if (attributes.containsKey("phoneNumber") && !attributes.get("phoneNumber").isEmpty()) {
        MatchQuery matchQuery = new MatchQuery.Builder()
                .field("phoneNumber")
                .query(attributes.get("phoneNumber"))
                .build();

        boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
    }

    if (attributes.containsKey("currentCountry") && !attributes.get("currentCountry").isEmpty()) {
        String country = attributes.get("currentCountry");

        MatchQuery matchQuery = new MatchQuery.Builder()
                .field("currentCountry")
                .query(country)
                .operator(Operator.And) // Tous les termes doivent correspondre
                .build();

        boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
    }

    if (attributes.containsKey("hometownCity") && !attributes.get("hometownCity").isEmpty()) {
        String hometownCity = attributes.get("hometownCity");

        MatchQuery matchQuery = new MatchQuery.Builder()
                .field("hometownCity")
                .query(hometownCity)
                .operator(Operator.And) // Tous les termes doivent correspondre
                .build();

        boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
    }

    if (attributes.containsKey("hometownCountry") && !attributes.get("hometownCountry").isEmpty()) {
        String hometownCountry = attributes.get("hometownCountry");

        MatchQuery matchQuery = new MatchQuery.Builder()
                .field("hometownCountry")
                .query(hometownCountry)
                .operator(Operator.And) // Tous les termes doivent correspondre
                .build();

        boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
    }

    // Pour relationshipStatus (exact)
    if (attributes.containsKey("relationshipStatus") && !attributes.get("relationshipStatus").isEmpty()) {
        String status = attributes.get("relationshipStatus");

        MatchQuery matchQuery = new MatchQuery.Builder()
                .field("relationshipStatus")
                .query(status)
                .operator(Operator.And)
                .build();

        boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
    }

    // Pour currentDepartment (TRAITEMENT SPÉCIAL) - SOLUTION GÉNÉRALE
    // Pour currentDepartment (TRAITEMENT SPÉCIAL)
    if (attributes.containsKey("currentDepartment") && !attributes.get("currentDepartment").isEmpty()) {
        String dept = attributes.get("currentDepartment");

        BoolQuery.Builder deptBoolBuilder = new BoolQuery.Builder();

        if (!dept.contains("-")) {
            // 1. MUST: Correspondance stricte sur currentDepartment
            MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery.Builder()
                    .field("currentDepartment.keyword") // Utilise le champ keyword si défini
                    .query(dept)
                    .build();

            deptBoolBuilder.must(new Query.Builder().matchPhrase(matchPhraseQuery).build());

            // 2. MUST_NOT: Exclure tous les départements avec tiret
            WildcardQuery wildcardQuery = new WildcardQuery.Builder()
                    .field("currentDepartment.keyword") // important d’utiliser keyword ici aussi
                    .wildcard("*-*")
                    .build();

            deptBoolBuilder.mustNot(new Query.Builder().wildcard(wildcardQuery).build());

        } else {
            // Cas où le département contient déjà un tiret : match exact
            MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery.Builder()
                    .field("currentDepartment.keyword") // exact match sur le champ keyword
                    .query(dept)
                    .build();

            deptBoolBuilder.must(new Query.Builder().matchPhrase(matchPhraseQuery).build());
        }

        boolQueryBuilder.must(new Query.Builder().bool(deptBoolBuilder.build()).build());
    }



    // Pour currentRegion (exact)
    if (attributes.containsKey("currentRegion") && !attributes.get("currentRegion").isEmpty()) {
        String region = attributes.get("currentRegion");

        MatchQuery matchQuery = new MatchQuery.Builder()
                .field("currentRegion")
                .query(region)
                .operator(Operator.And) // Tous les termes doivent correspondre
                .build();

        boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
    }

    // Traitement des autres attributs
    for (Map.Entry<String, String> entry : attributes.entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();

        if (!key.equals("firstName") && !key.equals("lastName") && !key.equals("email")
                && !key.equals("currentCity") && !key.equals("workplace") && !key.equals("gender")
                && !key.equals("phoneNumber") && !key.equals("currentCountry")
                && !key.equals("hometownCity") && !key.equals("hometownCountry")
                && !key.equals("relationshipStatus") && !key.equals("currentDepartment")
                && !key.equals("currentRegion")
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
                .withPageable(pageable)  // Utilisation du Pageable fourni par le client
                .withSort(Sort.by(Sort.Direction.DESC, "_score"))
                .withTrackTotalHits(true) // 💥 Cette ligne est ESSENTIELLE pour obtenir le vrai total
                .build();

        // Pour débogage - Affiche la requête générée
        // Décommenter si besoin de voir la requête exacte
        // System.out.println("DEBUG - Requête générée: " + searchQuery.toString());

        // Exécuter la recherche
        SearchHits<B2C> searchHits = elasticsearchOperations.search(searchQuery, B2C.class);

        // Récupérer le nombre total de résultats
        long totalHits = searchHits.getTotalHits();

        // Récupérer les résultats
        List<B2C> usersOnPage = searchHits.getSearchHits().stream()
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

    public long searchByAttributes04NoFusNumb(Map<String, String> attributes, Pageable pageable) {
        BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

        // Traitement spécial pour les champs courants
        if (attributes.containsKey("firstName") && !attributes.get("firstName").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("firstName")
                    .query(attributes.get("firstName"))
                    .operator(Operator.And)
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("lastName") && !attributes.get("lastName").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("lastName")
                    .query(attributes.get("lastName"))
                    .operator(Operator.And)
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("email") && !attributes.get("email").isEmpty()) {
            String email = attributes.get("email").toLowerCase();
            PrefixQuery prefixQuery = new PrefixQuery.Builder()
                    .field("email")
                    .value(email)
                    .build();
            boolQueryBuilder.must(new Query.Builder().prefix(prefixQuery).build());
        }

        // Traitement pour les champs exacts
        if (attributes.containsKey("currentCity") && !attributes.get("currentCity").isEmpty()) {
            String city = attributes.get("currentCity");
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("currentCity")
                    .query(city)
                    .operator(Operator.And)
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("workplace") && !attributes.get("workplace").isEmpty()) {
            String workplace = attributes.get("workplace");
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("workplace")
                    .query(workplace)
                    .operator(Operator.And)
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("gender") && !attributes.get("gender").isEmpty()) {
            String gender = attributes.get("gender");
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("gender")
                    .query(gender)
                    .operator(Operator.And)
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("phoneNumber") && !attributes.get("phoneNumber").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("phoneNumber")
                    .query(attributes.get("phoneNumber"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("currentCountry") && !attributes.get("currentCountry").isEmpty()) {
            String country = attributes.get("currentCountry");
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("currentCountry")
                    .query(country)
                    .operator(Operator.And)
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("hometownCity") && !attributes.get("hometownCity").isEmpty()) {
            String hometownCity = attributes.get("hometownCity");
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("hometownCity")
                    .query(hometownCity)
                    .operator(Operator.And)
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("hometownCountry") && !attributes.get("hometownCountry").isEmpty()) {
            String hometownCountry = attributes.get("hometownCountry");
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("hometownCountry")
                    .query(hometownCountry)
                    .operator(Operator.And)
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("relationshipStatus") && !attributes.get("relationshipStatus").isEmpty()) {
            String status = attributes.get("relationshipStatus");
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("relationshipStatus")
                    .query(status)
                    .operator(Operator.And)
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        if (attributes.containsKey("currentDepartment") && !attributes.get("currentDepartment").isEmpty()) {
            String dept = attributes.get("currentDepartment");
            BoolQuery.Builder deptBoolBuilder = new BoolQuery.Builder();

            if (!dept.contains("-")) {
                MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery.Builder()
                        .field("currentDepartment.keyword")
                        .query(dept)
                        .build();
                deptBoolBuilder.must(new Query.Builder().matchPhrase(matchPhraseQuery).build());

                WildcardQuery wildcardQuery = new WildcardQuery.Builder()
                        .field("currentDepartment.keyword")
                        .wildcard("*-*")
                        .build();
                deptBoolBuilder.mustNot(new Query.Builder().wildcard(wildcardQuery).build());
            } else {
                MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery.Builder()
                        .field("currentDepartment.keyword")
                        .query(dept)
                        .build();
                deptBoolBuilder.must(new Query.Builder().matchPhrase(matchPhraseQuery).build());
            }
            boolQueryBuilder.must(new Query.Builder().bool(deptBoolBuilder.build()).build());
        }

        if (attributes.containsKey("currentRegion") && !attributes.get("currentRegion").isEmpty()) {
            String region = attributes.get("currentRegion");
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("currentRegion")
                    .query(region)
                    .operator(Operator.And)
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Traitement des autres attributs
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (!key.equals("firstName") && !key.equals("lastName") && !key.equals("email")
                    && !key.equals("currentCity") && !key.equals("workplace") && !key.equals("gender")
                    && !key.equals("phoneNumber") && !key.equals("currentCountry")
                    && !key.equals("hometownCity") && !key.equals("hometownCountry")
                    && !key.equals("relationshipStatus") && !key.equals("currentDepartment")
                    && !key.equals("currentRegion")
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
                    .withTrackTotalHits(true) // Essentiel pour obtenir le total
                    .build();

            // Exécuter la recherche
            SearchHits<B2C> searchHits = elasticsearchOperations.search(searchQuery, B2C.class);

            // Récupérer et retourner uniquement le nombre total de résultats
            return searchHits.getTotalHits();
        } catch (Exception e) {
            System.err.println("Erreur lors de l'exécution de la requête Elasticsearch: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    public Map<String, Object> searchWithMultipleFilters(String gender, List<String> departments, List<String> regions, List<String> cities, Map<String, String> additionalAttributes, Pageable pageable) {

        BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

        // Traitement pour le genre (si fourni)
        if (gender != null && !gender.isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("gender")
                    .query(gender)
                    .operator(Operator.And)
                    .build();

            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Traitement pour la liste de départements (si fournie)
        if (departments != null && !departments.isEmpty()) {
            BoolQuery.Builder departmentsBoolBuilder = new BoolQuery.Builder();

            for (String department : departments) {
                BoolQuery.Builder singleDeptBoolBuilder = new BoolQuery.Builder();

                if (!department.contains("-")) {
                    // 1. Match exact sur le département
                    MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery.Builder()
                            .field("currentDepartment.keyword")
                            .query(department)
                            .build();

                    singleDeptBoolBuilder.must(new Query.Builder().matchPhrase(matchPhraseQuery).build());

                    // 2. Exclure tous les départements avec tiret
                    WildcardQuery wildcardQuery = new WildcardQuery.Builder()
                            .field("currentDepartment.keyword")
                            .wildcard("*-*")
                            .build();

                    singleDeptBoolBuilder.mustNot(new Query.Builder().wildcard(wildcardQuery).build());
                } else {
                    // Cas où le département contient déjà un tiret : match exact
                    MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery.Builder()
                            .field("currentDepartment.keyword")
                            .query(department)
                            .build();

                    singleDeptBoolBuilder.must(new Query.Builder().matchPhrase(matchPhraseQuery).build());
                }

                // Ajouter cette condition à la liste des départements (avec OR)
                departmentsBoolBuilder.should(new Query.Builder().bool(singleDeptBoolBuilder.build()).build());
            }

            // Assurer qu'au moins une des conditions de département est remplie
            departmentsBoolBuilder.minimumShouldMatch("1");

            // Ajouter la condition complète des départements à la requête principale
            boolQueryBuilder.must(new Query.Builder().bool(departmentsBoolBuilder.build()).build());
        }

        // Traitement pour la liste de régions (si fournie)
        if (regions != null && !regions.isEmpty()) {
            BoolQuery.Builder regionsBoolBuilder = new BoolQuery.Builder();

            for (String region : regions) {
                MatchQuery matchQuery = new MatchQuery.Builder()
                        .field("currentRegion")
                        .query(region)
                        .operator(Operator.And)
                        .build();

                // Ajouter cette condition à la liste des régions (avec OR)
                regionsBoolBuilder.should(new Query.Builder().match(matchQuery).build());
            }

            // Assurer qu'au moins une des conditions de région est remplie
            regionsBoolBuilder.minimumShouldMatch("1");

            // Ajouter la condition complète des régions à la requête principale
            boolQueryBuilder.must(new Query.Builder().bool(regionsBoolBuilder.build()).build());
        }
        if (cities != null && !cities.isEmpty()) {
            BoolQuery.Builder regionsBoolBuilder = new BoolQuery.Builder();

            for (String city : cities) {
                MatchQuery matchQuery = new MatchQuery.Builder()
                        .field("currentCity")
                        .query(city)
                        .operator(Operator.And)
                        .build();

                // Ajouter cette condition à la liste des régions (avec OR)
                regionsBoolBuilder.should(new Query.Builder().match(matchQuery).build());
            }

            // Assurer qu'au moins une des conditions de région est remplie
            regionsBoolBuilder.minimumShouldMatch("1");

            // Ajouter la condition complète des régions à la requête principale
            boolQueryBuilder.must(new Query.Builder().bool(regionsBoolBuilder.build()).build());
        }

        // Traitement des attributs supplémentaires (si fournis)
        if (additionalAttributes != null && !additionalAttributes.isEmpty()) {
            // Réutiliser le code existant pour traiter les attributs supplémentaires
            for (Map.Entry<String, String> entry : additionalAttributes.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                if (value != null && !value.isEmpty()) {
                    switch (key) {
                        case "firstName":
                            MatchQuery firstNameQuery = new MatchQuery.Builder()
                                    .field("firstName")
                                    .query(value)
                                    .operator(Operator.And)
                                    .build();
                            boolQueryBuilder.must(new Query.Builder().match(firstNameQuery).build());
                            break;

                        case "lastName":
                            MatchQuery lastNameQuery = new MatchQuery.Builder()
                                    .field("lastName")
                                    .query(value)
                                    .operator(Operator.And)
                                    .build();
                            boolQueryBuilder.must(new Query.Builder().match(lastNameQuery).build());
                            break;

                        case "email":
                            String email = value.toLowerCase();
                            PrefixQuery prefixQuery = new PrefixQuery.Builder()
                                    .field("email")
                                    .value(email)
                                    .build();
                            boolQueryBuilder.must(new Query.Builder().prefix(prefixQuery).build());
                            break;

                        case "currentCity":
                            MatchQuery cityQuery = new MatchQuery.Builder()
                                    .field("currentCity")
                                    .query(value)
                                    .operator(Operator.And)
                                    .build();
                            boolQueryBuilder.must(new Query.Builder().match(cityQuery).build());
                            break;

                        case "workplace":
                            MatchQuery workplaceQuery = new MatchQuery.Builder()
                                    .field("workplace")
                                    .query(value)
                                    .operator(Operator.And)
                                    .fuzziness("1")
                                    .build();
                            boolQueryBuilder.must(new Query.Builder().match(workplaceQuery).build());
                            break;

                        case "phoneNumber":
                            MatchQuery phoneQuery = new MatchQuery.Builder()
                                    .field("phoneNumber")
                                    .query(value)
                                    .build();
                            boolQueryBuilder.must(new Query.Builder().match(phoneQuery).build());
                            break;

                        case "currentCountry":
                            MatchQuery countryQuery = new MatchQuery.Builder()
                                    .field("currentCountry")
                                    .query(value)
                                    .operator(Operator.And)
                                    .build();
                            boolQueryBuilder.must(new Query.Builder().match(countryQuery).build());
                            break;

                        case "hometownCity":
                            MatchQuery hometownCityQuery = new MatchQuery.Builder()
                                    .field("hometownCity")
                                    .query(value)
                                    .operator(Operator.And)
                                    .build();
                            boolQueryBuilder.must(new Query.Builder().match(hometownCityQuery).build());
                            break;

                        case "hometownCountry":
                            MatchQuery hometownCountryQuery = new MatchQuery.Builder()
                                    .field("hometownCountry")
                                    .query(value)
                                    .operator(Operator.And)
                                    .build();
                            boolQueryBuilder.must(new Query.Builder().match(hometownCountryQuery).build());
                            break;

                        case "relationshipStatus":
                            MatchQuery relationshipQuery = new MatchQuery.Builder()
                                    .field("relationshipStatus")
                                    .query(value)
                                    .operator(Operator.And)
                                    .build();
                            boolQueryBuilder.must(new Query.Builder().match(relationshipQuery).build());
                            break;

                        // Ne pas traiter gender, currentDepartment et currentRegion ici car ils sont gérés séparément
                        case "gender":
                        case "currentDepartment":
                        case "currentRegion":
                            break;

                        default:
                            // Pour tous les autres champs
                            MatchQuery defaultQuery = new MatchQuery.Builder()
                                    .field(key)
                                    .query(value)
                                    .build();
                            boolQueryBuilder.must(new Query.Builder().match(defaultQuery).build());
                            break;
                    }
                }
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

            // Exécuter la recherche
            SearchHits<B2C> searchHits = elasticsearchOperations.search(searchQuery, B2C.class);

            // Récupérer le nombre total de résultats
            long totalHits = searchHits.getTotalHits();

            // Récupérer les résultats
            List<B2C> usersOnPage = searchHits.getSearchHits().stream()
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

    // Méthode pour convertir une ligne CSV en objet B2C
    @Async
    public CompletableFuture<ImportResult> importCsv(File file) {
        // Définition du logger
        Logger logger = LoggerFactory.getLogger(getClass());

        ImportResult result = new ImportResult();
        result.setStartTime(System.currentTimeMillis());

        // Création de pools d'exécution séparés pour les différentes tâches
        ExecutorService conversionExecutor = Executors.newFixedThreadPool(4); // Pour la conversion des lignes
        ExecutorService persistenceExecutor = Executors.newFixedThreadPool(3); // Pour la persistance des données

        try (BufferedReader br = new BufferedReader(new FileReader(file), 16384); // Buffer plus large
             CSVReader reader = new CSVReader(br)) {

            String[] headers = reader.readNext(); // Lire l'en-tête
            AtomicInteger totalProcessed = new AtomicInteger(0);
            AtomicInteger failedRecords = new AtomicInteger(0);

            // Configuration optimale des batches
            int batchSize = 10000; // Taille optimisée pour Elasticsearch

            // Files d'attente pour le pipeline de traitement
            BlockingQueue<String[]> lineQueue = new LinkedBlockingQueue<>(5000); // File pour les lignes brutes
            BlockingQueue<List<B2C>> batchQueue = new LinkedBlockingQueue<>(20); // File pour les lots d'utilisateurs

            // Flag pour indiquer la fin du fichier
            AtomicBoolean endOfFile = new AtomicBoolean(false);

            // Threads pour la conversion des lignes en objets B2C
            List<CompletableFuture<Void>> conversionFutures = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    List<B2C> currentBatch = new ArrayList<>(batchSize);
                    try {
                        while (!Thread.currentThread().isInterrupted()) {
                            String[] line = lineQueue.poll(100, TimeUnit.MILLISECONDS);

                            // Si aucune ligne n'est disponible et que la lecture du fichier est terminée
                            if (line == null) {
                                if (endOfFile.get() && lineQueue.isEmpty()) {
                                    // Envoyer le dernier lot s'il n'est pas vide
                                    if (!currentBatch.isEmpty()) {
                                        batchQueue.put(new ArrayList<>(currentBatch));
                                        currentBatch.clear();
                                    }
                                    break;
                                }
                                continue;
                            }

                            try {
                                // Convertir la ligne en objet B2C
                                B2C b2C = convertLineToUser(line);
                                currentBatch.add(b2C);

                                // Si la taille du lot atteint le seuil, on l'ajoute à la file d'attente
                                if (currentBatch.size() >= batchSize) {
                                    batchQueue.put(new ArrayList<>(currentBatch));
                                    currentBatch.clear();
                                }
                            } catch (Exception e) {
                                failedRecords.incrementAndGet();
                                logger.warn("Erreur lors de la conversion de la ligne: {}", Arrays.toString(line), e);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }, conversionExecutor);

                conversionFutures.add(future);
            }

            // Threads pour la persistance des données
            List<CompletableFuture<Integer>> persistenceFutures = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> {
                    int processed = 0;
                    try {
                        while (!Thread.currentThread().isInterrupted()) {
                            List<B2C> batch = batchQueue.poll(200, TimeUnit.MILLISECONDS);

                            // Si aucun lot n'est disponible et que toutes les conversions sont terminées
                            if (batch == null) {
                                boolean allConversionsDone = conversionFutures.stream()
                                        .allMatch(CompletableFuture::isDone);
                                if (endOfFile.get() && allConversionsDone && batchQueue.isEmpty()) {
                                    break;
                                }
                                continue;
                            }

                            try {
                                // Utiliser ElasticsearchRepository ou BulkRequestBuilder selon votre implémentation
                                long startSave = System.currentTimeMillis();
                                b2CRepository.saveAll(batch);
                                long saveTime = System.currentTimeMillis() - startSave;

                                processed += batch.size();
                                int currentTotal = totalProcessed.addAndGet(batch.size());

                                // Log détaillé avec performances
                                if (currentTotal % 1000 == 0) {
                                    logger.info("Progrès: {} enregistrements traités - dernier lot: {} records en {} ms",
                                            currentTotal, batch.size(), saveTime);
                                }
                            } catch (Exception e) {
                                logger.error("Erreur lors de l'enregistrement du lot: {}", e.getMessage());
                                failedRecords.addAndGet(batch.size());

                                // Stratégie avancée de retry avec subdivision des lots
                                if (batch.size() > 30) {
                                    // Diviser en 3 parties pour une meilleure granularité
                                    int partSize = batch.size() / 3;
                                    batchQueue.put(new ArrayList<>(batch.subList(0, partSize)));
                                    batchQueue.put(new ArrayList<>(batch.subList(partSize, partSize * 2)));
                                    batchQueue.put(new ArrayList<>(batch.subList(partSize * 2, batch.size())));
                                    logger.info("Lot divisé en 3 parties pour retry");
                                } else if (batch.size() > 5) {
                                    // Diviser en deux pour les petits lots
                                    int midPoint = batch.size() / 2;
                                    batchQueue.put(new ArrayList<>(batch.subList(0, midPoint)));
                                    batchQueue.put(new ArrayList<>(batch.subList(midPoint, batch.size())));
                                    logger.info("Lot divisé en 2 parties pour retry");
                                } else {
                                    // Traiter individuellement pour les très petits lots
                                    for (B2C user : batch) {
                                        try {
                                            b2CRepository.save(user);
                                            totalProcessed.incrementAndGet();
                                            processed++;
                                        } catch (Exception ex) {
                                            failedRecords.incrementAndGet();
                                            logger.error("Échec persistance individuelle: {}", user.getUserId(), ex);
                                        }
                                    }
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return processed;
                }, persistenceExecutor);

                persistenceFutures.add(future);
            }

            // Thread de lecture du fichier CSV (producteur)
            String[] line;
            while ((line = reader.readNext()) != null) {
                // Vérifier si la ligne contient suffisamment de colonnes
                if (line.length >= 12) {
                    lineQueue.put(line);
                } else {
                    failedRecords.incrementAndGet();
                    logger.warn("Ligne ignorée, nombre de colonnes insuffisant: {}", Arrays.toString(line));
                }
            }

            // Signaler la fin du fichier
            endOfFile.set(true);

            // Attendre que toutes les conversions soient terminées
            CompletableFuture.allOf(conversionFutures.toArray(new CompletableFuture[0])).join();

            // Attendre que toutes les persistances soient terminées
            CompletableFuture<Void> allPersistencesDone = CompletableFuture.allOf(
                    persistenceFutures.toArray(new CompletableFuture[0]));
            allPersistencesDone.join();

            // Récupérer le nombre total d'enregistrements traités avec succès
            int totalSuccess = persistenceFutures.stream()
                    .mapToInt(future -> {
                        try {
                            return future.get();
                        } catch (Exception e) {
                            logger.error("Erreur lors de la récupération des résultats", e);
                            return 0;
                        }
                    })
                    .sum();

            result.setSuccessCount(totalSuccess);
            result.setFailedCount(failedRecords.get());
            result.setEndTime(System.currentTimeMillis());

            logger.info("Import terminé: {} succès, {} échecs, durée: {} ms (moyenne: {} records/sec)",
                    result.getSuccessCount(),
                    result.getFailedCount(),
                    (result.getEndTime() - result.getStartTime()),
                    (int)(result.getSuccessCount() * 1000.0 / (result.getEndTime() - result.getStartTime())));

        } catch (Exception e) {
            logger.error("Erreur critique pendant l'importation du fichier", e);
            result.setError(e.getMessage());
        } finally {
            // Arrêt propre des pools d'exécution
            shutdownExecutor(conversionExecutor);
            shutdownExecutor(persistenceExecutor);
        }

        return CompletableFuture.completedFuture(result);
    }

    // Méthode pour arrêter les exécuteurs
    private void shutdownExecutor(ExecutorService executor) {
        try {
            executor.shutdown();
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // Version modifiée utilisant des setters
    private B2C convertLineToUser(String[] line) {
        B2C b2C = new B2C();
        try {
            String uuid = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 10);
            // Convertir en nombre si nécessaire
            long id = Long.parseLong(uuid, 16);
            // Utilisation de setters au lieu du constructeur
            b2C.setIdS(id);
            b2C.setUserId( Long.parseLong(line[1].trim()));
            b2C.setPhoneNumber(StringUtils.trimToNull(line[0]));
            b2C.setFirstName(StringUtils.trimToNull(line[2]));
            b2C.setLastName(StringUtils.trimToNull(line[3]));
            b2C.setGender(StringUtils.trimToNull(line[4]));
            b2C.setCurrentCity(StringUtils.trimToNull(line[5]));
            b2C.setCurrentCountry(StringUtils.trimToNull(line[6]));
            b2C.setHometownCity(StringUtils.trimToNull(line[7]));
            b2C.setHometownCountry(StringUtils.trimToNull(line[8]));
            b2C.setRelationshipStatus(StringUtils.trimToNull(line[9]));
            b2C.setWorkplace(StringUtils.trimToNull(line[10]));
            b2C.setEmail(StringUtils.trimToNull(line[11]));
            b2C.setCurrentDepartment(StringUtils.trimToNull(line[12]));
            b2C.setCurrentRegion(StringUtils.trimToNull(line[13]));

            return b2C;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Erreur de conversion pour l'ID utilisateur: " + line[1], e);
        }
    }

    // Classe pour contenir les résultats de l'importation
    public class ImportResult {
        private long startTime;
        private long endTime;
        private int successCount;
        private int failedCount;
        private String error;

        // Getters et setters
        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public long getEndTime() {
            return endTime;
        }

        public void setEndTime(long endTime) {
            this.endTime = endTime;
        }

        public int getSuccessCount() {
            return successCount;
        }

        public void setSuccessCount(int successCount) {
            this.successCount = successCount;
        }

        public int getFailedCount() {
            return failedCount;
        }

        public void setFailedCount(int failedCount) {
            this.failedCount = failedCount;
        }

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }
    }

    // Méthode pour récupérer des données protégées
    public String getProtectedData(Long idS, String type) {
        B2C b2C = b2CRepository.findById(idS)
                .orElseThrow(() -> new RuntimeException("B2C not found"));

        return switch (type) {
            case "email" -> b2C.getEmail() != null ? b2C.getEmail() : "inconnu";
            case "phone" -> b2C.getPhoneNumber() != null ? b2C.getPhoneNumber() : "inconnu";
            case "relationship" -> b2C.getRelationshipStatus() != null ? b2C.getRelationshipStatus() : "inconnu";
            default -> throw new IllegalArgumentException("Invalid data type: " + type);
        };
    }

}