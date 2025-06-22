package com.example.springelasticproject.repository;

import com.example.springelasticproject.model.b2cModel.B2C;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface B2CRepository extends ElasticsearchRepository<B2C,Long> {
    // Recherche par prénom
    List<B2C> findByFirstNameContaining(String firstName);

    // Recherche par nom
    List<B2C> findByLastNameContaining(String lastName);

    // Recherche par prénom ou nom
    List<B2C> findByFirstNameContainingOrLastNameContaining(String firstName, String lastName);
}
