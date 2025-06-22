package com.example.springelasticproject.controller.b2bController.ShadowpilotController;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/deduplication")
public class DeduplicationController {

    private final ShadowPilotServiceDupliquer shadowPilotService;


    @Autowired
    public DeduplicationController(ShadowPilotServiceDupliquer shadowPilotService) {
        this.shadowPilotService = shadowPilotService;
    }

    /**
     * Supprimer les entreprises en double.
     */
    @DeleteMapping("/remove-duplicates")
    public ResponseEntity<String> removeDuplicateCompanies() {
        try {
            shadowPilotService.removeDuplicateCompanies();
            return ResponseEntity.ok("✔ Opération de suppression des doublons terminée.");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("❌ Erreur lors de la suppression des doublons : " + e.getMessage());
        }
    }


}