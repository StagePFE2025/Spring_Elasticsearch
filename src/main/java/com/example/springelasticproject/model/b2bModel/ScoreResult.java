package com.example.springelasticproject.model.b2bModel;

public class ScoreResult {
    private double score;
    private String category;

    public ScoreResult(double score, String category) {
        this.score = score;
        this.category = category;
    }

    public double getScore() {
        return score;
    }

    public String getCategory() {
        return category;
    }
}
