package edu.jhu.bdpuh;

import java.time.Instant;

public class MovieRating {
    public String movieId, userId;
    public String rating;
    public Instant timestamp = null;

    public boolean loadData(String data){
        this.timestamp = null;
        String[] parts = data.trim().split("\\t");

        // should be exactly 4 parts
        if(parts.length == 4) {
            this.userId = parts[0];
            this.movieId = parts[1];
            this.rating = parts[2];
            this.timestamp = Instant.ofEpochSecond(Long.parseLong(parts[3]));
        }
        return isValid();
    }

    public boolean isValid() { return timestamp != null; }
}
