package edu.jhu.bdpuh;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MovieItem {
    static final String[] GENRE = {"Unknown","Action","Adventure","Animation","Childrens", "Comedy","Crime","Documentary","Drama",
                                   "Fantasy","FilmNoir","Horror","Musical","Mystery","Romance","SciFi","Thriller","War","Western"};
    static final int MOVIE_ID_OFFSET = 0;
    static final int TITLE_OFFSET = 1;
    static final int RELEASED_OFFSET = 2;
    static final int VIDEO_RELEASED_OFFSET = 3;
    static final int URL_OFFSET = 4;
    static final int GENRE_OFFSET = 5;
    public String movieId = null;
    public String title, released, video_released, url;
    public Set<String> genres = new HashSet<>();


    public boolean loadData(String data) {
        this.movieId = null;
        this.genres.clear();
        String[] parts = data.trim().split("\\|");
        if(parts.length == 24) {
            this.title = parts[TITLE_OFFSET];
            this.released = parts[RELEASED_OFFSET];
            this.video_released = parts[VIDEO_RELEASED_OFFSET];
            this.url = parts[URL_OFFSET];
            for(int i = 0; i < GENRE.length; i++) {
                if (parts[GENRE_OFFSET + i].equals("1"))
                    this.genres.add(GENRE[i]);
            }
            this.movieId = parts[MOVIE_ID_OFFSET];
        }
        return isValid();
    }

    public boolean isValid() { return this.movieId != null; }



//    movie id | movie title | release date | video release date |
//    IMDb URL | unknown | Action | Adventure | Animation |
//    Children's | Comedy | Crime | Documentary | Drama | Fantasy |
//    Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi |
//    Thriller | War | Western |
}
