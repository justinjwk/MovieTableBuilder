package edu.jhu.bdpuh;

import org.junit.Test;

import static org.junit.Assert.*;

public class MovieRatingTest {

    @Test
    public void testLifecycle() {
        MovieRating movieRating = new MovieRating();
        assertFalse(movieRating.isValid());
        movieRating.loadData("1\t2\t3\t1234\n");
        assertTrue(movieRating.isValid());
        assertEquals("1",movieRating.userId);
        assertEquals("2", movieRating.movieId);
        assertEquals("3",movieRating.rating);
        assertEquals(1234, movieRating.timestamp.getEpochSecond());
        movieRating.loadData("1|2|3|1234\n");
        assertFalse(movieRating.isValid());
    }

}