package edu.jhu.bdpuh;

import org.junit.Test;

import static org.junit.Assert.*;

public class MovieItemTest {
    static final String testData = "1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0";

    @Test
    public void testLifeCycle() {
        MovieItem movieItem = new MovieItem();
        assertFalse(movieItem.isValid());
        movieItem.loadData(testData);
        System.out.println(movieItem.genres);
        assertTrue(movieItem.isValid());
        assertEquals("Toy Story (1995)", movieItem.title);
        assertFalse(movieItem.genres.contains("SciFi"));
        assertTrue(movieItem.genres.contains("Animation"));
    }

}