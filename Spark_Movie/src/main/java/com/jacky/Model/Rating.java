package com.jacky.Model;

import java.io.Serializable;

public class Rating implements Serializable {
    private String userId;
    private String movieId;
    private String rating;
    private String times;

    public Rating(String userId, String movieId, String rating, String times)
    {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
        this.times = times;
    }

    public String getUserId()
    {
        return this.userId;
    }

    public String getMovieId()
    {
        return this.movieId;
    }

    public String getRating()
    {
        return this.rating;
    }

    public String getTimes()
    {
        return this.times;
    }

}
