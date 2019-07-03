package com.jacky.Model;

import java.io.Serializable;

public class Movie implements Serializable {

    private String movieId;
    private String title;
    private String genres;

    public Movie(String movieId, String title, String genres)
    {
        this.movieId = movieId;
        this.title = title;
        this.genres = genres;
    }


    public String getMovieId() { return this.movieId;}
    public void setMovieId(String movieId) { this.movieId = movieId;}

    public String getTitle() { return this.title;}
    public void setTitle(String title){ this.title = title;}

    public String getGenres() { return this.genres;}
    public void setGenres(String genres) { this.genres = genres;}


}
