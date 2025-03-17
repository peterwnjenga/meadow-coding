Sample test data
```json
{
  "name": "meadow_api/movie.watched",
  "data": {
    "movie_title": "Beetlejuice",
    "recipient_email": "pwn2107@columbia.edu"
  }
}
```

Non exact name
```json
{
  "name": "meadow_api/movie.watched",
  "data": {
    "movie_title": "Shawshank",
    "recipient_email": "pwn2107@columbia.edu"
  }
}
```


Invalid/undeliverable email
```json
{
  "name": "meadow_api/movie.watched",
  "data": {
    "movie_title": "The Godfather",
    "recipient_email": "pwn21xx07@columbia.edxxu"
  }
}
```