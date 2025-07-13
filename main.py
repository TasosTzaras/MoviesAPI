import requests
import pandas as pd
from sqlalchemy import create_engine
import time
import json
import sqlite3

# --- Settings ---
# TMDB API Key
API_KEY = 'PUT YOUR API KEY HERE'
BASE_URL = 'https://api.themoviedb.org/3'

def fetch_top_movies():
    """
    It fetches the 50 best rated movies from TMDB (3 pages)
    """
    top_movies = []
    # API returns around 20 movies per page, so we need 3 pages for 50+
    for page in range(1, 4):
        endpoint = f"{BASE_URL}/movie/top_rated"
        params = {'api_key': API_KEY, 'language': 'en-US', 'page': page}
        response = requests.get(endpoint, params=params)

        if response.status_code == 200:
            top_movies.extend(response.json().get('results', []))
        else:
            print(f"Error fetching top movies on page {page}: {response.status_code}")
            return []

    return top_movies[:50]

def fetch_movie_details(movie_id):
    """
    Fetches credit details about a movie
    """
    # Endpoints for details
    details_endpoint = f"{BASE_URL}/movie/{movie_id}"
    params = {'api_key': API_KEY, 'language': 'en-US'}
    details_response = requests.get(details_endpoint, params=params)

    credits_endpoint = f"{BASE_URL}/movie/{movie_id}/credits"
    credits_response = requests.get(credits_endpoint, params=params)

    if details_response.status_code == 200 and credits_response.status_code == 200:
        details = details_response.json()
        credits = credits_response.json()
        return details, credits
    else:
        print(f"Error fetching details for movie ID {movie_id}")
        return None, None

# --- Main Script ---
if __name__ == "__main__":
    print("Phase 1: Extract - Extracting data for movies from TMDB...")

    top_movies_list = fetch_top_movies()
    all_movies_data = []

    if top_movies_list:
        for i, movie_summary in enumerate(top_movies_list):
            movie_id = movie_summary.get('id')
            if movie_id:
                print(f"Fetching details for movie {i+1}/50: {movie_summary.get('title')}")
                details, credits = fetch_movie_details(movie_id)
                if details and credits:
                    # Putting the data in a dictionary
                    combined_data = {
                        'tmdb_id': details.get('id'),
                        'title': details.get('title'),
                        'overview': details.get('overview'),
                        'release_date': details.get('release_date'),
                        'vote_average': details.get('vote_average'),
                        'genres': details.get('genres', []),
                        'cast': credits.get('cast', []),
                        'crew': credits.get('crew', [])
                    }
                    all_movies_data.append(combined_data)

    # Putting the raw data in DataFrame
    raw_data_df = pd.DataFrame(all_movies_data)

    print("\nExtraction Complete!")
    print(f"Data were collected for {len(raw_data_df)} movies.")
    print("Preview of raw data:")
    print(raw_data_df.head())
    print("\nColumns being collected:", raw_data_df.columns)

    # --- Part 2: Transform ---

print("\nPhase 2: Transform - Data Transform...")

# 1. Edit Genres (Genres)
all_genres = {} # Dictionary for keeping the unique genres
movie_genres_list = [] # List for the connection table

for index, row in raw_data_df.iterrows():
    movie_id = row['tmdb_id']
    for genre in row['genres']:
        genre_id = genre['id']
        # Add the term to the dictionary (if it does not already exist)
        if genre_id not in all_genres:
            all_genres[genre_id] = genre['name']
        # Add the film-genre relationship to the list
        movie_genres_list.append({'movie_id': movie_id, 'genre_id': genre_id})

# Creation of DataFrames for Genres
genres_df = pd.DataFrame(list(all_genres.items()), columns=['id', 'name'])
movie_genres_df = pd.DataFrame(movie_genres_list)


# 2. Processing Coefficients (People: Actors & Directors)
all_people = {} # Dictionary for unique people
movie_cast_list = [] # List for the cast table
directors_map = {} # Dictionary to match film -> director

for index, row in raw_data_df.iterrows():
    movie_id = row['tmdb_id']
    # Finding the director
    for member in row['crew']:
        if member['job'] == 'Director':
            director_id = member['id']
            directors_map[movie_id] = director_id
            if director_id not in all_people:
                all_people[director_id] = member['name']
            break # Found the director, continue.

    # Processing of actors (e.g., the first 10)
    for actor in row['cast'][:10]:
        actor_id = actor['id']
        if actor_id not in all_people:
            all_people[actor_id] = actor['name']
        movie_cast_list.append({
            'movie_id': movie_id,
            'person_id': actor_id,
            'character_name': actor['character']
        })

# Creating DataFrames for people
people_df = pd.DataFrame(list(all_people.items()), columns=['id', 'name'])
movie_cast_df = pd.DataFrame(movie_cast_list)


# 3. Creating the final Movies table
movies_df = raw_data_df[['tmdb_id', 'title', 'overview', 'release_date', 'vote_average']].copy()
# Προσθήκη του director_id από το map που φτιάξαμε
movies_df['director_id'] = movies_df['tmdb_id'].map(directors_map)

# Rename the id column to match our schema
movies_df.rename(columns={'tmdb_id': 'id'}, inplace=True)

# Converting the date to a datetime object
movies_df['release_date'] = pd.to_datetime(movies_df['release_date'])

# Removal of films for which no director was found (for data integrity)
movies_df.dropna(subset=['director_id'], inplace=True)
movies_df['director_id'] = movies_df['director_id'].astype(int)


print("The transformation is complete!")
print("\n--- Preview of the final DataFrames ---")

print("\n1. Table of Movies (movies_df):")
print(movies_df.head())

print("\n2. Table of Genres (genres_df):")
print(genres_df.head())

print("\n3. Table of Coefficients (people_df):")
print(people_df.head())

print("\n4. Table of Tape-Item Connections (movie_genres_df):")
print(movie_genres_df.head())

print("\n5. Table of Film-Actor Connections (movie_cast_df):")
print(movie_cast_df.head())

# --- Part 3: Load ---

print("\nPhase 3: Load - Data loading into the database SQLite...")

# We define the name of the database
DB_NAME = 'movies.db'
# We create the connection to the database
engine = create_engine(f'sqlite:///{DB_NAME}')

try:
    # We load each DataFrame into a table in the database
    # if_exists='replace': If the table exists, it will delete it and recreate it.
    # index=False: We do not store the DataFrame index as a column

    movies_df.to_sql('movies', engine, if_exists='replace', index=False)
    print("The 'movies' table has been successfully loaded.")

    genres_df.to_sql('genres', engine, if_exists='replace', index=False)
    print("The 'genres' table has been successfully loaded.")

    people_df.to_sql('people', engine, if_exists='replace', index=False)
    print("The 'people' table has been successfully loaded.")

    movie_genres_df.to_sql('movie_genres', engine, if_exists='replace', index=False)
    print("The 'movie_genres' table has been successfully loaded.")

    movie_cast_df.to_sql('movie_cast', engine, if_exists='replace', index=False)
    print("The 'movie_cast' table has been successfully loaded.")

    print(f"\nThe ETL process is complete! The database '{DB_NAME}' has been created and populated with data.")

except Exception as e:
    print(f"An error occurred while loading the database: {e}")


DB_NAME = 'movies.db'

# --- The 5 SQL queries as separate strings ---

query1_top_movies = """
-- 1. Find the top 10 movies with the highest ratings.
SELECT title, vote_average
FROM movies
ORDER BY vote_average DESC
LIMIT 10;
"""

query2_movies_per_genre = """
-- 2. List of the number of films by genre. (JOIN & GROUP BY)
SELECT
    g.name,
    COUNT(mg.movie_id) AS movie_count
FROM genres g
JOIN movie_genres mg ON g.id = mg.genre_id
GROUP BY g.name
ORDER BY movie_count DESC;
"""

query3_top_directors = """
-- 3. The most prolific directors on the list (JOIN on multiple tables).
SELECT
    p.name,
    COUNT(m.id) AS directed_movies
FROM people p
JOIN movies m ON p.id = m.director_id
GROUP BY p.name
ORDER BY directed_movies DESC
LIMIT 5;
"""

query4_director_avg_rating = """
-- 4. Average movie rating per director (using Common Table Expression - CTE).
WITH DirectorAvgRating AS (
    SELECT
        p.name AS director_name,
        AVG(m.vote_average) AS avg_rating,
        COUNT(m.id) AS movie_count
    FROM people p
    JOIN movies m ON p.id = m.director_id
    GROUP BY p.name
)
SELECT director_name, avg_rating, movie_count
FROM DirectorAvgRating
WHERE movie_count > 1 -- Display directors with more than 1 film in the list
ORDER BY avg_rating DESC;
"""

query5_movies_after_2001= """
-- Find movies released after 2001
SELECT
    title,
    release_date
FROM movies
WHERE strftime('%Y', release_date) > '2001'
ORDER BY release_date DESC;
"""

# List of queries and their titles to execute them in bulk
queries = {
    "1. The top 10 movies based on ratings": query1_top_movies,
    "2. Number of films by genre": query2_movies_per_genre,
    "3. The 5 most prolific directors": query3_top_directors,
    "4. Average rating per director (with >1 film)": query4_director_avg_rating,
    "5. Films after 2001": query5_movies_after_2001
}

# --- Main SQL Script---
if __name__ == "__main__":
    try:
        # Connection to the database
        conn = sqlite3.connect(DB_NAME)
        print(f"I successfully connected to the database '{DB_NAME}'.\n")

        # Execute each query in the list
        for title, query in queries.items():
            print(f"--- Query Result: {title} ---\n")

            # Use pandas to execute the query and retrieve the results
            df = pd.read_sql_query(query, conn)

            # Check if the DataFrame is empty
            if df.empty:
                print("No results found.\n")
            else:
                print(df.to_string()) # to_string() to display all lines
                print("\n" + "="*50 + "\n")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Closing the connection to the database
        if 'conn' in locals() and conn:
            conn.close()
            print("The connection to the database has been closed.")
