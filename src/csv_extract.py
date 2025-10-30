import pandas as pd

movie_path = "data/movies_data.csv"
movie_df = pd.read_csv(movie_path)
movie_df_ord = movie_df.sort_values("release_date")
print(movie_df_ord.head(20))