import requests
import time
import csv
import random
import concurrent.futures

from bs4 import BeautifulSoup

# global headers to be used for requests
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
}

MAX_THREADS = 10


def extract_movie_details(movie_link):
    time.sleep(random.uniform(0, 0.2))
    try:
        response = requests.get(movie_link, headers=headers)
        response.raise_for_status()
        movie_soup = BeautifulSoup(response.content, 'html.parser')

        title = None
        date = None
        rating = None
        plot_text = None

        # Adjust the selectors based on IMDb's HTML structure
        title_tag = movie_soup.find('h1', attrs={'data-testid': 'hero__pageTitle'})
        date_tag = movie_soup.find('a', href=lambda x: x and 'releaseinfo' in x)
        rating_tag = movie_soup.find('span', class_='sc-bde20123-1 cMEQkK')
        plot_tag = movie_soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})

        if title_tag:
            title = title_tag.get_text()
        if date_tag:
            date = date_tag.get_text()
        if rating_tag:
            rating = rating_tag.get_text()
        if plot_tag:
            plot_text = plot_tag.get_text()

        # Print statements for debugging
        print(f"Title: {title}, Date: {date}, Rating: {rating}, Plot: {plot_text}")

        if all([title, date, rating, plot_text]):
            with open('movies.csv', mode='a', newline='', encoding='utf-8') as file:
                movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                movie_writer.writerow([title, date, rating, plot_text])

    except Exception as e:
        print(f"Failed to process {movie_link}: {e}")


def extract_movies(soup):
    movies_table = soup.find('ul', attrs={'class': 'ipc-metadata-list'})
    if not movies_table:
        print("Failed to find the movies table.")
        return

    movie_rows = movies_table.find_all('li')
    if not movie_rows:
        print("No movie rows found.")
        return

    movie_links = ['https://imdb.com' + row.find('a')['href'] for row in movie_rows if row.find('a')]
    if not movie_links:
        print("No movie links found.")
        return

    threads = min(MAX_THREADS, len(movie_links))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(extract_movie_details, movie_links)


def main():
    start_time = time.time()

    # IMDB Most Popular Movies - 100 movies
    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    response = requests.get(popular_movies_url, headers=headers)
    response.raise_for_status()  # Check for request errors
    soup = BeautifulSoup(response.content, 'html.parser')

    # Open the CSV file once to write the header
    with open('movies.csv', mode='w', newline='', encoding='utf-8') as file:
        movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        movie_writer.writerow(['Title', 'Release Date', 'Rating', 'Plot'])

    # Main function to extract the 100 movies from IMDB Most Popular Movies
    extract_movies(soup)

    end_time = time.time()
    print('Total time taken: ', end_time - start_time)


if __name__ == '__main__':
    main()
