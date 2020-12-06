import pickle as pkl
from decimal import Decimal
from random import random
from time import sleep
from urllib.request import urlopen

import numpy as np
from bs4 import BeautifulSoup
from tqdm import tqdm

# https://www.boxofficemojo.com/chart/top_lifetime_gross/?offset=200
root = "https://www.boxofficemojo.com"
no_films = 400

# Films x (internal_id, title, gross, year, synopsis)
# Need to let numpy know we are going to be passing more than just numbers
results = np.zeros([no_films, 5], dtype="<U11")
step = 200

print(f"Beginning scrape for {no_films} films")
pbar = tqdm(total=no_films)
try:
    for offset in np.arange(0, len(results) + step, step):
        main_list = BeautifulSoup(
            urlopen(f"{root}/chart/top_lifetime_gross/?offset={offset}")
            .read()
            .decode("UTF-8"),
            "html.parser",
        )
        for idx, row in enumerate(main_list.find_all("tr")[1:]):
            try:
                entries = row.find_all("td")
                rank = entries[0].contents[0]
                title = entries[1].a.contents[0]
                gross = Decimal(entries[2].contents[0][1:].replace(",", ""))
                year = entries[3].a.contents[0]

                synopsis_link = entries[1].a["href"]
                synopsis_soup = BeautifulSoup(
                    urlopen(f"{root}{synopsis_link}").read().decode("UTF-8"),
                    "html.parser",
                )
                synopsis = (
                    synopsis_soup.find(class_="a-fixed-left-grid-col a-col-right")
                    .find("div")
                    .find("span", class_="a-size-medium")
                    .contents
                )[0]

                results[offset + idx] = (rank, title, gross, year, synopsis)
            except AttributeError:
                print(
                    f"\nFailed to retrieve attributes for item {offset+idx} - skipping"
                )
            sleep(random() + 1)
            pbar.update(1)

    pkl.dump(results, open(f"complete{no_films}_films_and_synopsis.p", "wb"))
except KeyboardInterrupt:
    print("\nKeyboardInterrupt: dumping progress")
    pkl.dump(results, open(f"interrupted{offset+idx}_films_and_synopsis.p", "wb"))

pbar.close()