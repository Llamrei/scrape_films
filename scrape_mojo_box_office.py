import pickle as pkl
import sys
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

if len(sys.argv) > 1:
    no_films = int(sys.argv[1])

# Need to let numpy know we are going to be passing more than just numbers
results = np.zeros(
    (no_films,),
    dtype=[
        ("id", "int"),
        ("title", "U100"),
        ("gross", "float"),
        ("year", "int"),
        ("synopsis", "U1000"),
    ],
)
step = 5

print(f"Beginning scrape for {no_films} films")
pbar = tqdm(total=no_films)
try:
    for offset in np.arange(0, len(results), step):
        main_list = BeautifulSoup(
            urlopen(f"{root}/chart/top_lifetime_gross/?offset={offset}")
            .read()
            .decode("UTF-8"),
            "html.parser",
        )
        for idx, row in enumerate(main_list.find_all("tr")[1:]):
            step = idx + offset
            if step >= no_films:
                break
            try:
                entries = row.find_all("td")
                rank = entries[0].text
                title = entries[1].a.text
                gross = Decimal(entries[2].text[1:].replace(",", ""))
                try:
                    year = entries[3].a.text
                except AttributeError:
                    year = entries[3].text
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

                results[step][0] = rank
                results[step][1] = title
                results[step][2] = gross
                results[step][3] = year
                results[step][4] = synopsis
            except AttributeError:
                print(f"\nFailed to retrieve attributes for item {step} - skipping")
            sleep(random() + 1)
            pbar.update(1)

    pkl.dump(results, open(f"complete{no_films}_films_and_synopsis.p", "wb"))
except KeyboardInterrupt:
    print("\nKeyboardInterrupt: dumping progress")
    # Not including latest attempt as we broke it
    pkl.dump(
        results[:step],
        open(f"interrupted{step}_films_and_synopsis.p", "wb"),
    )

pbar.close()