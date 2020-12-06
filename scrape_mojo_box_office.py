from urllib.request import urlopen
from bs4 import BeautifulSoup
import numpy as np
import pickle as pkl
from time import sleep
from decimal import Decimal
from random import random
import progressbar

widgets = [
    " [",
    progressbar.Timer(),
    "|",
    progressbar.Counter(),
    "|",
    progressbar.Percentage(),
    "] ",
    progressbar.Bar(),
    " (",
    progressbar.ETA(),
    ") ",
]

# https://www.boxofficemojo.com/chart/top_lifetime_gross/?offset=200
root = "https://www.boxofficemojo.com"
no_films = 400

# Films x (internal_id, title, gross, year, synopsis)
# Need to let numpy know we are going to be passing more than just numbers
results = np.zeros([10000, 5], dtype="<U11")
step = 200

try:
    for offset in np.arange(0, len(results) + step, step):
        print(f"Scanning offset {offset}:")
        main_list = BeautifulSoup(
            urlopen(f"{root}/chart/top_lifetime_gross/?offset={offset}")
            .read()
            .decode("UTF-8"),
            "html.parser",
        )
        for idx, row in progressbar.progressbar(
            enumerate(main_list.find_all("tr")[1:]), widgets=widgets, max_value=200
        ):
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
                sleep(random() + 1)
            except AttributeError:
                print(f"Failed to retrieve attributes for item {offset+idx}")

    pkl.dump(results, open("films_and_synopsis.p", "wb"))
except KeyboardInterrupt:
    print("\nKeyboardInterrupt: dumping progress")
    pkl.dump(results, open(f"interrupted{offset+idx}_films_and_synopsis.p", "wb"))