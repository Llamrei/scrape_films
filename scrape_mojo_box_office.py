import pickle as pkl
import sys
from decimal import Decimal
from random import random
from time import sleep
from urllib.request import urlopen

import re
import os

import numpy as np
from bs4 import BeautifulSoup
from tqdm import tqdm

# https://www.boxofficemojo.com/chart/top_lifetime_gross/?offset=200
root = "https://www.boxofficemojo.com"
start = 0
no_films = 400

if len(sys.argv) > 1:
    no_films = int(sys.argv[1])

films_to_scrape = no_films
starting_string = f"Beginning scrape for {films_to_scrape} films"

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
# For keeping track of where we are through pagination
step = 0

# Check if there is a file to recover - preferentially recover interrupted file over
# temp file
INTERRUPTED_SAVE_STR = "interrupted{step}from{no_films}_films_and_synopsis.pickle"
interrupted_pattern = f"interrupted([0-9]*)from{no_films}"

TEMP_SAVE_STR = "temp_films_and_synopsis.pickle"
temp_pattern = "temp"

recently_modified = sorted(os.listdir(), key=os.path.getmtime, reverse=True)
for filename in recently_modified:
    if filename.endswith(".pickle"):
        res = re.match(interrupted_pattern, filename)
        if res:
            print(f"Found previously interrupted scrape: {filename}")
            interrupted_file = filename
            start = int(res[1])
            films_to_scrape = no_films - start
            print(f"Loading in {start} previously retrieved results")
            results[:start] = pkl.load(open(interrupted_file, "rb"))
            starting_string = f"Retrieving remaining {films_to_scrape}"
            break

        res = re.match(temp_pattern, filename)
        if res:
            print(f"Found previously saved temp file: {filename}")
            temp_file = filename
            temp_data = pkl.load(open(temp_file, "rb"))
            start = len(temp_data)
            films_to_scrape = no_films - start
            print(f"Loading in {start} previously retrieved results")
            results[:start] = pkl.load(open(interrupted_file, "rb"))
            starting_string = f"Retrieving remaining {films_to_scrape}"

# Pagination size - corerpsonds to the size 200 lists they have on website
offset_step = 200

init_offset = int(start / offset_step) * offset_step
first_init_idx = start % offset_step

print(starting_string)
pbar = tqdm(total=no_films, initial=start)
first_page = True
try:
    for offset in np.arange(init_offset, len(results), offset_step):
        main_list = BeautifulSoup(
            urlopen(f"{root}/chart/top_lifetime_gross/?offset={offset}")
            .read()
            .decode("UTF-8"),
            "html.parser",
        )
        if first_page:
            init_idx = first_init_idx
            first_page = False
        else:
            init_idx = 0
        # Add one in scan to remove header
        for idx, row in enumerate(main_list.find_all("tr")[init_idx + 1 :], init_idx):
            step = idx + offset
            # no_films is 1 indexed, step is 0 indexed
            if step >= no_films:
                break
            try:
                entries = row.find_all("td")
                rank = entries[0].text.replace(",", "")
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
                    .text
                )

                results[step][0] = rank
                results[step][1] = title
                results[step][2] = gross
                results[step][3] = year
                results[step][4] = synopsis
            except AttributeError:
                # Step is 0 indexed but ranking is 1 indexed
                print(f"\nFailed to retrieve attributes for item {step+1} - skipping")
            # To not bombard the server [1,2) second uniformly random delay
            pbar.update(1)
            sleep(random() + 1)
        if (offset) % 1000 == 0 and offset != 0:
            # Save every 1000 entries
            pkl.dump(results[:step], open(, "wb"))
    pkl.dump(results, open(f"complete{no_films}_films_and_synopsis.pickle", "wb"))
except (Exception, KeyboardInterrupt) as e:
    print(f"\n Error: dumping progress up to step {step}")
    # Not including latest attempt as we broke it
    pkl.dump(
        results[:step],
        open(INTERRUPTED_SAVE_STR.format(step=step, no_films=no_films), "wb"),
    )
    pbar.close()
    raise e


pbar.close()