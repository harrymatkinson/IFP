# functionality for calling from TheCocktailDB API

import requests
import json

# return the JSON for a given drink name
# this actually searches for all drink names that match the "s" param
# e.g. "Margarita" returns JSON for "Margarita" and also returns JSON for "Blue Margarita"
# this is ok, because the exact match will always be first
def read_drink(drink_name):
    url = "https://www.thecocktaildb.com/api/json/v1/1/search.php?s="+drink_name
    response = requests.request("GET",
                                url,
                                headers={
                                    "Accept": "application/json",
                                },)
    return(json.dumps(json.loads(response.text), sort_keys=True, indent=4, separators=(",", ": ")))