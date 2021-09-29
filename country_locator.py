import pandas as pd
import numpy as np
import geopy
from geopy.geocoders import Nominatim 

geolocator = Nominatim(user_agent = "geoapiExercises")

def country_locator(x):
        try:
            location = geolocator.geocode(x, language='en', exactly_one=False)
            if len(location)==1 :
                return location[0].address.split(sep=",")[-1].strip()
            else:
                result_list = []
                for result in location:
                    result_list.append(result.address.split(sep=",")[-1].strip())
                if len(set(result_list))==1:
                    return result_list[0]
        except:
            return ""
