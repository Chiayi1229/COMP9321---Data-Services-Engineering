#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
COMP9321 25T1 Assignment 2
Data publication as a RESTful service API

Getting Started
---------------

1. You MUST rename this file according to your zID, e.g., z1234567.py.

2. You MUST create a virtual environment for this assignment and install only
    the packages listed in the `requirements.txt` file.
"""

import sys
import sqlite3
import requests
import time
import json
from datetime import datetime
from flask import Flask, request, make_response
from flask_restx import Api, Resource, fields
import io
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

app = Flask(__name__)

######################################################
# Your code goes here ...
######################################################
api = Api(app, title="Travel API", description="Track the countries you have visited")
database_name = "z5467129.db"
api_namespace1 = api.namespace('', description='Processing country')
api_namespace2 = api.namespace('countries', description='Total picture calculate')
last_say = 0

def db_list_name():
    contodb = sqlite3.connect(database_name)
    cur = contodb.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS countries (
            code,
            name,
            native,
            capital,
            continent,
            languages,
            currencies,
            years_visited,
            last_updated,
            flag
        )
    ''')
    contodb.commit()
    contodb.close()

def ask_for_data(country_ID):
    global last_say
    now = time.time()
    if now - last_say < 1:
        time.sleep(1 - (now - last_say))
    last_say = time.time()
    
    finddata = """
    query ($code: ID!) {
      country(code: $code) {
        code
        name
        native
        capital
        continent { name }
        languages { code name native }
        currency
        emoji
      }
    }
    """
    UID = {"code": country_ID.upper()}
    try:
        response = requests.post(
            "https://countries.trevorblades.com/",
            json={"query": finddata, "variables": UID},
            timeout=5
        )
        if response.status_code != 200:
            return None
        truedata = response.json().get("data", {}).get("country")
        if not truedata:
            return None
        
        capital = truedata["capital"]
        if not capital: 
            capital = ""
        
        if truedata["currency"]:
            currency = truedata["currency"].split(",")
        else:
            currency = []
        result = {
            "code": truedata["code"],
            "name": truedata["name"],
            "native": truedata["native"],
            "flag": truedata["emoji"],
            "capital": capital,
            "continent": truedata["continent"]["name"],
            "languages": [
                {"code": item["code"], "name": item["name"], "native": item["native"]}
                for item in truedata["languages"]
            ],
            "currencies": currency,
            "flag": truedata["emoji"]
        }
        return result
    except requests.Timeout:
        return "timeout"
    except requests.RequestException:
        return None

Continent_transfer= {
    "AF": "Africa",
    "AN": "Antarctica",
    "AS": "Asia",
    "OC": "Oceania",
    "EU": "Europe",
    "NA": "North America",
    "SA": "South America"
}

inputdata = api.model('Year enter', {
    'years_visited': fields.List(
        fields.Integer,
        required=False,
        description='List of years you have been visited this country.'
    )
})

# GET /countries/{country_ID}
@api_namespace1.route('/countries/<string:country_ID>')
class CountryProcessing(Resource):
    @api.expect(inputdata)
    @api.doc(
        description="Add a year of the country you have been"
    )
    @api.response(201, 'Country created successfully')
    @api.response(200, 'Country updated successfully')
    @api.response(400, 'Invalid input')
    @api.response(404, 'Data not found')
    @api.response(504, 'Timeout')
    def put(self, country_ID):
        country_ID = country_ID.upper()
        if len(country_ID) != 2:
            return {"error": "Country code must be 2 letters"}, 400
        if not country_ID.isalpha():
            return {"error": "Country code must be 2 letters"}, 400

        data = request.get_json()
        if not data:
            data = {}
        givenyear = data.get("years_visited")
        if givenyear is None:
            givenyear = []

        nowyear = datetime.now().year
        okyears = []
        for year in givenyear:
            if not isinstance(year, int):
                continue
            if year < 1900:
                continue
            if year > nowyear:
                continue
            if year not in okyears:
                okyears.append(year)
        if len(okyears) != len(set(givenyear or [])):
            return {"error": "Years should be in 1900 and current year"}, 400

        contodb = sqlite3.connect(database_name)
        cur = contodb.cursor()
        search_sql = "SELECT * FROM countries WHERE code = ?"
        cur.execute(search_sql, (country_ID,))
        existcountry = cur.fetchone()

        if not existcountry:
            newcountry = ask_for_data(country_ID)
            if newcountry == "timeout":
                contodb.close()
                return {"error": "Timed out"}, 504
            if newcountry is None:
                contodb.close()
                return {"error": f"Country with code '{country_ID}' not found"}, 404

            last_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            speak = json.dumps(newcountry["languages"], ensure_ascii=False)
            money = json.dumps(newcountry["currencies"], ensure_ascii=False)
            okyears.sort()
            years = json.dumps(okyears, ensure_ascii=False)

            my_sql = "INSERT INTO countries (code, name, native, capital, continent, languages, currencies, years_visited, last_updated, flag) "
            my_sql += "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            my_values = (
                newcountry["code"],
                newcountry["name"],
                newcountry["native"],
                newcountry["capital"],
                newcountry["continent"],
                speak,
                money,
                years,
                last_time,
                newcountry["flag"]
            )
            cur.execute(my_sql, my_values)
            contodb.commit()
            what_code = 201
        else:
            before_years = existcountry[7]
            if not before_years:
                before_years = "[]"
            try:
                before_years = json.loads(before_years)
            except:
                before_years = []
            for y in okyears:
                if y not in before_years:
                    before_years.append(y)
            new_years = sorted(before_years)
            lasttime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            years = json.dumps(new_years, ensure_ascii=False)
            my_sql = "UPDATE countries SET years_visited = ?, last_updated = ? WHERE code = ?"
            cur.execute(my_sql, (years, lasttime, country_ID))
            contodb.commit()
            what_code = 200

        my_sql = "SELECT * FROM countries WHERE code = ?"
        cur.execute(my_sql, (country_ID,))
        existcountry = cur.fetchone()

        my_sql = "SELECT code FROM countries WHERE code < ? ORDER BY code DESC LIMIT 1"
        cur.execute(my_sql, (country_ID,))
        prev = cur.fetchone()
        my_sql = "SELECT code FROM countries WHERE code > ? ORDER BY code ASC LIMIT 1"
        cur.execute(my_sql, (country_ID,))
        next = cur.fetchone()

        contodb.close()

        myurl = f"http://{request.host}"
        links_data = {"self": {"href": f"{myurl}/countries/{country_ID}"}}
        if prev:
            links_data["prev"] = {"href": f"{myurl}/countries/{prev[0]}"}
        if next:
            links_data["next"] = {"href": f"{myurl}/countries/{next[0]}"}

        speak_data = existcountry[5]
        if not speak_data:
            speak_data = "[]"
        try:
            speaklist = json.loads(speak_data)
        except:
            speaklist = []

        money_data = existcountry[6]
        if not money_data:
            money_data = "[]"
        try:
            monlist = json.loads(money_data)
        except:
            monlist = []

        year_data = existcountry[7]
        if not year_data:
            year_data = "[]"
        try:
            yelist = json.loads(year_data)
        except:
            yelist = []

        send_data = {
            "code": existcountry[0],
            "name": existcountry[1],
            "native": existcountry[2],
            "flag": existcountry[9],
            "capital": existcountry[3],
            "continent": existcountry[4],
            "languages": speaklist,
            "currencies": monlist,
            "years_visited": yelist,
            "last_updated": existcountry[8],
            "_links": links_data
        }
        return send_data, what_code
    
    @api.doc(
        description="Find a country by its two-letter code",
        params={
            'country_ID': 'The 2-letter country code (e.g., JP)'
        }
    )
    @api.response(200, 'Country found successfully')
    @api.response(400, 'Invalid country code')
    @api.response(404, 'Country not found')
    def get(self, country_ID):
        country_ID = country_ID.upper()
        if len(country_ID) != 2:
            return {"error": "Country code must be 2 letters"}, 400
        if not country_ID.isalpha():
            return {"error": "Country code must be 2 letters"}, 400
        
        contodb = sqlite3.connect(database_name)
        cur = contodb.cursor()
        search_sql = "SELECT * FROM countries WHERE code = ?"
        cur.execute(search_sql, (country_ID,))
        existcountry = cur.fetchone()
        if not existcountry:
            contodb.close()
            return {"error": "Country with code " + country_ID + " not found"}, 404
        
        speak_data = existcountry[5]
        if not speak_data:
            speak_data = "[]"
        if speak_data[0] != "[":
            speak_data = "[]"
        languages = json.loads(speak_data)

        money_data = existcountry[6]
        if not money_data:
            money_data = "[]"
        if money_data[0] != "[":
            money_data = "[]"
        currencies = json.loads(money_data)

        year_data = existcountry[7]
        if not year_data:
            year_data = "[]"
        if year_data[0] != "[":
            year_data = "[]"
        years_visited = json.loads(year_data)

        search_sql = "SELECT code FROM countries WHERE code < ? ORDER BY code DESC LIMIT 1"
        cur.execute(search_sql, (country_ID,))
        prev = cur.fetchone()
        search_sql = "SELECT code FROM countries WHERE code > ? ORDER BY code ASC LIMIT 1"
        cur.execute(search_sql, (country_ID,))
        next = cur.fetchone()

        base_url = "http://" + request.host
        links_data = {"self": {"href": f"{base_url}/countries/{country_ID}"}}
        if prev:
            links_data["prev"] = {"href": f"{base_url}/countries/{prev[0]}"}
        if next:
            links_data["next"] = {"href": f"{base_url}/countries/{next[0]}"}

        send_data = {
            "code": existcountry[0],
            "name": existcountry[1],
            "native": existcountry[2],
            "flag": existcountry[9],
            "capital": existcountry[3],
            "continent": existcountry[4],
            "languages": languages,
            "currencies": currencies,
            "years_visited": years_visited,
            "last_updated": existcountry[8],
            "_links": links_data
        }
        contodb.close()
        return send_data, 200

    @api.expect(inputdata)
    @api.doc(
        description="Add the list of years visited for country in the database",
        params={
            'country_ID': 'The 2-letter country code'
        }
    )
    @api.response(200, 'Country updated successfully')
    @api.response(400, 'Invalid input (bad format or year out of range)')
    @api.response(404, 'Country not found')
    def patch(self, country_ID):
        country_ID = country_ID.upper()
        if len(country_ID) != 2:
            return {"error": "Country code must be 2 letters"}, 400
        if not country_ID.isalpha():
            return {"error": "Country code must be 2 letters"}, 400

        data = request.get_json() or {}
        givenyear = data.get("years_visited", [])

        nowyear = datetime.now().year
        okyears = []
        for year in givenyear:
            if not isinstance(year, int):
                continue
            if year < 1900:
                continue
            if year > nowyear:
                continue
            if year not in okyears:
                okyears.append(year)
        if len(okyears) != len(set(givenyear or [])):
            return {"error": "Years should be in 1900 and current year"}, 400

        contodb = sqlite3.connect(database_name)
        cur = contodb.cursor()
        search_sql ="SELECT * FROM countries WHERE code = ?"
        cur.execute(search_sql, (country_ID,))
        country = cur.fetchone()
        if not country:
            contodb.close()
            return {"error": f"Country with code '{country_ID}' not found"}, 404

        year_data = country[7]
        if not year_data:
            year_data = "[]"
        if year_data[0] != "[":
            year_data = "[]"
        existing_years = json.loads(year_data)

        for y in okyears:
            if y not in existing_years:
                existing_years.append(y)
        updated_year = sorted(existing_years)
        last_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        search_sql ="UPDATE countries SET years_visited = ?, last_updated = ? WHERE code = ?"
        cur.execute(search_sql,(json.dumps(updated_year), last_time, country_ID))
        contodb.commit()

        search_sql = "SELECT * FROM countries WHERE code = ?"
        cur.execute(search_sql, (country_ID,))
        updatedata = cur.fetchone()

        speak_data = updatedata[5]
        if not speak_data:
            speak_data = "[]"
        if speak_data[0] != "[":
            speak_data = "[]"
        languages = json.loads(speak_data)

        money_data = updatedata[6]
        if not money_data:
            money_data = "[]"
        if money_data[0] != "[":
            money_data = "[]"
        currencies = json.loads(money_data)

        year_data = updatedata[7]
        if not year_data:
            year_data = "[]"
        if year_data[0] != "[":
            year_data = "[]"
        years_visited = json.loads(year_data)

        search_sql = "SELECT code FROM countries WHERE code < ? ORDER BY code DESC LIMIT 1"
        cur.execute(search_sql, (country_ID,))
        prev = cur.fetchone()
        search_sql = "SELECT code FROM countries WHERE code > ? ORDER BY code ASC LIMIT 1"
        cur.execute(search_sql, (country_ID,))
        next = cur.fetchone()

        contodb.close()

        url = f"http://{request.host}"
        links = {"self": {"href": f"{url}/countries/{country_ID}"}}
        if prev:
            links["prev"] = {"href": f"{url}/countries/{prev[0]}"}
        if next:
            links["next"] = {"href": f"{url}/countries/{next[0]}"}

        send_data = {
            "code": updatedata[0],
            "name": updatedata[1],
            "native": updatedata[2],
            "flag": updatedata[9],
            "capital": updatedata[3],
            "continent": updatedata[4],
            "languages": languages,
            "currencies": currencies,
            "years_visited": years_visited,
            "last_updated": updatedata[8],
            "_links": links
        }
        return send_data, 200

    @api.doc(description="Delete a country and all associated data using its 2-letter country code.")
    @api.response(200, 'Country deleted successfully')
    @api.response(400, 'Invalid country code format')
    @api.response(404, 'Country not found')
    def delete(self, country_ID):
        country_ID = country_ID.upper()
        if len(country_ID) != 2:
            return {"error": "Country code must be 2 letters"}, 400
        if not country_ID.isalpha():
            return {"error": "Country code must be 2 letters"}, 400
        
        contodb = sqlite3.connect(database_name)
        cur = contodb.cursor()
        search_sql = "SELECT * FROM countries WHERE code = ?"
        cur.execute(search_sql, (country_ID,))
        country = cur.fetchone()
        if not country:
            contodb.close()
            return {"error": f"Country with code '{country_ID}' not found"}, 404

        delete_sql = "DELETE FROM countries WHERE code = ?"
        cur.execute(delete_sql, (country_ID,))
        contodb.commit()
        contodb.close()
        return {"message": f"{country[1]} deleted"}, 200

# GET /countries
@api_namespace1.route('/countries')
class CountryList(Resource):
    @api.doc(
        description="Retrieve a list of countries with optional filtering, sorting, and pagination",
        params={
            'continent': 'Filter by 2-letter continent code',
            'currency': 'Filter by 3-letter currency code',
            'language': 'Filter by 2-letter language code',
            'year': 'Filter by year visited',
            'sort': 'Sort by fields (e.g., continent,-last_updated)',
            'page': 'Page number',
            'size': 'Number of countries per page'
        }
    )
    @api.response(200, 'Countries retrieved successfully')
    @api.response(400, 'Invalid query parameters')
    def get(self):
        page = request.args.get('page', '1')
        size = request.args.get('size', '10')
        sort = request.args.get('sort', 'code')
        continent = request.args.get('continent')
        currency = request.args.get('currency')
        language = request.args.get('language')
        year = request.args.get('year')

        if not page.isdigit():
            return {"error": "page and size must be numbers"}, 400
        if not size.isdigit():
            return {"error": "page and size must be numbers"}, 400
        page = int(page)
        size = int(size)
        if page < 1:
            page = 1
        if size < 1:
            size = 10
        if size > 50:
            size = 10

        if year:
            if not year.isdigit():
                return {"error": "year must be a number"}, 400
            year = int(year)
            this_year = datetime.now().year
            if year < 1900 :
                return {"error": "year must be between 1900 and current year"}, 400
            if year > this_year:
                return {"error": "year must be between 1900 and current year"}, 400

        contodb = sqlite3.connect(database_name)
        cur = contodb.cursor()
        cur.execute("SELECT * FROM countries")
        totalcoun = cur.fetchall()

        filter_country = []

        for one_country in totalcoun:
            country_code = one_country[0]
            country_name = one_country[1]
            country_continent = one_country[4]
            country_languages = one_country[5]
            country_currencies = one_country[6]
            country_years = one_country[7]
            country_updated = one_country[8]

            if not country_languages:
                speak = []
            else:
                if isinstance(country_languages, str):
                    try:
                        speak = json.loads(country_languages)
                        if not isinstance(speak, list):
                            speak = []
                    except json.JSONDecodeError:
                        speak = []
                else:
                    speak = []

            if not country_currencies:
                money = []
            else:
                if isinstance(country_currencies, str):
                    try:
                        money = json.loads(country_currencies)
                        if not isinstance(money, list):
                            money = []
                    except json.JSONDecodeError:
                        money = []
                else:
                    money = []

            if not country_years:
                years = []
            else:
                if isinstance(country_years, str):
                    try:
                        years = json.loads(country_years)
                        if not isinstance(years, list):
                            years = []
                    except json.JSONDecodeError:
                        years = []
                else:
                    years = []

            is_match = True

            if continent:
                continent = continent.upper()
                if continent not in Continent_transfer:
                    return {"error": "Invalid continent code: " + continent}, 400
                conname = Continent_transfer[continent]
                if conname.upper() != country_continent.upper():
                    is_match = False

            if currency:
                findcurrency = False
                for one_currency in money:
                    if one_currency.upper() == currency.upper():
                        findcurrency = True
                        break
                if not findcurrency:
                    is_match = False

            if language:
                findlanguage = False
                for lang in speak:
                    if isinstance(lang, dict):
                        if lang.get("code", "").lower() == language.lower():
                            findlanguage = True
                            break
                if not findlanguage:
                    is_match = False

            if year:
                year_found = False
                for one_year in years:
                    if str(one_year) == str(year):
                        year_found = True
                        break
                if not year_found:
                    is_match = False

            if is_match:
                filter_country.append(one_country)

        total_country = len(filter_country)
        total_page = (total_country + size - 1) // size
        if total_country == 0:
            total_page = 1

        if page < 1:
            return {"error": "Page must be at least 1"}, 400

        if page > total_page and total_country > 0:
            return {"error": f"Page {page} exceeds total pages {total_page}"}, 400

        start = (page - 1) * size
        end = start + size
        paginated_countries = filter_country[start:end]

        sort_fields = sort.split(',')
        sort_fields = [f.lower() for f in sort_fields]
        def sort_data(country):
            sort_keys = []
            for field1 in sort_fields:
                reversedata = field1.startswith('-')
                if reversedata:
                    dataname = field1[1:]
                else:
                    dataname = field1
                data_map = {
                    'code': country[0],
                    'name': country[1].lower(),
                    'continent': country[4],
                    'last_updated': country[8]
                }
                value = data_map.get(dataname, country[0])
                if reversedata:
                    if dataname == 'last_updated':
                        value = datetime.max.strftime("%Y-%m-%d %H:%M:%S") if not value else (
                            datetime.max - datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                        ).total_seconds()
                    elif isinstance(value, str):
                        value = '~' + value
                    else:
                        value = -value
                sort_keys.append(value)
            return tuple(sort_keys)

        filter_country.sort(key=sort_data)
        start = (page - 1) * size
        end = start + size
        paginated_countries = filter_country[start:end]

        items = []
        url = "http://" + request.host
        for country in paginated_countries:
            years_visited = json.loads(country[7]) if country[7] else []
            links = {"self": {"href": f"{url}/countries/{country[0]}"}}
            items.append({
                "code": country[0],
                "name": country[1],
                "continent": country[4],
                "years_visited": years_visited,
                "last_updated": country[8],
                "_links": links
            })

        contodb.close()

        full_data = []
        if continent:
            full_data.append(f"continent={continent}")
        if currency:
            full_data.append(f"currency={currency}")
        if language:
            full_data.append(f"language={language}")
        if year:
            full_data.append(f"year={year}")
        full_data.append(f"sort={sort}")
        full_data.append(f"page={page}")
        full_data.append(f"size={size}")
        data_web = "&".join(full_data)

        links = {"self": {"href": f"{url}/countries?{data_web}"}}
        if page > 1:
            full_data[-2] = f"page={page-1}"
            links["prev"] = {"href": f"{url}/countries?{'&'.join(full_data)}"}
        if page < total_page:
            full_data[-2] = f"page={page+1}"
            links["next"] = {"href": f"{url}/countries?{'&'.join(full_data)}"}

        send_data = {
            "_metadata": {"page": page,
                "size": size,
                "total_pages": total_page,
                "total_countries": total_country
            },
            "countries": items,
            "_links": links
        }
        return send_data, 200

@api_namespace2.route('/visited')
@api.doc(
    description="Visited countries image",
)
@api.response(200, 'Success')
@api.response(204, 'No country visited')
class VisitedCountriesResource(Resource):
    def get(self):
        db = sqlite3.connect(database_name)
        cur = db.cursor()
        cur.execute("SELECT code, continent, years_visited FROM countries")
        countries = cur.fetchall()
        db.close()

        visitcoun = []
        for country in countries:
            code = country[0]
            continent = country[1]
            years_visited = json.loads(country[2])
            if not years_visited:
                years_list = []
            elif isinstance(years_visited, list):
                years_list = years_visited
            elif isinstance(years_visited, str):
                try:
                    years_list = json.loads(years_visited)
                    if not isinstance(years_list, list):
                        years_list = []
                except json.JSONDecodeError:
                    years_list = []
            else:
                years_list = []
            for year in years_list:
                visitcoun.append({"code": code, "continent": continent, "year": year})

        if not visitcoun:
            return "", 204

        year_counts = {}
        for visit in visitcoun:
            year = visit["year"]
            if year in year_counts:
                year_counts[year] = year_counts[year] + 1
            else:
                year_counts[year] = 1

        continent_counts = {}
        for visit in visitcoun:
            continent = visit["continent"]
            if continent in continent_counts:
                continent_counts[continent] = continent_counts[continent] + 1
            else:
                continent_counts[continent] = 1

        fig, (pic1, pic2) = plt.subplots(1, 2, figsize=(12, 5))
        years = sorted(year_counts.keys())
        counts = []
        for y in years:
            counts.append(year_counts[y])
        pic1.plot(years, counts, marker='o', color='b')
        pic1.set_title("Visited Countries Per Year")
        pic1.set_xlabel("Year")
        pic1.set_ylabel("Number of Countries")
        pic1.grid(True)

        continents = list(continent_counts.keys())
        values = []
        for c in continents:
            values.append(continent_counts[c])
        pic2.bar(continents, values, color='orange')
        pic2.set_title("Visits time of Continent")
        pic2.set_xlabel("Continent")
        pic2.set_ylabel("Visits")

        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        plt.close(fig)

        response = make_response(buf.getvalue(), 200)
        response.headers['Content-Type'] = 'image/png'
        return response

db_list_name()
######################################################
# NOTE: DO NOT MODIFY THE MAIN FUNCTION BELOW ...
######################################################
if __name__ == "__main__":
    if len(sys.argv) > 1:
        app.run(debug=True, port=int(sys.argv[1]))
    else:
        app.run(debug=True)