import requests
import sqlite3
import os

BASE_URL = "http://localhost:5000"
DB_NAME = "z5467129.db"

def reset_database():
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS countries (
            code TEXT PRIMARY KEY,
            name TEXT,
            native TEXT,
            flag TEXT,
            capital TEXT,
            continent TEXT,
            languages TEXT,
            currencies TEXT,
            years_visited TEXT,
            last_updated TEXT
        )
    ''')
    conn.commit()
    conn.close()

def test_api():
    print("重置資料庫...")
    reset_database()

    # Question 1: Import a Country (PUT)
    print("\n測試 Question 1: Import a Country (PUT)")
    payload = {"years_visited": [2023]}
    response = requests.put(f"{BASE_URL}/countries/GB", json=payload)
    print(f"PUT /countries/GB (新建): {response.status_code}")
    if response.status_code == 201:
        print(response.json())

    payload = {"years_visited": [2024]}
    response = requests.put(f"{BASE_URL}/countries/GB", json=payload)
    print(f"PUT /countries/GB (更新): {response.status_code}")
    if response.status_code == 200:
        print(response.json())

    response = requests.put(f"{BASE_URL}/countries/123", json={"years_visited": [2023]})
    print(f"PUT /countries/123 (無效代碼): {response.status_code}, {response.json()}")

    # Question 2: Retrieve a Country (GET)
    print("\n測試 Question 2: Retrieve a Country (GET)")
    response = requests.get(f"{BASE_URL}/countries/GB")
    print(f"GET /countries/GB: {response.status_code}")
    if response.status_code == 200:
        print(response.json())

    response = requests.get(f"{BASE_URL}/countries/XX")
    print(f"GET /countries/XX (不存在): {response.status_code}, {response.json()}")

    # Question 3: Delete a Country (DELETE)
    print("\n測試 Question 3: Delete a Country (DELETE)")
    response = requests.delete(f"{BASE_URL}/countries/GB")
    print(f"DELETE /countries/GB: {response.status_code}, {response.json()}")

    response = requests.delete(f"{BASE_URL}/countries/GB")
    print(f"DELETE /countries/GB (已刪除): {response.status_code}, {response.json()}")

    # Question 4: Update a Country (PATCH)
    print("\n測試 Question 4: Update a Country (PATCH)")
    requests.put(f"{BASE_URL}/countries/US", json={"years_visited": [2022]})
    payload = {"years_visited": [2023]}
    response = requests.patch(f"{BASE_URL}/countries/US", json=payload)
    print(f"PATCH /countries/US: {response.status_code}")
    if response.status_code == 200:
        print(response.json())

    response = requests.patch(f"{BASE_URL}/countries/US", json={"years_visited": [1800]})
    print(f"PATCH /countries/US (無效年份): {response.status_code}, {response.json()}")

    # Question 5: Retrieve a List of Countries (GET)
    print("\n測試 Question 5: Retrieve a List of Countries (GET)")
    requests.put(f"{BASE_URL}/countries/CA", json={"years_visited": [2019]})
    response = requests.get(f"{BASE_URL}/countries?continent=NA&year=2019")
    print(f"GET /countries?continent=NA&year=2019: {response.status_code}")
    if response.status_code == 200:
        print(response.json())

    response = requests.get(f"{BASE_URL}/countries?page=1&size=2&sort=-last_updated")
    print(f"GET /countries?page=1&size=2&sort=-last_updated: {response.status_code}")
    if response.status_code == 200:
        print(response.json())

    # Question 6: Visualise Visited Countries (GET)
    print("\n測試 Question 6: Visualise Visited Countries (GET)")
    response = requests.get(f"{BASE_URL}/countries/visited")
    print(f"GET /countries/visited: {response.status_code}")
    if response.status_code == 200:
        with open("visited.png", "wb") as f:
            f.write(response.content)
        print("圖片已儲存為 visited.png")
    elif response.status_code == 204:
        print("無資料可視化")

if __name__ == "__main__":
    test_api()