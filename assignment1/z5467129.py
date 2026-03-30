#! usrbinenv python3
# -- coding utf-8 --

# Third-party libraries
# NOTE You may only use the following third-party libraries
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
# NOTE It isn't necessary to use all of these to complete the assignment,
# but you are free to do so, should you choose.

# Standard libraries
# NOTE You may use any of the Python 3.11 or 3.13 standard libraries
# httpsdocs.python.org3.11libraryindex.html
# httpsdocs.python.org3.13libraryindex.html
from pathlib import Path
# ... import your standard libraries here ...


######################################################
# NOTE DO NOT MODIFY THE LINE BELOW ...
######################################################
studentid = Path(__file__).stem



######################################################
# NOTE DO NOT MODIFY THE FUNCTION BELOW ...
######################################################
def log(question, output_df=None, other=None):
    print(f"--------------- {question} ----------------")

    if other is not None:
        print(question, other)
    if output_df is not None:
        df = output_df.head(5).copy(True)
        for c in df.columns:
            df[c] = df[c].apply(lambda a: a[:20] if isinstance(a, str) else a)
        
        df.columns = [a[:10] + "..." for a in df.columns]
        print(df.to_string())



######################################################
# NOTE YOU MAY ADD ANY HELPER FUNCTIONS BELOW ...
######################################################


######################################################
# QUESTIONS TO COMPLETE BELOW ...
######################################################


######################################################
# NOTE DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_1(fuel_csv):
    ######################################################
    # TODO Your code goes here ...
    ######################################################
    try:
        pd.read_csv(fuel_csv, dtype=str, nrows=50000)
        error_row = None
    except pd.errors.ParserError as e:
        error_row = int(str(e).split("line ")[-1].split(",")[0]) - 1 
        
        Apart = pd.read_csv(fuel_csv, dtype=str, nrows=error_row+1, index_col=False, on_bad_lines="skip")
        error_PLACE = pd.read_csv(fuel_csv, dtype=str, skiprows=error_row, nrows=1, index_col=False, header=None)
        error_PLACE = error_PLACE.iloc[:, 1:]
        error_PLACE.columns = Apart.columns
        Bpart = pd.read_csv(fuel_csv, dtype=str, skiprows=error_row, index_col=False).iloc[:, 1:]
        Bpart.columns = Apart.columns
        df1 = pd.concat([Apart, error_PLACE, Bpart], ignore_index=True)

        df1["PriceUpdatedDate"] = pd.to_datetime(df1["PriceUpdatedDate"], errors="coerce")
        df1["Price"] = pd.to_numeric(df1["Price"], errors="coerce")
    ######################################################
    # NOTE DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    log("QUESTION 1", output_df=df1, other=df1.shape)
    return df1


######################################################
# NOTE DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_2(df1):
    ######################################################
    # TODO Your code goes here ...
    ######################################################
    df2 = df1.rename(columns={"ServiceStationName": "Name"})
    df2["Suburb"] = df2["Suburb"].str.upper()
    NSW_mask = df2["Address"].str.contains("NSW", na=False)
    New_South_Wales_mask = df2["Address"].str.contains("New South Wales", na=False)
    NEW_SOUTH_WALES_mask = df2["Address"].str.contains("NEW SOUTH WALES", na=False)
    df2 = df2[NSW_mask | New_South_Wales_mask | NEW_SOUTH_WALES_mask]
    ######################################################
    # NOTE DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    log("QUESTION 2", output_df=df2, other=df2.shape)
    return df2


######################################################
# NOTE DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_3(postcodes_json):
    ######################################################
    # TODO Your code goes here ...
    ######################################################
    df3 = pd.read_json(postcodes_json)
    df3 = df3.drop(columns=["accuracy"])
    ######################################################
    # NOTE DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    log("QUESTION 3", output_df=df3, other=df3.shape)
    return df3


######################################################
# NOTE DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_4(df2, df3):
    ######################################################
    # TODO Your code goes here ...
    ######################################################
    df2["Postcode"] = df2["Postcode"].astype(str)
    df3["postcode"] = df3["postcode"].astype(str)
    df2["Suburb"] = df2["Suburb"].str.upper()
    df3["place_name"] = df3["place_name"].str.upper()

    df4 = df2.merge(df3, left_on=["Postcode", "Suburb"], right_on=["postcode", "place_name"], how="left")

    df4["Latitude"] = df4["latitude"]
    df4["Longitude"] = df4["longitude"]

    nan_place = df4["Latitude"].isna()
    same_postcode = df3.sort_values("place_name").groupby("postcode").first().reset_index()
    df4.loc[nan_place, "Latitude"] = df4.loc[nan_place, "Postcode"].map(same_postcode.set_index("postcode")["latitude"])
    df4.loc[nan_place, "Longitude"] = df4.loc[nan_place, "Postcode"].map(same_postcode.set_index("postcode")["longitude"])

    df4 = df4[df2.columns.tolist() + ["Latitude", "Longitude"]]

    df4.to_csv("df4.csv", index=False)
    ######################################################
    # NOTE DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    log("QUESTION 4", output_df=df4, other=df4.shape)
    return df4

######################################################
# NOTE DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_5(df4):
    ######################################################
    # TODO Your code goes here ...
    ######################################################
    df4["PriceDate"] = df4["PriceUpdatedDate"].dt.date
    group_columns = ["Postcode", "FuelCode", "Name", "PriceDate"]
    grouped = df4.groupby(group_columns)
    station_ed_avg = grouped["Price"].mean().reset_index()
    postcode_ed_avg = station_ed_avg.groupby(["Postcode", "FuelCode", "PriceDate"])["Price"].mean().reset_index()
    df5 = postcode_ed_avg.groupby(["Postcode", "FuelCode"])["Price"].mean().reset_index()
    df5 = df5.rename(columns={"FuelCode": "FuelType", "Price": "AveragePrice"})

    fuel_type = df5["FuelType"].unique()
    postcode_type = df5["Postcode"].unique()
    correct_index = pd.MultiIndex.from_product([postcode_type, fuel_type], names=["Postcode", "FuelType"])
    df_missing = pd.DataFrame(list(correct_index), columns=["Postcode", "FuelType"])
    df5 = df5.merge(df_missing, on=["Postcode", "FuelType"], how="right").fillna(0.00)
    df5 = df5.sort_values(["Postcode", "FuelType"]).set_index(["Postcode", "FuelType"])
    df5["AveragePrice"] = df5["AveragePrice"].round(2)
    df5 = df5.sort_index(level=["Postcode", "FuelType"])
    ######################################################
    # NOTE DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    log("QUESTION 5", output_df=df5, other=df5.shape)
    return df5

######################################################
# NOTE DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_6(df4, df5):
    ######################################################
    # TODO Your code goes here ...
    ######################################################
    df5 = df5.reset_index()
    df6 = df4.merge(df5, how="left", left_on=["Postcode", "FuelCode"], right_on=["Postcode", "FuelType"])
    df6["PriceChangeAverage"] = ((df6["Price"] - df6["AveragePrice"]) / df6["AveragePrice"]) * 100
    df6["PriceChangeAverage"] = df6["PriceChangeAverage"].round(2)
    df6["PriceChangeAverage"] = df6["PriceChangeAverage"].fillna(0.00)
    df6.drop(columns=["FuelType", "AveragePrice"], inplace=True)
    ######################################################
    # NOTE DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    log("QUESTION 6", output_df=df6, other=df6.shape)
    return df6


######################################################
# NOTE DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_7(df6):
    ######################################################
    # TODO Your code goes here ...
    ######################################################
    df6["PriceUpdatedDate"] = pd.to_datetime(df6["PriceUpdatedDate"], errors="coerce")
    df7 = df6.copy()
    df7 = df7.sort_values(by=["Name", "Address", "FuelCode", "PriceUpdatedDate"])
    df7["Price"] = pd.to_numeric(df7["Price"], errors="coerce")
    df7["PriceChangePrevious"] = 0.00
    grouped = df7.groupby(["Name", "Address", "FuelCode"])
    price_diff = grouped["Price"].transform(lambda x: x.diff())
    df7["PriceChangePrevious"] = price_diff.fillna(0.00)
    df7["PriceChangePrevious"] = df7["PriceChangePrevious"].apply(lambda x: f"{x:.2f}")
    df7["PriceChangePrevious"] = pd.to_numeric(df7["PriceChangePrevious"], errors="coerce")
    ######################################################
    # NOTE DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    log("QUESTION 7", output_df=df7, other=df7.shape)
    return df7


######################################################
# NOTE DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_8(df7):
    ######################################################
    # TODO Your code goes here ...
    ######################################################
    def Independent_chooson(brand):
        if brand == "Independent":
            return "Independent"
        else:
            return "Franchised"

    df7["Type"] = df7["Brand"].apply(Independent_chooson)
    colors = {"Independent": "#2ca02c", "Franchised": "#1f77b4"}

    plt.figure(figsize=(10, 6))

    df7.boxplot(column="Price", by="Type", patch_artist=True, boxprops=dict(facecolor="lightgray"), medianprops=dict(color="red"))
    plt.title("Fuel Price: Independent vs Franchised", fontsize=14, fontweight="bold")
    plt.suptitle("")
    plt.ylabel("Fuel Price", fontsize=12)
    plt.xlabel("Fuel Station Type", fontsize=12)
    plt.xticks(fontsize=11)
    plt.yticks(fontsize=11)

    avg_prices = df7.groupby("Type")["Price"].mean()
    plt.scatter([1, 2], avg_prices.values, 
            color=[colors["Independent"], colors["Franchised"]], 
            edgecolors="red",  
            s=100, 
            zorder=3)
    
    labels = [f"Avg {t}: {p:.2f}" for t, p in zip(avg_prices.index, avg_prices.values)]
    plt.legend(labels, fontsize=11, loc="upper right")

    answer8 = (
        "We have chosen the box plot for comparing the distribution of fuel prices between independent and chain petrol stations. "
        "It is a very suitable visualisation tool for such comparisons as it effectively shows the Median, Interquartile Range and Outliers of the data to reveal price differences between different types of petrol stations. "
        "Based on this graph, we can see that the average price of fuel is higher for chain petrol stations with a median of around 190.90 cents/litre, while the median for independent petrol stations is lower at around 185.90 cents/litre, indicating that independent petrol stations tend to offer more competitive prices. "
        "Independent petrol filling stations have more stable prices with smaller IQRs, indicating less price volatility. "
        "On the other hand, prices at chain petrol filling stations are more variable, with more stations offering higher prices. "
        "However, it is clear that most of the independent petrol filling stations have high fixed prices, while the chain petrol filling stations may have lower prices due to their geographical location or brand name. "
        "From this we can see that the choice of petrol station may depend on the current situation, but generally speaking, independent petrol stations may be a better choice for long-term use because of their more stable prices. "
        "Chained petrol stations, on the other hand, have a higher brand recognition, but their prices fluctuate greatly, with prices in some areas even far exceeding the average."
    )



    ######################################################
    # NOTE DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    plt.savefig(f"{studentid}-Q8.png")
    log("QUESTION 8", other=answer8)
    return answer8

######################################################
# NOTE DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_9(df7):
    ######################################################
    # TODO Your code goes here ...
    ######################################################
    Q1, Q2, Q3, Q4 = df7["Price"].quantile([0.2, 0.4, 0.6, 0.8])
    price_level = ["low", "medium low", "medium", "medium high", "high"]
    price_colors = ["#1f77b4", "#2ca02c", "#ff7f0e", "#d62728", "#9467bd"]
    df7["PriceCategory"] = pd.cut(df7["Price"], 
                                  bins=[-float("inf"), Q1, Q2, Q3, Q4, float("inf")], 
                                  labels=price_level)
    df7["Color"] = df7["PriceCategory"].map(dict(zip(price_level, price_colors)))
    df7["Size"] = df7.groupby(["Longitude", "Latitude"])["Price"].transform("count")
    df7["Size"] = df7["Size"] / df7["Size"].max() * 100
    plt.figure(figsize=(12, 8))
    plt.scatter(df7["Longitude"], df7["Latitude"], 
                c=df7["Color"], alpha=0.6, 
                s=df7["Size"])
    legend_elements = [
        plt.Line2D([0], [0], marker='o', color='w', label=label,
                   markersize=10, markerfacecolor=color, markeredgecolor='black')
        for label, color in zip(price_level, price_colors)
    ]

    plt.legend(handles=legend_elements, title="Price Range", fontsize=11, loc="upper right")
    plt.title("NSW Average Fuel Price Distribution", fontsize=14, fontweight="bold")
    plt.xlabel("Longitude", fontsize=12)
    plt.ylabel("Latitude", fontsize=12)

    answer9 = (
        "We used a Geospatial Scatter Plot to analyse the distribution of fuel prices in different parts of NSW. "
        "This plot allows us to see the coastal boundaries, and can be compared to a map to make the picture clearer. "
        "We have categorised fuel prices by five colours (from low (blue) to high (purple)) and the size of the dots represents the number of petrol stations in the location to give a visual representation of the competition in the market. "
        "The geographical distribution of the chart shows that Sydney covers all price ranges, indicating a high degree of price diversity in the region. "
        "This reflects the fact that fuel prices in major cities are influenced by a number of factors, such as market competition, supply chain fluctuations, fuel discounts, and price variations based on geographic location. "
        "In contrast, fuel prices in the interior are generally low or medium and there are few high-priced (purple) petrol stations. "
        "This may be due to the lower density of petrol filling stations in the inland areas, where the market is less competitive and fuel prices are generally more stable. "
        "The lower operating costs of inland PFSs as compared with those in coastal metropolitan areas may also be one of the reasons for the low fuel prices. "
        "Moreover, coastal areas have more fuel transportation and supply centres, and hence fuel prices are more competitive and may be subject to discounts or promotional activities. "
        "However, the higher operating costs in these regions result in some petrol stations maintaining higher prices. "
        "On the other hand, inland areas, where transport costs are higher but market competition is lower, fuel prices are more stable and mostly in the low to medium range."
    )



    ######################################################
    # NOTE DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    plt.savefig(f"{studentid}-Q9.png")
    log("QUESTION 9", other=answer9)
    return answer9


######################################################
# NOTE DO NOT MODIFY THE MAIN FUNCTION BELOW ...
######################################################
if __name__ == "__main__":
    df1 = question_1("fuel.csv")
    df2 = question_2(df1.copy(True))
    df3 = question_3("postcodes.json")
    df4 = question_4(df2.copy(True), df3.copy(True))
    df5 = question_5(df4.copy(True))
    df6 = question_6(df4.copy(True), df5.copy(True))
    df7 = question_7(df6.copy(True))
    answer8 = question_8(df7.copy(True))
    answer9 = question_9(df7.copy(True))