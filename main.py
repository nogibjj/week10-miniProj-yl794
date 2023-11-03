"""
Main cli or app entry point
"""
from lib.lib import extract,load_data,query,start_spark,end_spark

def main():
    extract("https://github.com/fivethirtyeight/data/blob/master/daily-show-guests/daily_show_guests.csv?raw=true",
            "/workspaces/week10-miniProj-yl794/data.csv")
    spark = start_spark("DailyShowGuests")
    df = load_data(spark,"/workspaces/week10-miniProj-yl794/data.csv","DailyShowGuests")
    query(
        spark,
        df,
        "SELECT YEAR, COUNT(*) AS guest_count FROM guests GROUP BY YEAR ORDER BY YEAR",
        "guests",
    )
    end_spark(spark)


if __name__ == "__main__":
    main()