import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

logging.basicConfig(
    filename = "logs/get_vendor_summary.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a"
    
)


def create_vendor_summary(conn):

    ''' this function will merge the different tables to get the overall vendor summary and adding new columns to the resultant data '''

    vendor_sales_summary = pd.read_sql_query("""With freight_summary as (
                                       select VendorNumber , sum(freight) as FreightCost 
                                       from vendor_invoice 
                                       group by VendorNumber),

                                       purchase_summary as (
                                       Select p.VendorNumber,
                                       p.VendorName,
                                       p.Brand,
                                       p.Description,
                                       p.PurchasePrice, 
                                       pp.volume, 
                                       pp.price as ActualPrice,
                                       sum(p.Quantity) as TotalPurchaseQuantity,
                                       sum(p.Dollars) as TotalPurchaseDollars from purchases p
                                       join purchase_prices pp
                                       on p.brand = pp.brand
                                       where p.PurchasePrice>0
                                       group by p.VendorNumber, p.VendorName, p.Brand, p.Description, p.purchaseprice, pp.Volume, pp.price),

                                       sales_summary as (
                                       select VendorNo, Brand, sum(salesdollars) as TotalSalesDollars,
                                       sum(salesprice) as TotalSalesPrice,
                                       sum(salesquantity)as TotalSalesQuantity ,
                                       sum(excisetax) as TotalExciseTax  from sales
                                       group by VendorNo , brand
                                       )

                                    select  ps.VendorNumber,
                                       ps.VendorName,
                                       ps.Brand,
                                       ps.Description,
                                       ps.PurchasePrice, 
                                       ps.volume,
                                       ps.actualprice,
                                       ps.volume,
                                       ps.TotalPurchaseQuantity,
                                       ps.TotalPurchaseDollars,
                                       ss.TotalSalesDollars,
                                       ss.TotalSalesQuantity,
                                       ss.TotalSalesPrice,
                                       ss.TotalExciseTax,
                                       fs.FreightCost

                                       from purchase_summary ps left join sales_summary ss
                                       on ps.vendornumber = ss.vendorno
                                       and ps.brand = ss.brand

                                       left join freight_summary as fs
                                       on ps.vendornumber = fs.vendornumber
                                       order by ps.TotalPurchaseDollars desc""",conn)


    return vendor_sales_summary



def clean_data(df):
    
    """ this function will clean data """

    # changing the data type to float
    df['volume']= df['volume'].astype('float64')

    # filling missing value with 0
    df.fillna(0, inplace = True)

    # removing spaces from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = vendor_sales_summary['Description'].str.strip()

    # creating new columns for better analysis
    df['GrossProfit'] =  df['TotalSalesDollars'] - df['TotalPurchaseDollars'] 
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars'])*100
    df['StockTurnover']= df['TotalSalesQuantity']/df['TotalPurchaseQuantity']
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] /df['TotalPurchaseDollars']

    return df


if __name__ == "__main__" :

    #creating database connection

    conn= sqlite3.connect('inventory.db')
    logging.info("Creating Vendor Summary Table....")
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())


    logging.info("Cleaning data....")
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())


    logging.info("Ingesting data....")
    ingest_db(clean_df,'vendor_sales_summary',conn)
    logging.info("Completed"))