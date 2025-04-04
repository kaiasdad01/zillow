import os
import requests
import boto3
from botocore.exceptions import ClientError
import concurrent.futures
import logging

S3_BUCKET = 'zillow-data-raw-staging'

"""
adding this for logging, becasue the way I had this written previously was writing 
way too much data to terminal - and not actually helpful in logging
"""

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Stream to s3 instead of storing in memory to speed up here

def download_upload_csv(url, custom_filename=None):
    try:
        with requests.get(url, stream=True, timeout=10) as response:
            if response.status_code == 200:
                # Use custom filename if provided, otherwise extract from URL
                file_name = custom_filename if custom_filename else url.split("/")[-1].split('?')[0]
                s3_client = boto3.client('s3')

                # fix issue with showing symbols in streamed csv
                from io import BytesIO
                buffer = BytesIO()
                for chunk in response.iter_content(chunk_size=8192):
                    buffer.write(chunk)
                buffer.seek(0)  # Reset the buffer pointer to the beginning

                s3_client.upload_fileobj(buffer, S3_BUCKET, file_name)
                logger.info(f"Uploaded {file_name} to s3://{S3_BUCKET}/{file_name}")
            else:
                raise Exception(f"Failed to download from {url}: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")

def lambda_handler(event, context):
    # Define a mapping of URLs to custom filenames
    url_to_filename = {
        'https://files.zillowstatic.com/research/public_csvs/zhvi/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1743742807': 'ZHVI All Homes - Metro & US.csv',
        'https://files.zillowstatic.com/research/public_csvs/zhvi/State_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1743742807': 'ZHVI All Homes - State.csv',
        'https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1743742807': 'ZHVI All Homes - County.csv',
        'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1743742807': 'ZHVI All Homes - Zip Code.csv',
        'https://files.zillowstatic.com/research/public_csvs/zhvi/Neighborhood_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1743742807': 'ZHVI All Homes - Neighborhood.csv',
        'https://files.zillowstatic.com/research/public_csvs/zhvf_growth/Metro_zhvf_growth_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1743742807': 'ZHVF All Homes - Metro & US - Mid Tier ONLY.csv',
        'https://files.zillowstatic.com/research/public_csvs/zhvf_growth/Zip_zhvf_growth_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1743742807': 'ZHVF All Homes - Zip Code - Mid Tier ONLY.csv',
        'https://files.zillowstatic.com/research/public_csvs/invt_fs/Metro_invt_fs_uc_sfrcondo_sm_month.csv?t=1743742807': 'For-Sale Inventory - Smooth - All Homes Monthly - Metro & US.csv',
        'https://files.zillowstatic.com/research/public_csvs/invt_fs/Metro_invt_fs_uc_sfrcondo_sm_week.csv?t=1743742807': 'For-Sale Inventory - Smooth - All Homes Weekly - Metro & US.csv',
        'https://files.zillowstatic.com/research/public_csvs/invt_fs/Metro_invt_fs_uc_sfr_sm_month.csv?t=1743742807': 'For-Sale Inventory - Smooth - Single Family Monthly - Metro & US.csv',
        'https://files.zillowstatic.com/research/public_csvs/invt_fs/Metro_invt_fs_uc_sfr_sm_week.csv?t=1743742807': 'For-Sale Inventory - Smooth - Single Family Weekly - Metro & US.csv',
        'https://files.zillowstatic.com/research/public_csvs/sales_count_now/Metro_sales_count_now_uc_sfrcondo_month.csv?t=1743742807': 'Sales Count Nowcast - All Homes Monthly - Metro & US.csv',
        'https://files.zillowstatic.com/research/public_csvs/median_sale_price/Metro_median_sale_price_uc_sfrcondo_month.csv?t=1743742807': 'Median Sale Price - All Homes Monthly - Metro & US.csv',
        'https://files.zillowstatic.com/research/public_csvs/median_sale_price/Metro_median_sale_price_uc_sfr_month.csv?t=1743742807': 'Median Sale Price - SFR Monthly - Metro & US.csv',
        'https://files.zillowstatic.com/research/public_csvs/pct_sold_above_list/Metro_pct_sold_above_list_uc_sfrcondo_month.csv?t=1743742807': '% of Homes Sold Above List (Raw) - All Homes Monthly - Metro & US.csv',
        'https://files.zillowstatic.com/research/public_csvs/pct_sold_below_list/Metro_pct_sold_below_list_uc_sfrcondo_month.csv?t=1743742807': '% of Homes Sold Below List (Raw) - All Homes Monthly - Metro & US.csv',
        'https://files.zillowstatic.com/research/public_csvs/mean_sale_price/Metro_mean_sale_price_uc_sfrcondo_month.csv?t=1743742807': 'Mean Sale Price (Raw) - All Homes Monthly - Metro & US.csv',
        'https://files.zillowstatic.com/research/public_csvs/perc_listings_price_cut/Metro_perc_listings_price_cut_uc_sfrcondo_month.csv?t=1743742807': 'Share of Listing with Price Cut (Raw) - All Homes Monthly - Metro & US.csv',
        'https://files.zillowstatic.com/research/public_csvs/mean_listings_price_cut_amt/Metro_mean_listings_price_cut_amt_uc_sfrcondo_month.csv?t=1743742807': 'Mean Price Cut $ (Raw) - All Homes Monthly - Metro & US.csv',
        'https://files.zillowstatic.com/research/public_csvs/med_listings_price_cut_perc/Metro_med_listings_price_cut_perc_uc_sfrcondo_month.csv?t=1743742807': 'Median Price Cut % (Raw) - All Homes Monthly - Metro & US.csv',
        'https://files.zillowstatic.com/research/public_csvs/market_temp_index/Metro_market_temp_index_uc_sfrcondo_month.csv?t=1743742807': 'Market Heat Index - All Homes Monthly - Metro & US.csv',
        'https://files.zillowstatic.com/research/public_csvs/new_con_sales_count_raw/Metro_new_con_sales_count_raw_uc_sfrcondo_month.csv?t=1743742807': 'New Construction Sales Count (Raw) - All Homes Monthly - Metro & US.csv',
        'https://files.zillowstatic.com/research/public_csvs/new_listings/Metro_new_listings_uc_sfrcondo_month.csv?t=1743742807': 'New Listings (Raw) - All Homes Monthly - Metro & US.csv',
        'https://files.zillowstatic.com/research/public_csvs/mlp/Metro_mlp_uc_sfrcondo_month.csv?t=1743742807': 'Median List Price (Raw) - All Homes Monthly - Metro & US.csv',
        'https://files.zillowstatic.com/research/public_csvs/mlp/Metro_mlp_uc_sfrcondo_sm_month.csv?t=1743742807': 'Median List Price (Smooth) - All Homes Monthly - Metro & US.csv'
    }

    def process_url(url):
        try:
            logger.info(f"Processing {url}")
            custom_filename = url_to_filename.get(url)
            download_upload_csv(url, custom_filename)
        except Exception as e:
            logger.error(f"Error processing {url}: {e}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(process_url, url_to_filename.keys())

    return {
        'statusCode': 200,
        'body': 'CSV download and s3 upload completed successfully'
    }
