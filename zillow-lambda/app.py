import os
import requests
import boto3
from botocore.exceptions import ClientError

S3_BUCKET = 'zillow-data-raw-staging'

def upload_to_s3(file_content, s3_key):
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=file_content)
        print(f"Uploaded {file_path} to s3://{S3_BUCKET}/{s3_key}")
    except ClientError as e:
        print(f"Error Uploading {s3_key} to S3: {e}")

def download_upload_csv(url):
    response = requests.get(url)
    if response.status_code == 200:
        file_name = url.split("/")[-1].split('?')[0]
        upload_to_s3(response.content, file_name)
    else: 
        raise Exception(f"Failed to download from {url}: {response.status_code}")

def lambda_handler(event, context):
    urls = [
        'https://files.zillowstatic.com/research/public_csvs/zhvf_growth/Zip_zhvf_growth_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/zhvi/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfr_tier_0.33_0.67_sm_sa_month.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_1_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_2_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_3_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_4_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_bdrmcnt_5_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/invt_fs/Metro_invt_fs_uc_sfrcondo_sm_month.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/invt_fs/Metro_invt_fs_uc_sfrcondo_sm_week.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/invt_fs/Metro_invt_fs_uc_sfr_sm_month.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/invt_fs/Metro_invt_fs_uc_sfr_sm_week.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/new_listings/Metro_new_listings_uc_sfrcondo_sm_month.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/new_listings/Metro_new_listings_uc_sfrcondo_sm_week.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/median_sale_price/Metro_median_sale_price_uc_sfrcondo_sm_sa_month.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/median_sale_price/Metro_median_sale_price_uc_sfrcondo_month.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/median_sale_price/Metro_median_sale_price_uc_sfrcondo_week.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/mean_doz_pending/Metro_mean_doz_pending_uc_sfrcondo_month.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/mean_doz_pending/Metro_mean_doz_pending_uc_sfrcondo_week.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/perc_listings_price_cut/Metro_perc_listings_price_cut_uc_sfrcondo_month.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/perc_listings_price_cut/Metro_perc_listings_price_cut_uc_sfrcondo_week.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/mean_listings_price_cut_amt/Metro_mean_listings_price_cut_amt_uc_sfrcondo_month.csv?t=1743347598',
        'https://files.zillowstatic.com/research/public_csvs/mean_listings_price_cut_amt/Metro_mean_listings_price_cut_amt_uc_sfrcondo_week.csv?t=1743347598'
    ]
    for url in urls:
        try:
            download_upload_csv(url)
        except Exception as e:
            print(f"Error processing {url}: {e}")

    return {
            'statusCode': 200,
            'body': 'CSV download and s3 upload completed successfully'
}