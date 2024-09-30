import azure.functions as func
import logging
import polars as pl
# import xlsxwriter
# from datetime import datetime
import os
import requests
import tempfile


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        req_body = req.get_json()
        file = req_body.get('sql_table_name')
        name = req_body.get('output_file_name')

        client_id = 'b38bfc05-9e26-45b1-a8b7-0020b1abcc9a'
        client_secret = os.environ["client_secret"]
        tenant_id = 'bf0cdff8-6480-41e4-b77c-f2a3f2003ac8'
        sql_pwd = os.environ["sql_password"]
 
        # Authenticate and get an access token
        auth_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
        data = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_assertion_type':'urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
            'client_assertion':client_secret,
            'scope': 'https://princetonmedspapartners.sharepoint.com/.default'
        }
        response = requests.post(auth_url, data=data)
  
        access_token = response.json()['access_token']

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/octet-stream'
        }
        logging.info(f'creating workbook')

        logging.info(f'workbook completed')

        
        logging.info(f'starting {file}')
        workbook = os.path.join(tempfile.gettempdir(), f'{name}.xlsx')

        df = pl.read_database_uri(
            f"select * from {file};",
            f"snowflake://dbtuser:{sql_pwd}@tyzsrkt-ow48443.snowflakecomputing.com/ANALYTICS_DB/PROD_DWH?warehouse=READER_WH&role=OWNER_ANALYTICS",
            engine="adbc"
        )
        logging.info(f'completed {file} load')

        estimated_size =  df.estimated_size("mb") 
        logging.info('File size in MB: '+str(df.estimated_size("mb")))

        if estimated_size > 30:
            file_name=f'{name}.csv'
            workbook = os.path.join(tempfile.gettempdir(), f'{file_name}')

            logging.info(f'file too large...writing {file} to csv')
            df.write_csv(workbook, include_header=True)
            logging.info(f'completed writing {file} to csv')

        else:
            logging.info(f'writing {file} to excel')
            df.write_excel(workbook=workbook,worksheet=name,include_header=True) 

            logging.info(f'completed writing {file} to excel')
            file_name=f'{name}.xlsx'

        with open(workbook, 'rb') as file:
            response=requests.post(url=f"https://princetonmedspapartners.sharepoint.com/sites/FinanceFunctionBoard-ProjectSnowFlake_Analytics_8/_api/web/GetFolderByServerRelativeURL('/sites/FinanceFunctionBoard-ProjectSnowFlake_Analytics_8/Shared Documents/Project SnowFlake_Analytics_8/Analytics_8_Project_Folder_Q1_2024')/Files/add(url='{file_name}',overwrite=true)",headers=headers,data=file)
            logging.info('run complete: ')

        return func.HttpResponse(f"{name} created from {file}",status_code=200)
    except ValueError:
        return func.HttpResponse(f"Error:{ValueError}",status_code=500)
