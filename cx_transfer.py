from google.oauth2 import service_account
from googleapiclient.discovery import build
import time
import os

# Configuration
SOURCE_SPREADSHEET_ID = '1FzPPc-rvjfFs_R44Ok29ivFreDcW_W3WyQVrYIy8LaM'
SOURCE_SHEET_NAME = 'TEST'
TARGET_SPREADSHEET_ID = '1Hd2TbGozRvRqJKpnpKa3CDF_X_V9wXNYt5aXivHbats'
TARGET_SHEET_NAME = 'TETS'
CREDENTIALS_FILE = '/etc/secrets/auto.json'

# Scopes for Google Sheets API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def get_last_row(sheets, spreadsheet_id, sheet_name, column):
    """Get the last row with data in a specific column."""
    range_name = f'{sheet_name}!{column}:{column}'
    result = sheets.values().get(
        spreadsheetId=spreadsheet_id,
        range=range_name
    ).execute()
    values = result.get('values', [])
    return len(values) + 1 if values else 1

def get_existing_transactions(sheets, spreadsheet_id, sheet_name):
    """Get all transaction IDs from column A of the specified sheet."""
    range_name = f'{sheet_name}!A2:A'
    result = sheets.values().get(
        spreadsheetId=spreadsheet_id,
        range=range_name
    ).execute()
    values = result.get('values', [])
    return set(row[0].strip() for row in values if row and row[0].strip())

# --- MODIFIED FUNCTION ---
def clear_source_rows(sheets, spreadsheet_id, sheet_name, rows_to_clear):
    """Clear specified rows in Sheet1 using a single batch request."""
    if not rows_to_clear:
        return
    
    # Create a list of ranges to clear, e.g., ["Transfer!A2:G2", "Transfer!A5:G5"]
    ranges_to_clear = [f"{sheet_name}!A{row}:G{row}" for row in sorted(list(rows_to_clear))]
    
    body = {
        'ranges': ranges_to_clear
    }
    
    sheets.values().batchClear(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()

def insert_rows(sheets, spreadsheet_id, sheet_name, start_row, num_rows):
    """Insert blank rows in the specified sheet."""
    requests = [{
        'insertDimension': {
            'range': {
                'sheetId': get_sheet_id(sheets, spreadsheet_id, sheet_name),
                'dimension': 'ROWS',
                'startIndex': start_row - 1,
                'endIndex': start_row - 1 + num_rows
            },
            'inheritFromBefore': False
        }
    }]
    sheets.batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={'requests': requests}
    ).execute()

def add_borders(sheets, spreadsheet_id, sheet_name, start_row, num_rows):
    """Add borders to columns A:Z for specified rows in Sheet2."""
    requests = [{
        'updateBorders': {
            'range': {
                'sheetId': get_sheet_id(sheets, spreadsheet_id, sheet_name),
                'startRowIndex': start_row - 1,
                'endRowIndex': start_row - 1 + num_rows,
                'startColumnIndex': 0,
                'endColumnIndex': 26
            },
            'top': {'style': 'SOLID', 'width': 1},
            'bottom': {'style': 'SOLID', 'width': 1},
            'left': {'style': 'SOLID', 'width': 1},
            'right': {'style': 'SOLID', 'width': 1},
            'innerHorizontal': {'style': 'SOLID', 'width': 1},
            'innerVertical': {'style': 'SOLID', 'width': 1}
        }
    }]
    sheets.batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={'requests': requests}
    ).execute()

def get_sheet_id(sheets, spreadsheet_id, sheet_name):
    """Get the sheet ID for a given sheet name."""
    spreadsheet = sheets.get(spreadsheetId=spreadsheet_id).execute()
    for sheet in spreadsheet['sheets']:
        if sheet['properties']['title'] == sheet_name:
            return sheet['properties']['sheetId']
    return None

def process_new_records(sheets, last_known_row_count):
    """Process new or pending records from Sheet1 and return updated row count."""
    source_range = f'{SOURCE_SHEET_NAME}!A2:G'
    result = sheets.values().get(
        spreadsheetId=SOURCE_SPREADSHEET_ID,
        range=source_range
    ).execute()
    source_data = result.get('values', [])
    current_row_count = len(source_data) + 1

    target_transactions = get_existing_transactions(sheets, TARGET_SPREADSHEET_ID, TARGET_SHEET_NAME)

    source_transactions = {}
    rows_to_clear = set()
    for i, row in enumerate(source_data):
        if row and row[0].strip():
            transaction_id = row[0].strip()
            if transaction_id in source_transactions:
                rows_to_clear.add(i + 2)
                continue
            if transaction_id in target_transactions:
                rows_to_clear.add(i + 2)
                continue
            source_transactions[transaction_id] = i

    pending_records = [
        (i + 2, row) for i, row in enumerate(source_data)
        if row and row[0].strip()
        and all(cell.strip() for cell in row[:7])
        and row[0].strip() not in target_transactions
    ]

    if not pending_records and not rows_to_clear:
        print(f'No valid pending records or duplicate rows to process. Checking again...')
        return current_row_count, False

    if pending_records:
        last_row = get_last_row(sheets, TARGET_SPREADSHEET_ID, TARGET_SHEET_NAME, 'A')
        insert_rows(sheets, TARGET_SPREADSHEET_ID, TARGET_SHEET_NAME, last_row, len(pending_records))
        new_data = [record[1] for record in pending_records]
        target_range = f'{TARGET_SHEET_NAME}!A{last_row}:G{last_row + len(new_data) - 1}'
        sheets.values().update(
            spreadsheetId=TARGET_SPREADSHEET_ID,
            range=target_range,
            valueInputOption='RAW',
            body={'values': new_data}
        ).execute()
        add_borders(sheets, TARGET_SPREADSHEET_ID, TARGET_SHEET_NAME, last_row, len(new_data))
        rows_to_clear.update(record[0] for record in pending_records)

    clear_source_rows(sheets, SOURCE_SPREADSHEET_ID, SOURCE_SHEET_NAME, rows_to_clear)
    print(f'{len(pending_records)} records transferred. {len(rows_to_clear)} rows cleared from source.')
    return current_row_count, True

def main():
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheets = service.spreadsheets()

    last_row_count = 1
    print(f'Starting script at {time.ctime()}. Waiting for new or pending records...')
    while True:
        try:
            last_row_count, processed = process_new_records(sheets, last_row_count)
            # --- RECOMMENDED CHANGE ---
            # Increase sleep time to reduce frequency of API calls
            sleep_time = 30 
            if not processed:
                print(f'No new records processed. Checking again in {sleep_time} seconds...')
            time.sleep(sleep_time) 
        except KeyboardInterrupt:
            print('Script stopped by user.')
            break
        except Exception as e:
            print(f'Error occurred: {e}. Retrying in 10 seconds...')
            time.sleep(10)

if __name__ == '__main__':
    main()
