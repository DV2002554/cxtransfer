from google.oauth2 import service_account
from googleapiclient.discovery import build
import time
import os

# Configuration
SOURCE_SPREADSHEET_ID = '1LVCOmdI66eibnOXYueNgsCQy5U9vEpgdjXxVtliwCk4'
SOURCE_SHEET_NAME = 'Transfer'
TARGET_SPREADSHEET_ID = '1Hd2TbGozRvRqJKpnpKa3CDF_X_V9wXNYt5aXivHbats'
TARGET_SHEET_NAME = 'TEST'
# --- MODIFIED LINE ---
# This path points to where Render's Secret File will be located.
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
    range_name = f'{sheet_name}!A2:A'  # Start from row 2
    result = sheets.values().get(
        spreadsheetId=spreadsheet_id,
        range=range_name
    ).execute()
    values = result.get('values', [])
    return set(row[0].strip() for row in values if row and row[0].strip())

def clear_source_rows(sheets, spreadsheet_id, sheet_name, rows_to_clear):
    """Clear specified rows in Sheet1."""
    if not rows_to_clear:
        return
    for row in rows_to_clear:
        range_name = f'{sheet_name}!A{row}:G{row}'
        sheets.values().clear(
            spreadsheetId=spreadsheet_id,
            range=range_name
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
                'endColumnIndex': 26  # A:Z
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
    # Step 1: Get data from Sheet1 (columns A:G, starting from row 2)
    source_range = f'{SOURCE_SHEET_NAME}!A2:G'
    result = sheets.values().get(
        spreadsheetId=SOURCE_SPREADSHEET_ID,
        range=source_range
    ).execute()
    source_data = result.get('values', [])
    current_row_count = len(source_data) + 1  # Account for skipped header row

    # Get existing transactions in Sheet2
    target_transactions = get_existing_transactions(sheets, TARGET_SPREADSHEET_ID, TARGET_SHEET_NAME)

    # Check for repeated transactions in Sheet1 and collect rows to clear
    source_transactions = {}
    rows_to_clear = set()
    for i, row in enumerate(source_data):
        if row and row[0].strip():  # Non-empty transaction ID
            transaction_id = row[0].strip()
            if transaction_id in source_transactions:
                print(f'Warning: Repeated transaction ID "{transaction_id}" at Sheet1 row {i + 2}. Marking for clear...')
                rows_to_clear.add(i + 2)
                continue
            if transaction_id in target_transactions:  # Check for duplicates in Sheet2
                print(f'Warning: Transaction ID "{transaction_id}" exists in Sheet2. Marking for clear...')
                rows_to_clear.add(i + 2)
                continue
            source_transactions[transaction_id] = i

    # Filter valid pending records (non-duplicate, non-repeated, non-empty A:G)
    pending_records = [
        (i + 2, row) for i, row in enumerate(source_data)
        if row and row[0].strip()  # Non-empty transaction ID
        and all(cell.strip() for cell in row[:7])  # Non-empty A:G
        and row[0].strip() not in target_transactions  # Not in Sheet2
    ]

    if not pending_records:
        # Clear any duplicate rows found
        clear_source_rows(sheets, SOURCE_SPREADSHEET_ID, SOURCE_SHEET_NAME, rows_to_clear)
        print(f'No valid pending records to process. Cleared {len(rows_to_clear)} duplicate rows. Current row count: {current_row_count}.')
        return current_row_count, False

    # Find next empty row in Sheet2
    last_row = get_last_row(sheets, TARGET_SPREADSHEET_ID, TARGET_SHEET_NAME, 'A')

    # Insert new rows in Sheet2
    insert_rows(sheets, TARGET_SPREADSHEET_ID, TARGET_SHEET_NAME, last_row, len(pending_records))

    # Paste data to Sheet2 (columns A:G)
    new_data = [record[1] for record in pending_records]
    target_range = f'{TARGET_SHEET_NAME}!A{last_row}:G{last_row + len(new_data) - 1}'
    sheets.values().update(
        spreadsheetId=TARGET_SPREADSHEET_ID,
        range=target_range,
        valueInputOption='RAW',
        body={'values': new_data}
    ).execute()

    # Add borders to new rows (A:Z)
    add_borders(sheets, TARGET_SPREADSHEET_ID, TARGET_SHEET_NAME, last_row, len(new_data))

    # Clear processed and duplicate rows from Sheet1
    rows_to_clear.update(record[0] for record in pending_records)  # Add processed rows
    clear_source_rows(sheets, SOURCE_SPREADSHEET_ID, SOURCE_SHEET_NAME, rows_to_clear)

    print(f'{len(pending_records)} pending records transferred with {len(pending_records)} rows added and borders applied. Cleared {len(rows_to_clear)} rows from Sheet1. New row count: {current_row_count}.')
    return current_row_count, True

def main():
    # Authenticate with the service account
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheets = service.spreadsheets()

    # Wait for new or pending records in Sheet1
    last_row_count = 1  # Start from row 2
    print(f'Starting script at {time.ctime()}. Waiting for new or pending records...')
    while True:
        try:
            last_row_count, processed = process_new_records(sheets, last_row_count)
            if not processed:
                print(f'No records processed. Checking again in 10 seconds...')
                time.sleep(10)
        except KeyboardInterrupt:
            print('Script stopped by user.')
            break
        except Exception as e:
            print(f'Error occurred: {e}. Retrying in 10 seconds...')
            time.sleep(10)

if __name__ == '__main__':
    main()