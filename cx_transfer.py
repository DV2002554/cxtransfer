from google.oauth2 import service_account
from googleapiclient.discovery import build
import time
import os

# Configuration
SOURCE_SPREADSHEET_ID = '1LVCOmdI66eibnOXYueNgsCQy5U9vEpgdjXxVtliwCk4'
SOURCE_SHEET_NAME = 'Transfer'
TARGET_SPREADSHEET_ID = '1TuqS0LiYMMVlfWtum4RDwRRB0Zv8yPiH3VTyOexmnNo'
TARGET_SHEET_NAME = 'CX Sheet'
CREDENTIALS_FILE = '/etc/secrets/auto.json'

# Scopes for Google Sheets API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def get_last_row(sheets, spreadsheet_id, sheet_name, column):
    range_name = f'{sheet_name}!{column}:{column}'
    result = sheets.values().get(
        spreadsheetId=spreadsheet_id,
        range=range_name
    ).execute()
    values = result.get('values', [])
    return len(values) + 1 if values else 1

def get_existing_transactions(sheets, spreadsheet_id, sheet_name):
    range_name = f'{sheet_name}!A2:A'
    result = sheets.values().get(
        spreadsheetId=spreadsheet_id,
        range=range_name
    ).execute()
    values = result.get('values', [])
    return set(row[0].strip() for row in values if row and row[0].strip())

def clear_source_rows(sheets, spreadsheet_id, sheet_name, rows_to_clear):
    if not rows_to_clear:
        return
    ranges_to_clear = [f"{sheet_name}!A{row}:G{row}" for row in sorted(list(rows_to_clear))]
    body = {'ranges': ranges_to_clear}
    sheets.values().batchClear(spreadsheetId=spreadsheet_id, body=body).execute()

# --- MODIFIED FUNCTION ---
def insert_rows(sheets, spreadsheet_id, sheet_id, start_row, num_rows):
    """Insert blank rows using the provided sheet_id."""
    requests = [{
        'insertDimension': {
            'range': {
                'sheetId': sheet_id,
                'dimension': 'ROWS',
                'startIndex': start_row - 1,
                'endIndex': start_row - 1 + num_rows
            }, 'inheritFromBefore': False
        }
    }]
    sheets.batchUpdate(spreadsheetId=spreadsheet_id, body={'requests': requests}).execute()

# --- MODIFIED FUNCTION ---
def add_borders(sheets, spreadsheet_id, sheet_id, start_row, num_rows):
    """Add borders using the provided sheet_id."""
    requests = [{
        'updateBorders': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': start_row - 1,
                'endRowIndex': start_row - 1 + num_rows,
                'startColumnIndex': 0,
                'endColumnIndex': 26
            },
            'top': {'style': 'SOLID', 'width': 1}, 'bottom': {'style': 'SOLID', 'width': 1},
            'left': {'style': 'SOLID', 'width': 1}, 'right': {'style': 'SOLID', 'width': 1},
            'innerHorizontal': {'style': 'SOLID', 'width': 1}, 'innerVertical': {'style': 'SOLID', 'width': 1}
        }
    }]
    sheets.batchUpdate(spreadsheetId=spreadsheet_id, body={'requests': requests}).execute()

def get_sheet_id(sheets, spreadsheet_id, sheet_name):
    """Get the sheet ID for a given sheet name."""
    spreadsheet = sheets.get(spreadsheetId=spreadsheet_id).execute()
    for sheet in spreadsheet['sheets']:
        if sheet['properties']['title'] == sheet_name:
            return sheet['properties']['sheetId']
    raise Exception(f"Sheet '{sheet_name}' not found in spreadsheet.")

# --- MODIFIED FUNCTION ---
def process_new_records(sheets, target_sheet_id):
    """Process records using the provided target_sheet_id."""
    source_range = f'{SOURCE_SHEET_NAME}!A2:G'
    result = sheets.values().get(spreadsheetId=SOURCE_SPREADSHEET_ID, range=source_range).execute()
    source_data = result.get('values', [])
    
    target_transactions = get_existing_transactions(sheets, TARGET_SPREADSHEET_ID, TARGET_SHEET_NAME)

    source_transactions = {}
    rows_to_clear = set()
    for i, row in enumerate(source_data):
        if row and row[0].strip():
            transaction_id = row[0].strip()
            if transaction_id in source_transactions or transaction_id in target_transactions:
                rows_to_clear.add(i + 2)
                continue
            source_transactions[transaction_id] = i

    pending_records = [
        (i + 2, row) for i, row in enumerate(source_data)
        if row and row[0].strip() and all(cell.strip() for cell in row[:7]) and row[0].strip() not in target_transactions
    ]

    if not pending_records and not rows_to_clear:
        print('No valid pending records or duplicate rows to process.')
        return False

    if pending_records:
        last_row = get_last_row(sheets, TARGET_SPREADSHEET_ID, TARGET_SHEET_NAME, 'A')
        insert_rows(sheets, TARGET_SPREADSHEET_ID, target_sheet_id, last_row, len(pending_records))
        new_data = [record[1] for record in pending_records]
        target_range = f'{TARGET_SHEET_NAME}!A{last_row}:G{last_row + len(new_data) - 1}'
        sheets.values().update(spreadsheetId=TARGET_SPREADSHEET_ID, range=target_range, valueInputOption='RAW', body={'values': new_data}).execute()
        add_borders(sheets, TARGET_SPREADSHEET_ID, target_sheet_id, last_row, len(new_data))
        rows_to_clear.update(record[0] for record in pending_records)

    clear_source_rows(sheets, SOURCE_SPREADSHEET_ID, SOURCE_SHEET_NAME, rows_to_clear)
    print(f'{len(pending_records)} records transferred. {len(rows_to_clear)} rows cleared from source.')
    return True

def main():
    creds = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheets = service.spreadsheets()

    print(f'Starting script at {time.ctime()}.')
    
    try:
        # --- OPTIMIZATION: Get sheet ID once at the start ---
        print("Fetching target sheet ID...")
        target_sheet_id = get_sheet_id(sheets, TARGET_SPREADSHEET_ID, TARGET_SHEET_NAME)
        print(f"Target sheet ID '{target_sheet_id}' fetched successfully.")
    except Exception as e:
        print(f"FATAL ERROR: Could not get sheet ID. Please check spreadsheet/sheet names and permissions. Error: {e}")
        return

    while True:
        try:
            process_new_records(sheets, target_sheet_id)
            sleep_time = 30
            print(f'Checking again in {sleep_time} seconds...')
            time.sleep(sleep_time)
        except KeyboardInterrupt:
            print('Script stopped by user.')
            break
        except Exception as e:
            print(f'An error occurred: {e}. Retrying in 10 seconds...')
            time.sleep(10)

if __name__ == '__main__':
    main()
