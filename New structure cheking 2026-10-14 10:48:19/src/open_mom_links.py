#!/usr/bin/env python3
import os
import re
import sys
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from openpyxl import Workbook, load_workbook
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

CLIENT_SECRET_FILE = Path('/home/softsensor/Desktop/October/New structure cheking/credentials/client_secret_2_1069911700212-9kponnktv21g55ngdkkkb4n6m00h8bfm.apps.googleusercontent.com.json')
TOKEN_FILE = Path('/home/softsensor/Desktop/October/New structure cheking/credentials/token.json')
WORKBOOK_FILE = Path('/home/softsensor/Desktop/October/New structure cheking/data/MOM.xlsx')
OUTPUT_FILE = Path('/home/softsensor/Desktop/October/New structure cheking/data/MOM_folder_contents.xlsx')
FOLDER_CHECK_FILE = Path('/home/softsensor/Desktop/October/New structure cheking/data/folder chek.xlsx')
OLD_CSV_FILE = Path('/home/softsensor/Desktop/October/New structure cheking/data/MOM_deliverable_check.csv')
SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets.readonly',
]


def get_drive_service():
    creds = None
    if TOKEN_FILE.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
        except Exception:
            creds = None

    if creds and creds.valid:
        return build('drive', 'v3', credentials=creds)

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            TOKEN_FILE.write_text(creds.to_json())
            return build('drive', 'v3', credentials=creds)
        except Exception:
            pass

    flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRET_FILE), SCOPES)
    creds = flow.run_local_server(port=0)
    TOKEN_FILE.write_text(creds.to_json())
    return build('drive', 'v3', credentials=creds)


def extract_folder_id(url):
    if not isinstance(url, str):
        return None
    match = re.search(r'/folders/([A-Za-z0-9_-]+)', url)
    return match.group(1) if match else None


def decode_safe_link(url):
    if not url:
        return url
    parsed = urlparse(url)
    if parsed.netloc.lower().endswith('safelinks.protection.outlook.com'):
        params = parse_qs(parsed.query)
        encoded = params.get('url', [None])[0]
        if encoded:
            return unquote(encoded)
    return url


def get_cell_hyperlink_target(cell):
    if cell is None:
        return None
    hyperlink = getattr(cell, 'hyperlink', None)
    if hyperlink:
        target = getattr(hyperlink, 'target', None)
        if target:
            return str(target).strip()
    value = cell.value
    if isinstance(value, str):
        value = value.strip()
        if value.lower().startswith('http'):
            return value
    return None


def find_column_index(headers, aliases):
    lower_headers = [str(h).strip().lower() if h is not None else '' for h in headers]
    for alias in aliases:
        alias_lower = alias.lower()
        for idx, header in enumerate(lower_headers):
            if alias_lower in header:
                return idx
    return None


def get_rows_for_processing(ws):
    headers = [cell.value for cell in ws[1]]
    access_idx = find_column_index(headers, ['d1-d4 access', 'd1 d4 access', 'access'])
    url_idx = find_column_index(headers, ['report submission google drive (link)', 'report submission', 'drive link', 'link'])
    upc_idx = find_column_index(headers, ['upc code as per o&m master', 'upc as per survey conducted', 'upc'])

    if access_idx is None:
        access_idx = 18
    if url_idx is None:
        url_idx = 10
    if upc_idx is None:
        upc_idx = 5

    rows = []
    for row in ws.iter_rows(min_row=2, values_only=False):
        access_cell = row[access_idx] if access_idx < len(row) else None
        if access_cell is None:
            continue
        access_value = str(access_cell.value).strip().lower() if access_cell.value is not None else ''
        if access_value != 'accessible':
            continue

        url_cell = row[url_idx] if url_idx < len(row) else None
        url = get_cell_hyperlink_target(url_cell)
        if not url:
            continue
        url = decode_safe_link(url)

        folder_id = extract_folder_id(url)
        if not folder_id and not url.lower().startswith('http'):
            continue

        upc_cell = row[upc_idx] if upc_idx < len(row) else None
        upc_value = upc_cell.value if upc_cell is not None else ''
        upc = str(upc_value).strip() if upc_value is not None else ''
        rows.append((upc, url, folder_id, row[0].row))

    return rows


def list_folder_contents(service, folder_id):
    response = service.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields='files(id, name, mimeType)',
        pageSize=1000,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    return response.get('files', [])


def collect_recursive_names(service, folder_id, prefix=''):
    items = list_folder_contents(service, folder_id)
    found = []
    for item in items:
        name = item.get('name', '(unnamed)')
        mime = item.get('mimeType', '')
        full_name = f'{prefix}/{name}' if prefix else name
        found.append(full_name)
        if mime == 'application/vnd.google-apps.folder':
            child_ids = list_folder_contents(service, item.get('id'))
            if child_ids:
                found.extend(collect_recursive_names(service, item.get('id'), full_name))
    return found


def build_found_value(service, url, folder_id):
    if 'drive.google.com' in url.lower():
        names = collect_recursive_names(service, folder_id)
        if not names:
            return 'Drive folder found but no child items were returned.'

        ordered = []
        unmatched = []
        for path in names:
            match = re.search(r'(?i)(?:deliverable\s*|d)([1-4])\b', path)
            if match:
                ordered.append((int(match.group(1)), path))
            else:
                unmatched.append(path)

        ordered.sort(key=lambda x: x[0])
        final_paths = [path for _, path in ordered] + unmatched
        final_text = []
        for path in final_paths:
            path_label = path.replace('/', ' / ')
            if re.search(r'(?i)(?:deliverable\s*|d)([1-4])\b', path):
                m = re.search(r'(?i)(?:deliverable\s*|d)([1-4])\b', path)
                label = f"D{int(m.group(1))}"
                final_text.append(f'{label} - {path_label}')
            else:
                final_text.append(path_label)
        return '; '.join(final_text)

    if 'sharepoint.com' in url.lower() or 'onedrive' in url.lower():
        return f'OneDrive link detected: {url}'

    return f'URL found: {url}'


def get_recursive_drive_entries(service, folder_id):
    result = {f'D{idx}': [] for idx in range(1, 5)}
    seen = set()

    def walk(current_id, current_path=''):
        for item in list_folder_contents(service, current_id):
            name = item.get('name', '(unnamed)')
            mime = item.get('mimeType', '')
            full_path = f'{current_path}/{name}' if current_path else name
            normalized = full_path.strip('/ ')

            if normalized and normalized not in seen:
                seen.add(normalized)
                for idx in range(1, 5):
                    label = f'D{idx}'
                    if re.search(rf'(?i)(?:deliverable\s*|d){idx}\b', normalized):
                        result[label].append(normalized)
                        break
                    if label.lower() in normalized.lower():
                        result[label].append(normalized)
                        break

            if mime == 'application/vnd.google-apps.folder':
                walk(item.get('id'), full_path)

    walk(folder_id)
    return result


def write_excel_output(rows):
    wb_out = Workbook()
    ws_out = wb_out.active
    ws_out.title = 'Accessible Links'

    headers = [
        'S. No.',
        'Zone',
        'Phase',
        'Cycle',
        'Scope Type (Scope / Additional / De-scoped)',
        'UPC code as per O&M master',
        'UPC as per survey conducted',
        'Actual Survey Start Date',
        'Actual Survey End Date',
        'What I found',
    ]
    ws_out.append(headers)

    for row in rows:
        src_row = row[3]
        values = [
            ws_src.cell(src_row, 1).value,
            ws_src.cell(src_row, 2).value,
            ws_src.cell(src_row, 3).value,
            ws_src.cell(src_row, 4).value,
            ws_src.cell(src_row, 5).value,
            ws_src.cell(src_row, 6).value,
            ws_src.cell(src_row, 7).value,
            ws_src.cell(src_row, 8).value,
            ws_src.cell(src_row, 9).value,
            row[4],
        ]
        ws_out.append(values)

    wb_out.save(OUTPUT_FILE)


def write_folder_check_output(service, rows):
    wb = Workbook()
    ws = wb.active
    ws.title = 'Folder Check'
    ws.append(['Row No', 'UPC', 'D1', 'D2', 'D3', 'D4'])

    for upc, url, folder_id, row_num in rows:
        if not folder_id or 'drive.google.com' not in url.lower():
            ws.append([row_num, upc, '', '', '', ''])
            continue

        entries = get_recursive_drive_entries(service, folder_id)
        ws.append([
            row_num,
            upc,
            '; '.join(entries['D1']),
            '; '.join(entries['D2']),
            '; '.join(entries['D3']),
            '; '.join(entries['D4']),
        ])

    wb.save(FOLDER_CHECK_FILE)
    print(f'Created folder check report: {FOLDER_CHECK_FILE}')


def main():
    if not WORKBOOK_FILE.exists():
        print(f'Workbook not found: {WORKBOOK_FILE}')
        return 1

    global ws_src
    ws_src = load_workbook(WORKBOOK_FILE, data_only=False).active
    rows = get_rows_for_processing(ws_src)

    if not rows:
        print('No accessible rows with valid hyperlink targets were found in column K and access column S.')
        return 1

    if OLD_CSV_FILE.exists():
        OLD_CSV_FILE.unlink()
        print(f'Removed old file: {OLD_CSV_FILE}')

    service = get_drive_service()
    report_rows = []
    for upc, url, folder_id, row_num in rows:
        found_text = build_found_value(service, url, folder_id) if folder_id else url
        report_rows.append((upc, url, folder_id, row_num, found_text))

    write_excel_output(report_rows)
    write_folder_check_output(service, [(upc, url, folder_id, row_num) for upc, url, folder_id, row_num, _ in report_rows])
    print(f'Created output report: {OUTPUT_FILE}')
    print(f'Processed {len(report_rows)} accessible rows with folder links.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
