"""
Gmail Email Reader with SQLite Storage and Excel Export

A streamlined script to read emails, extract contact data, store in SQLite database,
and append new data to an Excel file with basic statistics.
"""

import base64
import json
import os
import re
import sqlite3
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Optional, Dict, Any, List

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

try:
    import pandas as pd
    EXCEL_SUPPORT = True
except ImportError:
    EXCEL_SUPPORT = False
    print("Warning: pandas library not installed. Excel export functionality will be disabled.")


class GmailReader:
    """A class to handle Gmail API operations for reading and parsing emails."""
    
    SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
    CREDENTIALS_FILE = "credentials.json"
    TOKEN_FILE = "token.json"
    
    def __init__(self):
        """Initialize the Gmail reader with authentication."""
        self.service = None
        self._authenticate()
    
    def _authenticate(self) -> None:
        """Authenticate with Gmail API using OAuth2."""
        creds = None
        
        if os.path.exists(self.TOKEN_FILE):
            try:
                creds = Credentials.from_authorized_user_file(self.TOKEN_FILE, self.SCOPES)
            except Exception:
                os.remove(self.TOKEN_FILE)
        
        if not creds or not creds.valid:
            if not os.path.exists(self.CREDENTIALS_FILE):
                raise FileNotFoundError(f"Credentials file '{self.CREDENTIALS_FILE}' not found.")
            
            flow = InstalledAppFlow.from_client_secrets_file(self.CREDENTIALS_FILE, self.SCOPES)
            creds = flow.run_local_server(port=0)
            
            with open(self.TOKEN_FILE, "w") as token_file:
                token_file.write(creds.to_json())
        
        self.service = build("gmail", "v1", credentials=creds)
    
    @staticmethod
    def _extract_header_value(headers: list, header_name: str) -> str:
        """Extract a specific header value from email headers."""
        for header in headers:
            if header["name"].lower() == header_name.lower():
                return header["value"]
        return "Unknown"
    
    @staticmethod
    def _decode_email_body(payload: Dict[str, Any]) -> str:
        """Decode email body from the payload."""
        if "data" in payload.get("body", {}):
            try:
                return base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8")
            except Exception:
                return "Error decoding email body"
        
        if "parts" in payload:
            for part in payload["parts"]:
                if part["mimeType"] == "text/plain" and "data" in part["body"]:
                    try:
                        return base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8")
                    except Exception:
                        return "Error decoding email part"
        
        return "No readable content found"
    
    @staticmethod
    def _parse_contact_info(email_body: str) -> Dict[str, str]:
        """Parse structured contact information from email body."""
        contact_info = {"name": "", "address": "", "postcode": "", "other": ""}
        cleaned_body = re.sub(r'\r\n', '\n', email_body.strip())
        
        patterns = {
            "name": r"name\s*:\s*(.+?)(?=\n|$)",
            "address": r"address\s*:\s*(.+?)(?=\n|$)",
            "postcode": r"postcode\s*:\s*(.+?)(?=\n|$)",
            "other": r"other\s*:\s*(.+?)(?=\n|$)"
        }
        
        for field, pattern in patterns.items():
            match = re.search(pattern, cleaned_body, re.IGNORECASE | re.MULTILINE)
            if match:
                contact_info[field] = match.group(1).strip()
        
        return contact_info
    
    def get_recent_emails(self, minutes: int = 10, subject_filter: str = "Subject Application") -> List[Dict[str, str]]:
        """Retrieve emails from the last X minutes with subject filtering."""
        if not self.service:
            return []
        
        try:
            query = f'subject:"{subject_filter}"'
            results = self.service.users().messages().list(
                userId="me", q=query, maxResults=50
            ).execute()
            
            messages = results.get("messages", [])
            if not messages:
                return []
            
            recent_emails = []
            for message_info in messages:
                try:
                    message = self.service.users().messages().get(userId="me", id=message_info["id"]).execute()
                    payload = message["payload"]
                    headers = payload.get("headers", [])
                    
                    email_date = self._extract_header_value(headers, "Date")
                    email_date_dt = parsedate_to_datetime(email_date)
                    
                    if email_date_dt.tzinfo is None:
                        email_date_dt = email_date_dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
                    
                    cutoff_time = datetime.now().astimezone() - timedelta(minutes=minutes)
                    
                    if email_date_dt >= cutoff_time:
                        email_data = {
                            "sender": self._extract_header_value(headers, "From"),
                            "subject": self._extract_header_value(headers, "Subject"),
                            "date": email_date,
                            "body": self._decode_email_body(payload)
                        }
                        recent_emails.append(email_data)
                except Exception:
                    continue
            
            return recent_emails
            
        except Exception:
            return []
    
    def parse_recent_contact_emails(self, minutes: int = 10) -> List[Dict[str, str]]:
        """Retrieve recent emails and parse them for contact information."""
        emails = self.get_recent_emails(minutes)
        
        parsed_contacts = []
        for email_data in emails:
            contact_info = self._parse_contact_info(email_data["body"])
            contact_info.update({
                "email_sender": email_data["sender"],
                "email_subject": email_data["subject"],
                "email_date": email_data["date"]
            })
            parsed_contacts.append(contact_info)
        
        return parsed_contacts


def create_contacts_database(db_path: str = "contacts.db") -> None:
    """Create the contacts database and table if they don't exist."""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS contacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT, address TEXT, postcode TEXT, other TEXT,
                    email_sender TEXT, email_subject TEXT, email_date TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(email_sender, email_date, name)
                )
            ''')
            
            conn.commit()
            
    except sqlite3.Error as e:
        print(f"Database creation error: {e}")


def save_contacts_to_database(contacts: List[Dict[str, str]], db_path: str = "contacts.db") -> int:
    """Save multiple contacts to the SQLite database."""
    create_contacts_database(db_path)
    
    saved_count = 0
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            for contact_info in contacts:
                cursor.execute('''
                    INSERT OR IGNORE INTO contacts 
                    (name, address, postcode, other, email_sender, email_subject, email_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    contact_info.get('name', ''),
                    contact_info.get('address', ''),
                    contact_info.get('postcode', ''),
                    contact_info.get('other', ''),
                    contact_info.get('email_sender', ''),
                    contact_info.get('email_subject', ''),
                    contact_info.get('email_date', '')
                ))
                
                if cursor.rowcount > 0:
                    saved_count += 1
            
            conn.commit()
            
    except sqlite3.Error as e:
        print(f"Database save error: {e}")
    
    return saved_count


def get_database_stats(db_path: str = "contacts.db") -> Dict[str, int]:
    """Get statistics about the contacts database."""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute('SELECT COUNT(*) FROM contacts')
            total_contacts = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM contacts WHERE DATE(created_at) = DATE('now')")
            today_contacts = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM contacts WHERE created_at >= DATE('now', '-7 days')")
            week_contacts = cursor.fetchone()[0]
            
            return {
                'total_contacts': total_contacts,
                'today_contacts': today_contacts,
                'week_contacts': week_contacts
            }
            
    except sqlite3.Error:
        return {'total_contacts': 0, 'today_contacts': 0, 'week_contacts': 0}


def print_database_stats(db_path: str = "contacts.db") -> None:
    """Print database statistics."""
    stats = get_database_stats(db_path)
    print("=== Database Statistics ===")
    print(f"Total Contacts: {stats['total_contacts']}")
    print(f"New Today: {stats['today_contacts']}")
    print(f"This Week: {stats['week_contacts']}")
    print("=" * 30)


def export_contacts_to_excel(contacts: List[Dict[str, str]], filename: str = "contacts.xlsx") -> bool:
    """
    Export contacts to an Excel file, appending to existing file.
    """
    if not EXCEL_SUPPORT:
        print("Error: pandas library required for Excel export.")
        return False
    
    if not contacts:
        return False
    
    try:
        all_contacts = contacts
        
        if os.path.exists(filename):
            try:
                existing_df = pd.read_excel(filename)
                existing_contacts = existing_df.to_dict('records')
                
                # Remove duplicates
                existing_ids = set()
                for contact in existing_contacts:
                    identifier = (
                        str(contact.get('name', '')).strip().lower(),
                        str(contact.get('email_sender', '')).strip().lower(),
                        str(contact.get('email_date', '')).strip()
                    )
                    existing_ids.add(identifier)
                
                unique_new_contacts = []
                for contact in contacts:
                    identifier = (
                        str(contact.get('name', '')).strip().lower(),
                        str(contact.get('email_sender', '')).strip().lower(),
                        str(contact.get('email_date', '')).strip()
                    )
                    if identifier not in existing_ids:
                        unique_new_contacts.append(contact)
                
                if unique_new_contacts:
                    all_contacts = existing_contacts + unique_new_contacts
                else:
                    all_contacts = existing_contacts
                    
            except Exception:
                all_contacts = contacts
        
        df = pd.DataFrame(all_contacts)
        
        # Reorder columns for better readability
        column_order = ['name', 'address', 'postcode', 'other', 'email_sender', 'email_subject', 'email_date']
        available_columns = [col for col in column_order if col in df.columns]
        df = df[available_columns]
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Contacts', index=False)
            
            worksheet = writer.sheets['Contacts']
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
            
            worksheet.freeze_panes = 'A2'
            worksheet.auto_filter.ref = worksheet.dimensions
        
        return True
        
    except Exception as e:
        print(f"Error exporting to Excel: {e}")
        return False


def main():
    """Main function to read emails, store in database, and export to Excel."""
    try:
        # Initialize Gmail reader
        gmail_reader = GmailReader()
        
        # Check for new emails in the last 30 minutes
        print("Checking for new emails in the last 30 minutes...")
        contacts = gmail_reader.parse_recent_contact_emails(30)
        
        if contacts:
            print(f"Found {len(contacts)} new contact(s)")
            
            # Save to database
            saved_count = save_contacts_to_database(contacts, "contacts.db")
            print(f"Saved {saved_count} new contacts to database")
            
            # Export to Excel (append to existing file)
            if export_contacts_to_excel(contacts, "contacts.xlsx"):
                print("Contacts appended to Excel file")
            
        else:
            print("No new emails found in the last 30 minutes")
        
        # Print statistics
        print_database_stats("contacts.db")
        
    except FileNotFoundError as e:
        print(f"Setup error: {e}")
        print("Make sure you have downloaded credentials.json from Google Cloud Console.")
    except Exception as e:
        print(f"Application error: {e}")


if __name__ == "__main__":
    main()