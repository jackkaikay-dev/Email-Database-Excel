import base64
import json
import os
import re
import sqlite3
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import psycopg2
from supabase import create_client

try:
    load_dotenv()
    import pandas as pd
    EXCEL_SUPPORT = True
except ImportError:
    EXCEL_SUPPORT = False
    print("Warning: pandas library not installed. Excel export functionality will be disabled.")
    
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

def get_connection():
    """Create and return a database connection"""
    try:
        conn = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME
    )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

# 1. INSERT SINGLE RECORD
def insert_single_record():
    conn = get_connection()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        
        # Using parameterized query (safe from SQL injection)
        insert_query = """
        INSERT INTO usersinfo (name, address, postcode, skills, other, email_sender, email_subject, email_date) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        
        data = ('Jack Kay', '2 Green Drive', 'PR1 0RD', 'DevOps', 'Other Example', 'example@hotmail.co.uk', 'Subject Application', datetime.now())
        
        cur.execute(insert_query, data)
        new_id = cur.fetchone()[0]  # Get the returned ID
        
        conn.commit()
        print(f"Record inserted with ID: {new_id}")
        
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error inserting record: {e}")
    finally:
        cur.close()
        conn.close()

insert_single_record()

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
    def _extract_email_address(from_header: str) -> str:
        """Extract just the email address from the From header."""
        if not from_header:
            return "Unknown"
        
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        match = re.search(email_pattern, from_header)
        if match:
            return match.group(0).lower().strip()
        return from_header
    
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
        contact_info = {
            "name": "", "address": "", "postcode": "", "skills": "", "other": ""
        }
        
        cleaned_body = re.sub(r'\r\n', '\n', email_body.strip())
        
        patterns = {
            "name": [
                r"name\s*:?\s*(.+?)(?=\s*(?:address|postcode|skills|other|from|subject|$))",
                r"^(.+?)(?=\s*(?:address|postcode|skills|other|from|subject|$))"
            ],
            "address": [
                r"address\s*:?\s*(.+?)(?=\s*(?:postcode|skills|other|from|subject|$))",
                r"address\s*:?\s*(.+?)(?=\s*postcode\s*:)",
            ],
            "postcode": [
                r"postcode\s*:?\s*([A-Za-z0-9\s]{2,10}?)(?=\s*(?:skills|other|from|subject|$))",
                r"postcode\s*:?\s*([A-Za-z0-9\s]{2,10})"
            ],
            "skills": [
                r"skills\s*:?\s*(.+?)(?=\s*(?:other|from|subject|$))",
                r"skills\s*:?\s*(.+)"
            ],
            "other": [
                r"other\s*:?\s*(.+?)(?=\s*(?:from|subject|$))",
                r"other\s*:?\s*(.+)"
            ]
        }
        
        for field, pattern_list in patterns.items():
            for pattern in pattern_list:
                match = re.search(pattern, cleaned_body, re.IGNORECASE | re.MULTILINE | re.DOTALL)
                if match:
                    value = match.group(1).strip()
                    value = re.sub(r'^:\s*', '', value)
                    value = re.sub(r'\s+', ' ', value)
                    contact_info[field] = value
                    break
        
        return contact_info
    
    def get_emails_by_subject(self, subject_filter: str = "Subject Application") -> List[Dict[str, str]]:
        """Retrieve ALL emails with subject containing the filter phrase."""
        if not self.service:
            return []
        
        try:
            query = f'subject:"{subject_filter}"'
            all_messages = []
            page_token = None
            
            while True:
                results = self.service.users().messages().list(
                    userId="me", 
                    q=query, 
                    maxResults=500,
                    pageToken=page_token
                ).execute()
                
                messages = results.get("messages", [])
                all_messages.extend(messages)
                
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            
            if not all_messages:
                print("No emails found with the specified subject filter.")
                return []
            
            print(f"Found {len(all_messages)} emails with subject containing '{subject_filter}'")
            
            processed_emails = []
            for i, message_info in enumerate(all_messages):
                try:
                    print(f"Processing email {i+1}/{len(all_messages)}...")
                    message = self.service.users().messages().get(
                        userId="me", 
                        id=message_info["id"],
                        format='full'
                    ).execute()
                    
                    payload = message["payload"]
                    headers = payload.get("headers", [])
                    
                    email_date = self._extract_header_value(headers, "Date")
                    from_header = self._extract_header_value(headers, "From")
                    sender_email = self._extract_email_address(from_header)
                    
                    email_data = {
                        "sender": sender_email,
                        "sender_full": from_header,
                        "subject": self._extract_header_value(headers, "Subject"),
                        "date": email_date,
                        "body": self._decode_email_body(payload),
                        "message_id": message_info["id"]
                    }
                    processed_emails.append(email_data)
                    
                except Exception as e:
                    print(f"Error processing email {i+1}: {e}")
                    continue
            
            print(f"Successfully processed {len(processed_emails)} emails")
            return processed_emails
            
        except Exception as e:
            print(f"Error fetching emails: {e}")
            return []
    
    def parse_contact_emails(self, subject_filter: str = "Subject Application") -> List[Dict[str, str]]:
        """Retrieve ALL emails and parse them for contact information."""
        emails = self.get_emails_by_subject(subject_filter)
        
        parsed_contacts = []
        for email_data in emails:
            contact_info = self._parse_contact_info(email_data["body"])
            contact_info.update({
                "email_sender": email_data["sender"],
                "email_subject": email_data["subject"],
                "email_date": email_data["date"],
                "message_id": email_data["message_id"]
            })
            parsed_contacts.append(contact_info)
        
        print(f"Parsed {len(parsed_contacts)} contacts from emails")
        return parsed_contacts
    
    def parse_recent_contact_emails(self, minutes: int = 30) -> List[Dict[str, str]]:
        """Retrieve recent emails only (for background processing)."""
        if not self.service:
            return []
        
        try:
            cutoff_time = datetime.now().astimezone() - timedelta(minutes=minutes)
            cutoff_timestamp = int(cutoff_time.timestamp())
            
            query = f'subject:"Subject Application" after:{cutoff_timestamp}'
            results = self.service.users().messages().list(
                userId="me", q=query, maxResults=50
            ).execute()
            
            messages = results.get("messages", [])
            if not messages:
                return []
            
            recent_contacts = []
            for message_info in messages:
                try:
                    message = self.service.users().messages().get(
                        userId="me", 
                        id=message_info["id"],
                        format='full'
                    ).execute()
                    
                    payload = message["payload"]
                    headers = payload.get("headers", [])
                    
                    email_date = self._extract_header_value(headers, "Date")
                    from_header = self._extract_header_value(headers, "From")
                    sender_email = self._extract_email_address(from_header)
                    
                    contact_info = self._parse_contact_info(self._decode_email_body(payload))
                    contact_info.update({
                        "email_sender": sender_email,
                        "email_subject": self._extract_header_value(headers, "Subject"),
                        "email_date": email_date,
                        "message_id": message_info["id"]
                    })
                    recent_contacts.append(contact_info)
                    
                except Exception as e:
                    print(f"Error processing recent email: {e}")
                    continue
            
            return recent_contacts
            
        except Exception as e:
            print(f"Error fetching recent emails: {e}")
            return []


def save_contacts_to_database(contacts: List[Dict[str, str]], db_path: str = "contacts.db") -> int:
    """Save multiple contacts to the SQLite database."""

    
    saved_count = 0
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            for contact_info in contacts:
                # Use message_id for uniqueness to avoid duplicates
                cursor.execute('''
                    INSERT OR IGNORE INTO contacts 
                    (name, address, postcode, skills, other, email_sender, email_subject, email_date, message_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    contact_info.get('name', ''),
                    contact_info.get('address', ''),
                    contact_info.get('postcode', ''),
                    contact_info.get('skills', ''),
                    contact_info.get('other', ''),
                    contact_info.get('email_sender', ''),
                    contact_info.get('email_subject', ''),
                    contact_info.get('email_date', ''),
                    contact_info.get('message_id', '')  # Use message_id for uniqueness
                ))
                
                if cursor.rowcount > 0:
                    saved_count += 1
            
            conn.commit()
            
    except sqlite3.Error as e:
        print(f"Database save error: {e}")
    
    return saved_count


def main():
    """Main function to read emails, store in database, and export to Excel."""
    try:
        # Initialize Gmail reader
        gmail_reader = GmailReader()
        
        # Check for new emails in the last 30 minutes
        print("Checking for new emails in the last 30 minutes...")
        contacts = gmail_reader.parse_recent_contact_emails(7200)
        
        if contacts:
            print(f"Found {len(contacts)} new contact(s)")
            
            # Save to database
            saved_count = save_contacts_to_database(contacts, "contacts.db")
            print(f"Saved {saved_count} new contacts to database")
            
        else:
            print("No new emails found in the last 30 minutes")
        
    except FileNotFoundError as e:
        print(f"Setup error: {e}")
        print("Make sure you have downloaded credentials.json from Google Cloud Console.")
    except Exception as e:
        print(f"Application error: {e}")


if __name__ == "__main__":
    main()