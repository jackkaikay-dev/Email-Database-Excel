import os
import sqlite3
from flask import Flask, render_template, request, jsonify, send_file
import threading
import time
from datetime import datetime, timedelta
import pandas as pd
import io

try:
    from gmail_reader import GmailReader, save_contacts_to_database, get_database_stats
except ImportError as e:
    print(f"Import error: {e}")
    print("Trying alternative import...")
    # Fallback: define minimal versions if import fails
    class GmailReader:
        def __init__(self):
            raise Exception("GmailReader not available - check gmail_reader.py")
    
    def save_contacts_to_database(contacts, db_path="contacts.db"):
        return 0
    
    def get_database_stats(db_path="contacts.db"):
        return {'total_contacts': 0, 'today_contacts': 0, 'week_contacts': 0}

app = Flask(__name__)
app.config['DATABASE'] = 'contacts.db'

# Global variables for background processing
is_processing = False
last_processed = None
processing_thread = None

def init_database():
    """Initialize the database"""
    conn = sqlite3.connect(app.config['DATABASE'])
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT, address TEXT, postcode TEXT, skills TEXT, other TEXT,
            email_sender TEXT, email_subject TEXT, email_date TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(email_sender, email_date, name)
        )
    ''')
    conn.commit()
    conn.close()

def get_contacts(search_term=None):
    """Get contacts from database with optional search"""
    conn = sqlite3.connect(app.config['DATABASE'])
    cursor = conn.cursor()
    
    if search_term:
        cursor.execute('''
            SELECT * FROM contacts 
            WHERE name LIKE ? OR address LIKE ? OR postcode LIKE ? OR skills LIKE ? OR other LIKE ? OR email_sender LIKE ?
            ORDER BY created_at DESC
        ''', (f'%{search_term}%', f'%{search_term}%', f'%{search_term}%', f'%{search_term}%', f'%{search_term}%', f'%{search_term}%'))
    else:
        cursor.execute('SELECT * FROM contacts ORDER BY created_at DESC')
    
    contacts = []
    for row in cursor.fetchall():
        contacts.append({
            'id': row[0],
            'name': row[1],
            'address': row[2],
            'postcode': row[3],
            'skills': row[4],
            'other': row[5],
            'email_sender': row[6],
            'email_subject': row[7],
            'email_date': row[8],
            'created_at': row[9]
        })
    
    conn.close()
    return contacts

def background_email_processing():
    """Background thread for processing emails"""
    global is_processing, last_processed
    
    while is_processing:
        try:
            print("Checking for new emails...")
            gmail_reader = GmailReader()
            contacts = gmail_reader.parse_recent_contact_emails(30)  # Check last 30 minutes
            
            if contacts:
                saved_count = save_contacts_to_database(contacts, app.config['DATABASE'])
                print(f"Saved {saved_count} new contacts")
                last_processed = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
        except Exception as e:
            print(f"Error processing emails: {e}")
        
        # Wait 5 minutes before next check
        for _ in range(300):  # 300 seconds = 5 minutes
            if not is_processing:
                break
            time.sleep(1)

@app.route('/')
def index():
    """Main page"""
    contacts = get_contacts()
    stats = get_database_stats(app.config['DATABASE'])
    
    return render_template('index.html', 
                         contacts=contacts, 
                         stats=stats,
                         is_processing=is_processing,
                         last_processed=last_processed)

@app.route('/search')
def search():
    """Search contacts"""
    search_term = request.args.get('q', '')
    contacts = get_contacts(search_term)
    return jsonify(contacts)

@app.route('/stats')
def stats():
    """Get current statistics"""
    stats = get_database_stats(app.config['DATABASE'])
    return jsonify(stats)

@app.route('/start_processing', methods=['POST'])
def start_processing():
    """Start background email processing"""
    global is_processing, processing_thread
    
    if not is_processing:
        is_processing = True
        processing_thread = threading.Thread(target=background_email_processing)
        processing_thread.daemon = True
        processing_thread.start()
        return jsonify({'status': 'started'})
    
    return jsonify({'status': 'already_running'})

@app.route('/stop_processing', methods=['POST'])
def stop_processing():
    """Stop background email processing"""
    global is_processing
    is_processing = False
    return jsonify({'status': 'stopped'})

@app.route('/process_once', methods=['POST'])
def process_once():
    """Process emails once manually - now does full import"""
    try:
        gmail_reader = GmailReader()
        contacts = gmail_reader.parse_contact_emails()  # Get ALL emails
        
        if contacts:
            saved_count = save_contacts_to_database(contacts, app.config['DATABASE'])
            return jsonify({'status': 'success', 'processed': saved_count, 'total_found': len(contacts)})
        else:
            return jsonify({'status': 'success', 'processed': 0, 'total_found': 0})
    
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

# Update the background processing to still check recent emails only
def background_email_processing():
    """Background thread for processing NEW emails only"""
    global is_processing, last_processed
    
    # Keep the original recent email checking for background processing
    while is_processing:
        try:
            print("Checking for NEW emails in the last 30 minutes...")
            gmail_reader = GmailReader()
            
            # For background processing, we'll still use a time filter to avoid reprocessing everything
            # We'll use the original method name but with a different implementation
            contacts = gmail_reader.parse_recent_contact_emails(2880)  # Check last 30 minutes only
            
            if contacts:
                saved_count = save_contacts_to_database(contacts, app.config['DATABASE'])
                print(f"Saved {saved_count} new contacts")
                last_processed = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
        except Exception as e:
            print(f"Error processing emails: {e}")
        
        # Wait 5 minutes before next check
        for _ in range(300):  # 300 seconds = 5 minutes
            if not is_processing:
                break
            time.sleep(1)
    
@app.route('/import_all', methods=['POST'])
def import_all_emails():
    """Import ALL emails from the entire inbox"""
    try:
        gmail_reader = GmailReader()
        contacts = gmail_reader.parse_contact_emails()  # This will get ALL emails
        
        if contacts:
            saved_count = save_contacts_to_database(contacts, app.config['DATABASE'])
            return jsonify({'status': 'success', 'processed': saved_count, 'total_found': len(contacts)})
        else:
            return jsonify({'status': 'success', 'processed': 0, 'total_found': 0})
    
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/export_excel')
def export_excel():
    """Export contacts to Excel file"""
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(app.config['DATABASE'])
        
        # Read data into pandas DataFrame
        df = pd.read_sql_query("SELECT * FROM contacts ORDER BY created_at DESC", conn)
        
        # Close connection
        conn.close()
        
        # Create Excel file in memory
        output = io.BytesIO()
        
        # Create Excel writer
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Write DataFrame to Excel
            df.to_excel(writer, sheet_name='Contacts', index=False)
            
            # Get workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets['Contacts']
            
            # Add some formatting
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#D7E4BC',
                'border': 1
            })
            
            # Write column headers with format
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Auto-adjust column widths
            for idx, col in enumerate(df.columns):
                max_len = max(
                    df[col].astype(str).str.len().max(),
                    len(col)
                ) + 2
                worksheet.set_column(idx, idx, max_len)
        
        # Seek to beginning of the stream
        output.seek(0)
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"contacts_export_{timestamp}.xlsx"
        
        # Return Excel file as download
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/export_excel_filtered')
def export_excel_filtered():
    """Export filtered contacts to Excel based on search term"""
    try:
        search_term = request.args.get('q', '')
        
        # Connect to SQLite database
        conn = sqlite3.connect(app.config['DATABASE'])
        
        if search_term:
            # Use the same search logic as get_contacts()
            query = '''
                SELECT * FROM contacts 
                WHERE name LIKE ? OR address LIKE ? OR postcode LIKE ? OR skills LIKE ? OR other LIKE ? OR email_sender LIKE ?
                ORDER BY created_at DESC
            '''
            params = (f'%{search_term}%', f'%{search_term}%', f'%{search_term}%', f'%{search_term}%', f'%{search_term}%', f'%{search_term}%')
            df = pd.read_sql_query(query, conn, params=params)
        else:
            # Export all contacts if no search term
            df = pd.read_sql_query("SELECT * FROM contacts ORDER BY created_at DESC", conn)
        
        # Close connection
        conn.close()
        
        # Create Excel file in memory
        output = io.BytesIO()
        
        # Create Excel writer
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Write DataFrame to Excel
            sheet_name = 'Contacts' if not search_term else f'Contacts - "{search_term}"'
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)  # Sheet name max 31 chars
            
            # Get workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets[sheet_name[:31]]
            
            # Add formatting
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#D7E4BC',
                'border': 1
            })
            
            # Write column headers with format
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Auto-adjust column widths
            for idx, col in enumerate(df.columns):
                max_len = max(
                    df[col].astype(str).str.len().max(),
                    len(col)
                ) + 2
                worksheet.set_column(idx, idx, max_len)
        
        # Seek to beginning of the stream
        output.seek(0)
        
        # Create filename with timestamp and search term
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if search_term:
            filename = f"contacts_{search_term.replace(' ', '_')}_{timestamp}.xlsx"
        else:
            filename = f"contacts_export_{timestamp}.xlsx"
        
        # Return Excel file as download
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

def migrate_database():
    """Add skills and message_id columns to existing database"""
    conn = sqlite3.connect('contacts.db')
    cursor = conn.cursor()
    
    try:
        # Check if skills column exists
        cursor.execute("PRAGMA table_info(contacts)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'skills' not in columns:
            cursor.execute('ALTER TABLE contacts ADD COLUMN skills TEXT')
            print("Added skills column")
            
        if 'message_id' not in columns:
            cursor.execute('ALTER TABLE contacts ADD COLUMN message_id TEXT')
            # Add uniqueness constraint
            cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_message_id ON contacts(message_id)')
            print("Added message_id column")
            
        conn.commit()
        print("Database migrated successfully")
            
    except sqlite3.Error as e:
        print(f"Migration error: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    init_database()
    migrate_database()
    app.run(debug=True, host='0.0.0.0', port=5000)