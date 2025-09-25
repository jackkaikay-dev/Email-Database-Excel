import os
import sys
import subprocess
from PyInstaller.__main__ import run

def build_windows():
    # Ensure required files exist
    required_files = ['app.py', 'gmail_reader.py', 'templates/index.html', 'static/style.css']
    for file in required_files:
        if not os.path.exists(file):
            print(f"Error: {file} not found!")
            return
    
    # PyInstaller configuration
    opts = [
        'app.py',
        '--name=EmailProcessor',
        '--onefile',
        '--windowed',
        '--add-data=templates;templates',
        '--add-data=static;static',
        '--add-data=credentials.json;.',
        '--hidden-import=googleapiclient',
        '--hidden-import=google.auth',
        '--hidden-import=email',
        '--clean'
    ]
    
    try:
        run(opts)
        print("Windows build completed successfully!")
    except Exception as e:
        print(f"Build error: {e}")

if __name__ == '__main__':
    build_windows()