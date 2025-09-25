import os
import subprocess

def build_android():
    # Create a BeeWare project if it doesn't exist
    if not os.path.exists('pyproject.toml'):
        # Initialize BeeWare project
        subprocess.run(['briefcase', 'new'], check=True)
    
    # Configure for Android
    config = {
        'project_name': 'EmailProcessor',
        'bundle': 'com.example.emailprocessor',
        'version': '1.0.0',
        'description': 'Email Processor App',
        'author': 'Your Name',
        'author_email': 'your@email.com',
        'url': 'https://example.com'
    }
    
    # Update pyproject.toml with our configuration
    pyproject_content = f"""
[tool.briefcase]
project_name = "{config['project_name']}"
bundle = "{config['bundle']}"
version = "{config['version']}"
description = "{config['description']}"
author = "{config['author']}"
author_email = "{config['author_email']}"
url = "{config['url']}"
license = "MIT"

[tool.briefcase.app.emailprocessor]
formal_name = "Email Processor"
sources = ["app.py", "gmail_reader.py"]
requirements = ["flask", "google-api-python-client", "pandas"]

[tool.briefcase.app.emailprocessor.android]
requires = ["android-30"]
"""
    
    with open('pyproject.toml', 'w') as f:
        f.write(pyproject_content)
    
    # Build for Android
    try:
        subprocess.run(['briefcase', 'create', 'android'], check=True)
        subprocess.run(['briefcase', 'build', 'android'], check=True)
        subprocess.run(['briefcase', 'package', 'android', '--format', 'apk'], check=True)
        print("Android build completed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"Android build failed: {e}")

if __name__ == '__main__':
    build_android()